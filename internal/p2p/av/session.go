// Caller 状态机：v2 主叫取流全流程（真机实测验证路径的生产化移植）。
//
//	idle → calling → forward(0x3A) → mtpres(0x24) → meter(72B 双向)
//	     → transfer(KCP 双 conv 建立) → ctl(AvStreamCtl cmd=1 ×18)
//	     → streaming(START + TLV4 媒体)
//	任意态错误/设备 REJECT/CLOSE → closed（REJECT code 映射 app reset 码）。
//
// 会话保持（session_hold，实测）：streaming 态可 Pause——不拆链
// （KCP/meter/TLV 接收全部存活），仅停 cmd=1 重发并丢弃 TLV4 媒体；
// Resume 在同一条活链路上重跑 cmd=1 循环，设备对重复 cmd=1 会再次
// ACCEPT（ctl=[6,2,2]），舞蹈与媒体随之恢复。完整拆链（CLOSE）后
// 15-30 分钟内重呼会被服务器节流，故观众离开走 Pause 而非 Stop。
package av

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

// State 是呼叫状态。
type State string

const (
	StateIdle      State = "idle"
	StateCalling   State = "calling"
	StateForward   State = "forward"
	StateMtpres    State = "mtpres"
	StateMeter     State = "meter"
	StateTransfer  State = "transfer"
	StateCtl       State = "ctl"
	StateStreaming State = "streaming"
	StatePaused    State = "paused"
	StateClosed    State = "closed"
)

var stateRank = map[State]int{
	StateIdle: 0, StateCalling: 1, StateForward: 2, StateMtpres: 3,
	StateMeter: 4, StateTransfer: 5, StateCtl: 6, StateStreaming: 7,
	StateClosed: 100,
}

// StatePaused 不进入 stateRank/state 存储：它由 State() 按 paused 位叠加
// 报告（底层状态保持 streaming/ctl 不回退），closed 优先于一切。

// CloudCreds 是 gute 帧构造所需的账号凭据（frame.Builder 四元组）。
type CloudCreds struct{ C1, C2, S1, S2 uint32 }

// Options 配置一路呼叫。
type Options struct {
	Transport Transport
	Creds     CloudCreds
	Log       *slog.Logger

	// NoPassword=true 时 CALLING +0x68 密码槽保持 0（免密呼叫，未真机
	// 验证）。默认 false：写 GwEncodePassword(treatedPwd)——真机实测
	// 验证行为，AvStreamCtl cmd=1 的 +0x20 同值。
	NoPassword bool

	// LanIP 覆盖 0x3A 对称应答 LAN 地址块的 IP（默认取 socket 本地地址，
	// 通配地址时为 0.0.0.0——公网呼叫路径不依赖该块，实测）。
	LanIP net.IP

	CallingRetry   time.Duration // CALLING 无 meter 交互的重发间隔（默认 6s，最多 3 次）
	MeterTicks     int           // 我方 meter_req 发送上限（默认 200）
	MeterTickEvery time.Duration // meter_req 节拍（默认 100ms，参考实现 20ms 档的保守值）

	// LinkWatchdog 建链看门狗超时窗：呼叫发起到进入 streaming 的总
	// 预算。0 = 按内部预算推导（meter 泵全预算 MeterTicks×MeterTickEvery
	// + AvStreamCtl 18 次重发全预算 + 余量，默认配置 ≈28s）。真机形态
	// CALLING→首帧 300-500ms，正常建链亚秒级、远小于该窗；被服务端
	// 节流/无应答的病态链路超时后主动完整拆链走错误退避，避免会话
	// 永停 calling 占死槽位。
	LinkWatchdog time.Duration
}

// TapFunc 由 session 读循环调用：收到原始 UDP 包先给 tap，返回 true 表示
// av 已消费（0xc8 meter 族 / 0x88/0x90 KCP 封包头族），不再走 gute 解析。
type TapFunc func(pkt []byte, from *net.UDPAddr) bool

// Caller 驱动一路设备 AV 呼叫。并发模型：tap/gute 回调来自 session 读
// 循环（单 goroutine），KCP 读循环与舞蹈/meter 泵各自 goroutine；共享
// 字段由 mu 保护，状态由 stateMu 保护。
type Caller struct {
	devID      uint32
	uid        uint64
	treatedPwd int

	pwdEnc uint32 // rc5.GwEncodePassword(treatedPwd)
	token  []byte // pdwPrm 呼叫参数块 {callType=1, magic=0x1011717, videoMode=2, 0} LE
	fb     *frame.Builder
	log    *slog.Logger
	tr     Transport
	opts   Options

	linkID    uint32
	sessionID uint32

	stateMu   sync.Mutex
	state     State
	lastErr   error
	onStateFn func(State, error)

	mu       sync.Mutex
	onH264Fn func([]byte)

	devAcks, meterAcks int
	ackCtr             uint32
	devAddr            *net.UDPAddr
	mtpresAddr         *net.UDPAddr
	meterRelays        []*net.UDPAddr
	relaysAdded        bool
	gateOpen           bool
	callRetry          int
	callSent           time.Time
	mtpresSeen         bool

	kcp     *kcpPair
	ctlSent int

	// linkWatch 建链看门狗（mu 保护）：呼叫发起后布防，进入 streaming
	//（START）或会话拆除时撤防；超时未建链则 fail 完整拆链。
	linkWatch *time.Timer

	stream   TlvStream
	annex    AnnexStream
	dancer   acceptDance
	started  bool
	accepted bool
	terminal bool
	preStart int
	fwdSent  bool

	cancel  context.CancelFunc
	ctxDone <-chan struct{}
	done    chan struct{}
	stopped atomic.Bool
	// 会话保持（session_hold）：paused 置位期间 TLV4 媒体丢弃（不进
	// onH264Fn）、AvStreamCtl 重发循环退出；KCP pair、meter 应答与 TLV3
	// 信令（含 REJECT/CLOSE 终结）全部保持存活。
	paused atomic.Bool

	kick      chan struct{} // 唤醒等待中的 ctl 重发循环（Pause 后即退）
	ctlActive bool          // mu 保护：ctl 重发循环是否在跑（防双循环并发）
}

// New 建立一路呼叫（不发送任何包；Start 才开始）。link_id 与 AvStreamCtl
// 会话 id 在此独立随机抽取（与官方 SDK 语义一致）。
func New(devID uint32, uid uint64, treatedPwd int, opts Options) *Caller {
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	if opts.CallingRetry <= 0 {
		opts.CallingRetry = 6 * time.Second
	}
	if opts.MeterTicks <= 0 {
		opts.MeterTicks = 200
	}
	if opts.MeterTickEvery <= 0 {
		opts.MeterTickEvery = 100 * time.Millisecond
	}
	token := make([]byte, 16)
	for i, v := range []uint32{1, 0x1011717, 2, 0} {
		Put32(token, i*4, v)
	}
	c := &Caller{
		devID:      devID,
		uid:        uid,
		treatedPwd: treatedPwd,
		pwdEnc:     rc5.GwEncodePassword(uint32(treatedPwd)),
		token:      token,
		fb:         frame.NewBuilder(uid, opts.Creds.C1, opts.Creds.C2, opts.Creds.S1, opts.Creds.S2),
		log:        log,
		tr:         opts.Transport,
		opts:       opts,
		linkID:     RandCallID(),
		sessionID:  RandCallID(),
		state:      StateIdle,
		done:       make(chan struct{}),
		kick:       make(chan struct{}, 1),
	}
	c.dancer.sid = c.sessionID
	return c
}

// IDs 返回 (linkID, sessionID)（诊断用）。
func (c *Caller) IDs() (uint32, uint32) { return c.linkID, c.sessionID }

// Tap 返回 session 读循环挂钩。
func (c *Caller) Tap() TapFunc { return c.handleRawPacket }

// OnH264 注册 H264 NAL 回调（Annex-B，含起始码）。
func (c *Caller) OnH264(fn func(nal []byte)) {
	c.mu.Lock()
	c.onH264Fn = fn
	c.mu.Unlock()
}

// OnState 注册状态回调。
func (c *Caller) OnState(fn func(State, error)) {
	c.stateMu.Lock()
	c.onStateFn = fn
	c.stateMu.Unlock()
}

// State 返回当前状态。paused 位置位且底层未 closed 时叠加报告
// StatePaused（closed 优先于一切）。
func (c *Caller) State() State {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	if c.paused.Load() && c.state != StateClosed {
		return StatePaused
	}
	return c.state
}

// Err 返回进入 closed 时携带的错误（无则 nil）。
func (c *Caller) Err() error {
	c.stateMu.Lock()
	defer c.stateMu.Unlock()
	return c.lastErr
}

func (c *Caller) setState(to State, err error) {
	c.stateMu.Lock()
	from := c.state
	if stateRank[to] < stateRank[from] && to != StateClosed {
		c.stateMu.Unlock()
		return // 只前进不回退（closed 例外）
	}
	if from == StateClosed {
		c.stateMu.Unlock()
		return
	}
	c.state = to
	if err != nil {
		c.lastErr = err
	}
	cb := c.onStateFn
	c.stateMu.Unlock()
	if from != to && cb != nil {
		cb(to, err)
	}
}

// Start 启动呼叫（异步）。Transport 未配置时返回错误。
func (c *Caller) Start(ctx context.Context) error {
	if c.tr == nil {
		return fmt.Errorf("av: no transport")
	}
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel
	c.ctxDone = ctx.Done()
	go func() {
		defer close(c.done)
		c.setState(StateCalling, nil)
		c.sendCalling()
		c.mu.Lock()
		c.callSent = time.Now()
		c.mu.Unlock()
		c.armLinkWatch() // 建链看门狗：病态链路兜底，防止永停 calling
		go c.meterPump()
		<-ctx.Done()
		c.shutdown()
	}()
	return nil
}

// Stop 完整挂断（CLOSE 单发 + 拆链，v1 语义）并等待所有
// goroutine 结束。观众离开且无节流顾虑时用本方法；会话保持（不拆链、
// 同链路复活）见 Pause/Resume。
func (c *Caller) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	<-c.done
}

// shutdown 在 run goroutine 内执行拆除：非终止态时礼貌 CLOSE 单发
// （对齐官方客户端挂断形态：TLV3 经 KCP 直达设备，实测仅发一次，
// 服务器侧不可见）。
func (c *Caller) shutdown() {
	// 拆除即停吞包：生产路径从不显式置位会导致 closed caller 的 tap 仍
	// 消费 meter/KCP 包（handleRawPacket 的 stopped 早退形同虚设）。置位
	// 后晚到包交回 gute 解析路径自然丢弃。
	c.stopped.Store(true)
	c.mu.Lock()
	kp := c.kcp
	terminal := c.terminal
	c.stopLinkWatchLocked() // 会话拆除：撤看门狗（无论成败，不再触发）
	c.mu.Unlock()
	if kp != nil && !terminal && kp.ctl != nil {
		f := BuildAvStreamCtl(c.sessionID, CmdClose, c.token, 0, 0)
		_, _ = kp.ctl.Write(f)
	}
	if kp != nil {
		kp.close()
	}
	c.setState(StateClosed, nil)
}

// ---------------------------------------------------------------------------
// 会话保持（session_hold）：Pause 不拆链，Resume 同链路复活
// ---------------------------------------------------------------------------

// Pause 暂停取流但完整保留呼叫链路：置 paused 位并停止 AvStreamCtl
// 重发循环；KCP pair、meter 应答、TLV 接收（含 REJECT/CLOSE 终结处理）
// 全部存活——tap 仍消费 meter/KCP 包（gute 会话心跳抑制依赖此行为）。
// TLV4 媒体在暂停期丢弃（无观众不做 RC5 解密/RTP 打包）。已 closed 的
// 呼叫为 no-op；重复 Pause 幂等。
func (c *Caller) Pause() {
	if c.State() == StateClosed {
		return
	}
	if c.paused.Swap(true) {
		return
	}
	// 挂起进入边界：丢弃 annex 重组缓冲中的跨期半包——暂停前最后一
	// 批 TLV4 可能滞留未完成 NAL（等下一起始码），若带入恢复期会与
	// 新数据拼合成坏帧（Resume 后首帧花屏）。与在途 Feed 互斥：后者
	// 持 mu 执行，且 paused 置位后新 TLV4 不再进缓冲，重置安全。
	c.mu.Lock()
	c.annex.Reset()
	c.mu.Unlock()
	// 唤醒等待中的重发循环，使其立即回循环头看到 paused 退出（至多
	// 多发一次 cmd=1——设备对重复 cmd=1 幂等，实测）。
	select {
	case c.kick <- struct{}{}:
	default:
	}
	c.log.Info("AV paused (session hold)", "dev", c.devID)
}

// Resume 在同一条活链路上恢复取流：清 paused 位、复位本轮接受态并重跑
// AvStreamCtl cmd=1 ×18 循环；设备再次 ACCEPT（实测：同
// 链路对重复 cmd=1 幂等再 ACCEPT）后舞蹈 [6,1] 自动重放、媒体恢复喂出。
// 未 pause 的 Resume 是 no-op；已 closed 的呼叫为 no-op。媒体门未开过
// （KCP 未建立）时仅清位，待正常建链流程接管。
func (c *Caller) Resume() {
	if c.State() == StateClosed {
		return
	}
	if !c.paused.Swap(false) {
		return
	}
	// 复位本周期接受/舞蹈状态：ctl 循环跑到新的 ACCEPT 才停，舞蹈在
	// 新 ACCEPT 上重放（dancer.observe 的一次性防重需重置）。恢复边
	// 界再复位一次 annex（幂等防御）：确保不存在任何跨暂停期的半包
	// 残留，恢复期首帧从干净的 NAL 边界开始。
	c.mu.Lock()
	c.accepted = false
	c.dancer.fired = false
	c.annex.Reset()
	kp := c.kcp
	c.mu.Unlock()
	c.log.Info("AV resumed (cmd=1 re-issue)", "dev", c.devID)
	if kp != nil && kp.ctl != nil {
		c.goCtlLoop()
	}
}

// goCtlLoop 启动 cmd=1 重发循环。若上一轮尚未退出（Pause kick 后的调度
// 尾巴），先等它退出再起，避免双循环并发发送；等待有界（重复 cmd=1 对
// 设备无害，超时兜底继续）。
func (c *Caller) goCtlLoop() {
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		active := c.ctlActive
		c.mu.Unlock()
		if !active || time.Now().After(deadline) {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	c.mu.Lock()
	c.ctlActive = true
	c.mu.Unlock()
	go func() {
		defer func() {
			c.mu.Lock()
			c.ctlActive = false
			c.mu.Unlock()
		}()
		c.ctlLoop()
	}()
}

// ---------------------------------------------------------------------------
// 建链看门狗（link watchdog）
// ---------------------------------------------------------------------------

// linkWatchBudget 推导看门狗超时窗：meter 泵全预算（MeterTicks×
// MeterTickEvery，默认 200×100ms=20s）+ AvStreamCtl 18 次重发全预算
// （CtlPlan 17 个间隔 ≈6s）+ 2s 余量——覆盖"meter 门最迟开启 + ctl
// 重发跑满"的最坏正常路径，不与现有周期打架；显式配置 LinkWatchdog>0
// 时以配置为准（测试可注入短窗）。
func (c *Caller) linkWatchBudget() time.Duration {
	if c.opts.LinkWatchdog > 0 {
		return c.opts.LinkWatchdog
	}
	var ctl time.Duration
	for _, d := range CtlPlan() {
		ctl += d
	}
	return c.opts.MeterTickEvery*time.Duration(c.opts.MeterTicks) + ctl + 2*time.Second
}

// armLinkWatch 布防建链看门狗：呼叫发起后若在预算窗内未进入 streaming
// （真机形态 CALLING→首帧 300-500ms，正常建链远小于窗口，不会误杀），
// 则主动完整拆链并让媒体源进入既有错误退避路径——被服务端节流/无
// 应答时 Caller 不再永停 StateCalling（meterPump 20s 静默退出、ctlLoop
// 预算耗尽仅打日志，二者都不拆链），槽位得以释放、后续 Acquire 能
// 重新呼叫。
func (c *Caller) armLinkWatch() {
	budget := c.linkWatchBudget()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.linkWatch != nil {
		c.linkWatch.Stop()
	}
	c.linkWatch = time.AfterFunc(budget, c.linkWatchFire)
}

// stopLinkWatchLocked 撤防看门狗（须持 mu）：建链成功（TLV3 START）或
// 会话拆除（shutdown）时调用，保证不与正常流程竞态误杀。
func (c *Caller) stopLinkWatchLocked() {
	if c.linkWatch != nil {
		c.linkWatch.Stop()
		c.linkWatch = nil
	}
}

// linkWatchFire 看门狗触发：仅当仍未建链成功（未 START）、未终结且
// 未拆除时拆链。与 START/拆除的竞态由 mu 下的状态读取 + stopped 兜底：
// 恰在触发前建链成功的会话不拆；fail/cancel 幂等，重复触发无害。
func (c *Caller) linkWatchFire() {
	c.mu.Lock()
	started := c.started
	terminal := c.terminal
	c.linkWatch = nil
	c.mu.Unlock()
	if started || terminal || c.stopped.Load() {
		return
	}
	budget := c.linkWatchBudget()
	c.log.Warn("建链看门狗超时：预算窗内未进入 streaming，主动拆链走重拨退避",
		"dev", c.devID, "state", c.State(), "budget", budget.String())
	c.fail(fmt.Errorf("p2p 建链超时：%.0fs 内未进入 streaming（服务端节流或无应答）", budget.Seconds()))
}

// ---------------------------------------------------------------------------
// 出向信令
// ---------------------------------------------------------------------------

func (c *Caller) sendCalling() {
	var pwdEnc uint32
	if !c.opts.NoPassword {
		pwdEnc = c.pwdEnc
	}
	f := buildCalling(c.fb, c.linkID, c.uid, c.devID, 0x00010000,
		c.opts.Creds.C1, c.opts.Creds.C2, pwdEnc)
	if err := c.tr.Send(f); err != nil {
		c.log.Warn("CALLING send", "err", err)
	} else {
		c.log.Info("CALLING sent", "dev", c.devID, "link_id", c.linkID,
			"session", c.sessionID)
	}
}

// meterPump 周期发送我方 meter_req（参考实现 20ms 档的保守 100ms；设备
// 存活窗口极短需高频——实测经验），并在无设备交互时重发 CALLING（6s×3）。
func (c *Caller) meterPump() {
	for i := 1; i <= c.opts.MeterTicks; i++ {
		select {
		case <-c.ctxDone:
			return
		case <-time.After(c.opts.MeterTickEvery):
		}
		c.mu.Lock()
		dev := c.devAddr
		if dev == nil {
			dev = c.mtpresAddr // 尚无设备首包时，主动探测服务器交付的地址。
		}
		relays := c.meterRelays
		if c.devAcks == 0 && c.callRetry < 3 && time.Since(c.callSent) > c.opts.CallingRetry {
			c.callRetry++
			c.callSent = time.Now()
			devAcks := c.devAcks
			c.mu.Unlock()
			if devAcks == 0 {
				c.sendCalling()
			}
			continue
		}
		c.mu.Unlock()
		if dev == nil && len(relays) == 0 {
			continue
		}
		req := BuildMeterReq(c.uid, c.devID, c.linkID, uint32(i))
		if dev != nil {
			_ = c.tr.SendTo(req, dev)
		}
		for _, a := range relays {
			_ = c.tr.SendTo(req, a)
		}
	}
}

// ---------------------------------------------------------------------------
// 入向：原始包 tap（session 读循环调用）
// ---------------------------------------------------------------------------

// handleRawPacket 入向原始包 tap（session 读循环调用）。消费即归本呼叫：
// meter 族须 link_id@+0x08 与本呼叫一致、KCP 段须 conv 匹配本呼叫双
// conv（ctl=linkID|0x80000000、file=linkID&0x7fffffff，均由 linkID 派生
// 且每次呼叫随机抽取）——杂散/跨呼叫包不消费、交回 gute 解析路径自然
// 丢弃。归属校验同时是会话心跳存活判定的依据：session 侧以「tap 消费」
// 为流量活跃证据清零心跳失联计数，宽泛匹配会让无关包制造假活（永不
// 重连），必须强归属（session M4）。
func (c *Caller) handleRawPacket(pkt []byte, from *net.UDPAddr) bool {
	if c.stopped.Load() {
		return false
	}
	if sub, ok := ClassifyMeter(pkt); ok {
		if len(pkt) < 12 || MeterLinkID(pkt) != c.linkID {
			return false // 杂散 meter 形态包：非本呼叫，不作活跃证据
		}
		c.onMeter(pkt, from, sub)
		return true
	}
	if IsKcpFrame(pkt) {
		if seg, ok := UnwrapKcp(pkt); ok {
			conv := KcpConvOf(seg)
			if conv != c.linkID|0x80000000 && conv != c.linkID&0x7fffffff {
				return false // conv 不属于本呼叫：非本链路流量
			}
			c.mu.Lock()
			kp := c.kcp
			c.mu.Unlock()
			if kp != nil {
				kp.feed(seg, from)
			}
			// ctl 门未开时的本呼叫 KCP 段也消费（gute 解析器不该看到它们）
			return true
		}
	}
	return false
}

func (c *Caller) onMeter(pkt []byte, from *net.UDPAddr, sub byte) {
	var ack []byte
	var open bool
	c.mu.Lock()
	if c.devAddr == nil && from != nil {
		c.devAddr = from
		c.log.Info("device meter source", "addr", from.String())
	}
	switch sub {
	case MeterSubReq:
		c.ackCtr++
		ack = BuildMeterAck(pkt, c.ackCtr)
		c.meterAcks++
		if !c.gateOpen && c.meterAcks >= 6 && c.devAcks >= 6 {
			open = true
		}
	case MeterSubAck:
		c.devAcks++
		if !c.gateOpen && c.devAcks == 6 { // 单侧设备 6 ack 即开门（实测教训的反向保底）
			open = true
		}
	}
	c.mu.Unlock()
	if ack != nil && from != nil {
		_ = c.tr.SendTo(ack, from)
	}
	c.setState(StateMeter, nil)
	if open {
		c.openGate()
	}
}

// openGate — meter 双侧就绪：进入 transfer，建立 KCP 双 conv 会话并开跑
// AvStreamCtl(cmd=1) 循环（与官方 SDK 语义一致）。
func (c *Caller) openGate() {
	c.mu.Lock()
	if c.gateOpen {
		c.mu.Unlock()
		return
	}
	c.gateOpen = true
	dev := c.mtpresAddr
	if dev == nil {
		dev = c.devAddr
	}
	c.mu.Unlock()
	if dev == nil {
		c.log.Warn("ctl gate without device address")
		return
	}
	c.setState(StateTransfer, nil)
	c.log.Info("*** meter gate open — starting KCP pair ***")
	kp := startKcpPair(c.tr, dev, c.linkID, c.devID, uint32(c.uid), c.onKcpBytes, c.log)
	if kp == nil {
		c.fail(fmt.Errorf("kcp pair start failed"))
		return
	}
	c.mu.Lock()
	c.kcp = kp
	c.mu.Unlock()
	c.setState(StateCtl, nil)
	c.goCtlLoop()
}

// ctlLoop — AvStreamCtl(cmd=1) 18 次重发（固定节奏），ACCEPT/REJECT/
// CLOSE/paused 提前停止（裸 INITREQ 回声不停——实测教训）。Pause 经
// kick 唤醒等待、循环头重评即退；Resume 复位 accepted 后重跑本循环。
func (c *Caller) ctlLoop() {
	plan := CtlPlan()
	for i := 0; i <= len(plan); i++ {
		c.mu.Lock()
		done := c.accepted || c.terminal
		kp := c.kcp
		c.mu.Unlock()
		if done || c.stopped.Load() || c.paused.Load() {
			c.log.Info("AvStreamCtl cycle stopped", "sends", i)
			return
		}
		if kp == nil || kp.ctl == nil {
			return
		}
		if _, err := kp.ctl.Write(BuildAvStreamCtl(c.sessionID, CmdInitReq, c.token, c.pwdEnc, 0)); err != nil {
			c.log.Warn("AvStreamCtl write", "err", err)
			return
		}
		c.mu.Lock()
		c.ctlSent++ // 累计（跨 Resume 周期单调，诊断/测试可观测）
		c.mu.Unlock()
		if i < len(plan) {
			select {
			case <-time.After(plan[i]):
			case <-c.ctxDone:
				return
			case <-c.kick: // Pause：回循环头重评（paused → 退出）
			}
		}
	}
	c.log.Warn("AvStreamCtl retry budget exhausted (18 sends, no TLV3)")
}

// ---------------------------------------------------------------------------
// 入向：gute 帧（session gute 解析路径转交 0x3A/0x24）
// ---------------------------------------------------------------------------

// HandleGute 处理 gute 帧；返回 true 表示 av 消费。
func (c *Caller) HandleGute(p *frame.Parsed) bool {
	switch p.Type {
	case 0x3a:
		c.onForward(p)
		return true
	case 0x24:
		c.onMtpres(p)
		return true
	}
	return false
}

func (c *Caller) onForward(p *frame.Parsed) {
	_ = c.tr.Send(c.fb.Ack(0x3a))
	if len(p.Payload) >= 0x20 {
		// Payload 自帧 +0x18 起，故 Payload+0x18/+0x1C 实读帧 +0x30/
		// +0x34（出向 0x3A 布局的地址块区域，语义未实证），按实际
		// 偏移如实标注诊断值，不冒充会话 id。
		c.log.Info("SRV_FORWARD",
			"u32@frame+0x30", fmt.Sprintf("0x%08x", le32(p.Payload[0x18:])),
			"u32@frame+0x34", fmt.Sprintf("0x%08x", le32(p.Payload[0x1C:])))
	}
	c.setState(StateForward, nil)
	c.mu.Lock()
	fired := c.fwdSent
	c.fwdSent = true
	c.mu.Unlock()
	if fired {
		return
	}
	go func() { // 对称应答（callee-mirror）×5
		addr := c.lanIPBytes()
		for i := 0; i < 5; i++ {
			select {
			case <-time.After(600 * time.Millisecond):
			case <-c.ctxDone:
				return
			}
			_ = c.tr.Send(buildSrvForward(c.fb, c.uid, c.devID, c.linkID, addr))
		}
	}()
}

func (c *Caller) onMtpres(p *frame.Parsed) {
	c.mu.Lock()
	first := !c.mtpresSeen
	c.mtpresSeen = true
	c.mu.Unlock()
	// ACK + 0x23 回显仅首发（参考实现不应答重复 MTPRES；逐条应答仅属
	// 兼容冗余——实测服务器呼叫态以 ~3000+/秒持续下发，逐条应答=18 万
	// 垃圾 tx）。地址注册每次都做。
	if first {
		_ = c.tr.Send(c.fb.Ack(0x24))
		if len(p.Payload) >= 0x4C {
			_ = c.tr.Send(buildMtpResRequest(c.fb, c.uid, p.Payload))
		}
	}
	if len(p.Payload) < 0x4C {
		c.log.Info("MTPRES_ACK (short)")
		return
	}
	portBE := int(p.Payload[0x34])<<8 | int(p.Payload[0x35])
	ip := net.IPv4(p.Payload[0x38], p.Payload[0x39], p.Payload[0x3A], p.Payload[0x3B])
	if a, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", ip.String(), portBE)); err == nil && portBE != 0 && !ip.IsUnspecified() {
		c.mu.Lock()
		c.mtpresAddr = a
		kp := c.kcp
		c.mu.Unlock()
		if kp != nil {
			kp.setNatAddr(a) // §6A.8：MTPRES 端点权威于 meter 源地址
		}
		if first {
			c.log.Info("MTPRES peer addr", "addr", a.String())
		}
	}
	c.mu.Lock()
	kp := c.kcp
	addRelays := !c.relaysAdded
	c.relaysAdded = true
	c.mu.Unlock()
	if relays := ParseMtpresRelays(p.Payload); len(relays) > 0 && addRelays {
		var addrs []*net.UDPAddr
		for _, r := range relays {
			if a, err := net.ResolveUDPAddr("udp4",
				fmt.Sprintf("%d.%d.%d.%d:%d", r.IP[0], r.IP[1], r.IP[2], r.IP[3], r.Port)); err == nil {
				addrs = append(addrs, a)
			}
		}
		c.mu.Lock()
		c.meterRelays = append(c.meterRelays, addrs...)
		c.mu.Unlock()
		if kp != nil {
			kp.addRelayDests(relays, c.uid, c.devID, c.linkID)
		}
		c.log.Info("MTPRES relays registered", "n", len(relays))
	}
	c.setState(StateMtpres, nil)
}

// lanIPBytes 构造 {u16 type=2, u16 port BE, u32 ip} LAN 地址块。端口取
// 实测验证值 51881（公网呼叫路径不依赖该块，实测零 0x3A 也能通）。
func (c *Caller) lanIPBytes() []byte {
	b := make([]byte, 8)
	Put16(b, 0, 2)
	const port = 51881
	b[2], b[3] = byte(port>>8), byte(port&0xff)
	ip := c.opts.LanIP
	if ip == nil {
		ip = net.IPv4zero
	}
	copy(b[4:], ip.To4())
	return b
}

// ---------------------------------------------------------------------------
// KCP 重组字节 → TLV → 状态/媒体
// ---------------------------------------------------------------------------

func (c *Caller) onKcpBytes(b []byte) {
	c.mu.Lock()
	frames := c.stream.Feed(b)
	var nals [][]byte
	var dance func(uint32) error
	fireDance := false
	for _, fr := range frames {
		switch fr.Typ {
		case 3:
			if send := c.onTlv3Locked(fr); send != nil {
				dance = send
				fireDance = c.dancer.observe(fr, func(msg string, args ...any) { c.log.Warn(msg, args...) })
			}
		case 4:
			nals = append(nals, c.onTlv4Locked(fr)...)
		default:
			c.log.Debug("TLV other", "type", fr.Typ, "flags", fr.Flags, "len", len(fr.Raw))
		}
	}
	cb := c.onH264Fn
	c.mu.Unlock()

	if fireDance && dance != nil { // ACCEPT 舞蹈：cmd=6→file conv，cmd=1→ctl conv
		c.dancer.fire(dance, func(msg string, args ...any) { c.log.Warn(msg, args...) })
	}
	if cb != nil {
		for _, nal := range nals {
			cb(nal)
		}
	}
}

// onTlv3Locked 处理 TLV3 应答（持有 mu）；ACCEPT 时返回舞蹈发送钩子。
// cmd@帧+0x08、REJECT code@帧+0x0C。
func (c *Caller) onTlv3Locked(fr TlvFrame) func(uint32) error {
	cmd, ok := fr.Cmd()
	if !ok {
		c.log.Info("TLV3 short", "len", len(fr.Raw))
		return nil
	}
	switch cmd {
	case CmdAccept:
		c.accepted = true
		kp := c.kcp
		if kp == nil {
			return nil
		}
		sid, tok, pwd := c.sessionID, c.token, c.pwdEnc
		return func(cmd uint32) error { return kp.danceSend(sid, cmd, tok, pwd) }
	case CmdStart:
		c.started = true
		c.stopLinkWatchLocked() // 建链成功：撤看门狗（防误杀正常会话）
		go c.setState(StateStreaming, nil)
		return nil
	case CmdReject:
		c.terminal = true
		code, _ := fr.RejectCode()
		reset, _ := RejectReset(code)
		go c.fail(fmt.Errorf("设备 REJECT code=%d (app reset 0x%x)", code, reset))
		return nil
	case CmdClose:
		c.terminal = true
		go c.fail(fmt.Errorf("设备 CLOSE"))
		return nil
	}
	return nil
}

// onTlv4Locked 处理 TLV4 媒体帧（持有 mu）：flags&1 → RC5 解密；pre-START
// 帧按参考实现语义丢弃（生产不产出垃圾 NAL）；session_hold
// 暂停期整帧丢弃（不进解密与 NAL 重组——无观众不打包）。
func (c *Caller) onTlv4Locked(fr TlvFrame) [][]byte {
	if c.paused.Load() {
		return nil
	}
	if fr.Flags&1 != 0 {
		DecryptTlv4(fr.Raw)
	}
	if !c.started {
		c.preStart++
		return nil
	}
	return c.annex.Feed(GwAVPayload(fr.Payload()))
}

func (c *Caller) fail(err error) {
	c.setState(StateClosed, err)
	if c.cancel != nil {
		c.cancel() // 触发 run 的 shutdown 拆链（terminal 态不再发 CLOSE）
	}
}

func le32(b []byte) uint32 {
	return uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
}

// Stats 返回一行诊断摘要。
func (c *Caller) Stats() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return fmt.Sprintf("state=%s meter(tx.acks=%d rx.acks=%d) ctl_sent=%d pre_start=%d accepted=%v started=%v paused=%v",
		c.State(), c.meterAcks, c.devAcks, c.ctlSent, c.preStart, c.accepted, c.started, c.paused.Load())
}

// ---------------------------------------------------------------------------
// gute 信令帧构造（与参考实现逐字段一致；帧校验和 = rc5.Chkval）
// ---------------------------------------------------------------------------

// buildCalling — CALLING 0x25/128B（真机实测 ground truth）：
// [0x16]=0x22（0x02 monitor | 0x20 bit5 密码绕过，设备跳过 verifyEncPasswd）；
// +0x68=GwEncodePassword(密码)（callflag≠0 时；bit5 绕过下设备不校验但
// 字段保留）；+0x78={c1,c2} 账号凭据尾。
func buildCalling(fb *frame.Builder, linkID uint32, uid uint64, devID uint32, flags uint32,
	c1, c2, pwdEnc uint32) []byte {
	f := make([]byte, 128)
	f[0x00] = 0x7F
	f[0x01] = 0x25
	f[0x02] = 128
	Put64(f, 0x04, uid)
	Put32(f, 0x0C, fb.SeqNext())
	f[0x14] = 0x05 // mode1 + category bits
	f[0x15] = 0x1d
	f[0x16] = 0x22 // monitor | bit5 密码绕过
	f[0x17] = 0x00
	Put32(f, 0x18, linkID)
	Put64(f, 0x1C, uid)
	Put32(f, 0x24, devID)
	Put32(f, 0x28, 0)
	Put32(f, 0x2C, flags)
	Put32(f, 0x68, pwdEnc)
	Put32(f, 0x78, c1)
	Put32(f, 0x7C, c2)
	Put32(f, 0x10, 0)
	Put32(f, 0x10, rc5.Chkval(f))
	return frame.Encrypt(f)
}

// buildSrvForward — 0x3A/64B 对称应答（与官方 SDK 参考实现一致）：
// dstID@0x18、srcID@0x1C、subtype u8@0x28=3（add address）、type u16@0x2A、
// link_id u32@0x2C、地址块@0x30。
func buildSrvForward(fb *frame.Builder, uid uint64, devID, linkID uint32, addr []byte) []byte {
	f := make([]byte, 64)
	f[0x00] = 0x7F
	f[0x01] = 0x3a
	f[0x02] = 64
	Put64(f, 0x04, uid)
	Put32(f, 0x0C, fb.SeqNext())
	f[0x14] = 0x05
	f[0x15] = 0x1d
	Put32(f, 0x18, devID) // dstID
	Put32(f, 0x1C, uint32(uid))
	f[0x28] = 3       // subtype = add address
	Put16(f, 0x2A, 0) // type = 0
	Put32(f, 0x2C, linkID)
	copy(f[0x30:], addr) // {u16 type=2, u16 port BE, u32 ip}
	Put32(f, 0x10, 0)
	Put32(f, 0x10, rc5.Chkval(f))
	return frame.Encrypt(f)
}

// buildMtpResRequest — MTP 通道请求帧（0x23/104B）：
// 回显 MTPRES(0x24) 帧的路由块，flags3|=1（want-ack）。
func buildMtpResRequest(fb *frame.Builder, uid uint64, mtpres []byte) []byte {
	f := make([]byte, 0x68)
	f[0x00] = 0x7F
	f[0x01] = 0x23
	Put16(f, 0x02, 0x68)
	Put64(f, 0x04, uid)
	Put32(f, 0x0C, fb.SeqNext())
	f[0x14] = 0x01 // RC5 mode1
	f[0x16] = 0x01 // want-ack
	if len(mtpres) >= 0x4C {
		copy(f[0x18:0x1C], mtpres[0x00:0x04])
		copy(f[0x1C:0x24], mtpres[0x04:0x0C])
		copy(f[0x24:0x2C], mtpres[0x0C:0x14])
		copy(f[0x2C:0x48], mtpres[0x14:0x30])
		copy(f[0x48:0x64], mtpres[0x30:0x4C])
	}
	Put32(f, 0x10, 0)
	Put32(f, 0x10, rc5.Chkval(f))
	return frame.Encrypt(f)
}
