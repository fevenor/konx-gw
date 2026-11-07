// Package media implements the stream source layer (DESIGN §5.2):
// explicit source selection (external-rtsp / device-rtsp / off), reference
// counting with idle stop, event prewarm hold, and RTSP re-distribution.
package media

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

// Profile names a stream variant.
type Profile string

const (
	ProfileMain Profile = "main"
	ProfileSub  Profile = "sub"
)

// source 是媒体源槽位的统一契约（rtspSource / p2pSource 实现）。
type source interface {
	Desc() *description.Session
	SetSink(fn func(*description.Media, *rtp.Packet))
	Start()
	Stop()
	States() <-chan SourceState
}

// pausableSource 是支持会话保持（session_hold）的源：暂停保留呼叫链路
// 与描述缓存，恢复即同链路重新喂流（目前仅 p2pSource 实现；Manager 的
// 空闲/唤醒路径按此可选能力分流，其余源维持既有拆链语义）。
type pausableSource interface {
	Pause()
	Resume()
}

// State of one source slot.
type State string

const (
	StateStopped  State = "stopped"
	StateStarting State = "starting"
	StateActive   State = "active"
	StateError    State = "error"
	// StatePaused：p2p-av 会话保持（media.p2p.session_hold）——观众清零
	// 后源不拆链只暂停（KCP/meter 存活，同链路重发 cmd=1 即复活），描述
	// 缓存保留，DESCRIBE 照常服务。
	StatePaused State = "paused"
)

// SourceState is emitted on every transition.
type SourceState struct {
	Device  string
	Profile Profile
	State   State
	Err     error
}

// rtspSource pulls one upstream RTSP URL and forwards RTP packets to a
// sink. It implements reconnect with capped backoff.
type rtspSource struct {
	url       string
	transport gortsplib.Transport
	log       *slog.Logger

	mu      sync.Mutex
	sink    func(media *description.Media, pkt *rtp.Packet)
	stateCh chan SourceState
	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool // 拉流循环存活标记：Start 幂等（退避/连接中不重复起）

	// emitMu 串行化 emit 与 Stop 的通道收尾：close 后不再发送（防
	// send-on-closed-channel panic），缓冲保序（终态先入队再关闭）。
	emitMu        sync.Mutex
	stateChClosed bool

	desc *description.Session
}

func newRTSPSource(url, transport string, log *slog.Logger) *rtspSource {
	s := &rtspSource{url: url, log: log, stateCh: make(chan SourceState, 16), done: make(chan struct{})}
	if transport == "udp" {
		s.transport = gortsplib.TransportUDP
	} else {
		s.transport = gortsplib.TransportTCP // device prefers interleaved (F9)
	}
	return s
}

// Desc returns the negotiated upstream description once ready.
func (s *rtspSource) Desc() *description.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.desc
}

// SetSink installs the packet consumer (must be set before Start).
func (s *rtspSource) SetSink(fn func(*description.Media, *rtp.Packet)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = fn
}

func (s *rtspSource) emit(st State, err error) {
	s.emitMu.Lock()
	defer s.emitMu.Unlock()
	if s.stateChClosed {
		return
	}
	select {
	case s.stateCh <- SourceState{State: st, Err: err}:
	default:
	}
}

// Start launches the pull loop in the background. 幂等：循环已在跑
// （连接中/退避重连中）时二次 Start 直接返回，不再叠加第二个循环与
// 状态风暴；ctx 挂到 s.cancel 供 Stop 取消（不会被并发的 Start 覆盖）。
func (s *rtspSource) Start() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	go func() {
		defer s.running.Store(false)
		defer close(s.done)
		s.emit(StateStarting, nil)
		backoff := time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			err := s.pull(ctx)
			if ctx.Err() != nil {
				return
			}
			s.emit(StateError, err)
			s.log.Warn("rtsp source error, retrying", "url", redact(s.url), "err", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff *= 2
			if backoff > 15*time.Second {
				backoff = 15 * time.Second
			}
		}
	}()
}

// Stop cancels the loop and waits. 终态 StateStopped 先入通道（缓冲保
// 序）再关闭通道——监听方 for-range 正常退出不再泄漏；幂等（重复 Stop
// 不二次 close）。
func (s *rtspSource) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	<-s.done
	s.emit(StateStopped, nil)
	s.emitMu.Lock()
	if !s.stateChClosed {
		s.stateChClosed = true
		close(s.stateCh)
	}
	s.emitMu.Unlock()
}

// States 返回状态通道（source 契约）。
func (s *rtspSource) States() <-chan SourceState { return s.stateCh }

func (s *rtspSource) pull(ctx context.Context) error {
	u, err := base.ParseURL(s.url)
	if err != nil {
		return fmt.Errorf("url: %w", err)
	}
	c := &gortsplib.Client{
		Scheme:    u.Scheme,
		Host:      u.Host,
		Transport: &s.transport,
	}
	if err := c.Start2(); err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer c.Close()
	desc, _, err := c.Describe(u)
	if err != nil {
		return fmt.Errorf("describe: %w", err)
	}
	if err := c.SetupAll(desc.BaseURL, desc.Medias); err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	s.mu.Lock()
	s.desc = desc
	sink := s.sink
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.desc = nil
		s.mu.Unlock()
	}()

	c.OnPacketRTPAny(func(m *description.Media, _ format.Format, pkt *rtp.Packet) {
		if sink != nil {
			sink(m, pkt)
		}
	})
	s.emit(StateActive, nil)
	if _, err := c.Play(nil); err != nil {
		return fmt.Errorf("play: %w", err)
	}
	errCh := make(chan error, 1)
	go func() { errCh <- c.Wait() }()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func redact(u string) string {
	if i := strings.Index(u, "@"); i > 0 {
		if j := strings.Index(u, "://"); j > 0 {
			return u[:j+3] + "***" + u[i:]
		}
	}
	return u
}

// ---------------------------------------------------------------------------
// slot: one (device, profile) source with refcount + idle stop
// ---------------------------------------------------------------------------

type slot struct {
	device  string
	profile Profile
	src     source

	mu        sync.Mutex
	refs      int
	lastFull  time.Time
	idleTimer *time.Timer
	idleDur   time.Duration // p2p-av 用 HardIdle，其余用 IdleStop
	state     State
	lastErr   error
	desc      *description.Session
	pktSink   func(*description.Media, *rtp.Packet)
	paused    bool // session_hold：源处于暂停保持（m.mu 与 state 同锁保护）
}

func (sl *slot) statePublish() {
	sl.mu.Lock()
	st := sl.state
	err := sl.lastErr
	desc := sl.desc
	sink := sl.pktSink
	sl.mu.Unlock()
	_ = st
	_ = err
	_ = desc
	_ = sink
}

// Manager owns one slot per (device, profile).
type Manager struct {
	cfg *config.App
	log *slog.Logger
	eng *events.Engine

	mu    sync.Mutex
	slots map[string]*slot // key device|profile

	// 状态订阅（多订阅：app 接线 API 广播与 rtspout OnSourceState 等）。
	// subsMu 仅保护 subs 切片本身；回调在锁外遍历调用（回调可能反向
	// 进入 Manager/Desc 等，持锁调用会自锁）。
	subsMu sync.Mutex
	subs   []func(SourceState)

	// 槽位 egress writer 注册表（实例化：多 Manager 实例互不串味，
	// 测试可并行）。
	wmu     sync.Mutex
	writers map[string]func(*description.Media, *rtp.Packet)

	ffmpeg string

	// p2p-av 注入（app 接线）：呼叫建立器（生产=session.AttachAV）与
	// treated 密码解析器（cloud LoadDeviceInfo→gwpass 链）。
	p2pDial P2PDialer
	p2pPwd  P2PPwdResolver
}

func NewManager(cfg *config.App, log *slog.Logger, eng *events.Engine) *Manager {
	m := &Manager{
		cfg: cfg, log: log, eng: eng,
		slots:   map[string]*slot{},
		writers: map[string]func(*description.Media, *rtp.Packet){},
	}
	return m
}

// SetP2PDialer 安装 p2p-av 呼叫建立器。
func (m *Manager) SetP2PDialer(d P2PDialer) { m.p2pDial = d }

// SetP2PPwdResolver 安装 treated 密码解析器。
func (m *Manager) SetP2PPwdResolver(r P2PPwdResolver) { m.p2pPwd = r }

// OnState 追加一个状态变迁订阅（多订阅，按注册序遍历调用）。
func (m *Manager) OnState(fn func(SourceState)) {
	m.subsMu.Lock()
	m.subs = append(m.subs, fn)
	m.subsMu.Unlock()
}

func (m *Manager) emit(ss SourceState) {
	m.subsMu.Lock()
	subs := make([]func(SourceState), len(m.subs))
	copy(subs, m.subs)
	m.subsMu.Unlock()
	// 锁外调用：订阅方（如 rtspout OnSourceState）可能反查 Manager
	// 的 Desc/Release 等持 m.mu 的方法。
	for _, fn := range subs {
		fn(ss)
	}
	if m.eng != nil {
		m.eng.Submit(events.Event{
			DeviceID: ss.Device, Type: events.TypeSource, TSRecv: time.Now().Unix(),
			Payload: fmt.Sprintf(`{"profile":"%s","state":"%s","err":%q}`, ss.Profile, ss.State, errStr(ss.Err)),
		})
	}
}

func errStr(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// sourceURL resolves the upstream URL for a device/profile from config.
func (m *Manager) sourceURL(dev config.Device, p Profile) (string, error) {
	switch m.cfg.Media.Source {
	case "external-rtsp":
		u := m.cfg.Media.ExternalURL
		u = strings.ReplaceAll(u, "{profile}", string(p))
		u = strings.ReplaceAll(u, "{device}", dev.ID)
		return u, nil
	case "device-rtsp":
		pass := dev.RTSP.Pass
		if pass == "auto" || pass == "" {
			pass = deviceRTSPPassword(dev.ID)
		}
		path := "/onvif1"
		if p == ProfileSub {
			path = "/onvif2"
		}
		user := dev.RTSP.User
		if user == "" {
			user = "admin"
		}
		return fmt.Sprintf("rtsp://%s:%s@%s:554%s", user, pass, dev.IP, path), nil
	default:
		return "", fmt.Errorf("media source %q does not provide streams", m.cfg.Media.Source)
	}
}

func (sl *slot) key() string { return sl.device + "|" + string(sl.profile) }

// normProfile 统一 profile 键规范化：p2p-av 单码流设备只有 main，
// sub 请求（Web 实时页/HLS/回环管线）一律映射到 main。Acquire/HasSlot/
// Desc/Release/RegisterWriter 必须走同一映射，否则槽位键错位（如回环
// /sub DESCRIBE 永远 404、writer 注册键与 sink 查询键不一致导致零帧）。
func (m *Manager) normProfile(p Profile) Profile {
	if m.cfg.Media.Source == "p2p-av" && p == ProfileSub {
		return ProfileMain
	}
	return p
}

// NormProfile 导出键规范化（rtspout 出口层统一归一 profile 键用：
// streams/sessions/RegisterWriter/UnregisterWriter/Desc/Release 必须
// 与 Acquire 走同一映射，否则写者注册键与查询键错位互踩）。
// cfg.Media.Source 启动定死，无需加锁。
func (m *Manager) NormProfile(p Profile) Profile { return m.normProfile(p) }

// Acquire takes a reference, starting the source if needed. The returned
// release func must be called when the consumer goes away.
func (m *Manager) Acquire(deviceID string, p Profile) (func(), error) {
	dev, ok := m.lookupDevice(deviceID)
	if !ok {
		return nil, fmt.Errorf("unknown device %q", deviceID)
	}
	if np := m.normProfile(p); np != p {
		m.log.Info("p2p-av has no sub profile, falling back to main", "device", deviceID)
		p = np
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	k := deviceID + "|" + string(p)
	sl, ok := m.slots[k]
	if !ok {
		sl = &slot{device: deviceID, profile: p}
		log := m.log.With("device", deviceID, "profile", p)
		switch m.cfg.Media.Source {
		case "p2p-av":
			devID, err := strconv.ParseUint(dev.ID, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("p2p-av 需要数字设备 id，%q: %w", dev.ID, err)
			}
			treated := m.cfg.Media.P2P.TreatedPassword
			if treated <= 0 {
				if m.p2pPwd == nil {
					return nil, fmt.Errorf("p2p-av 缺 treated 密码（media.p2p.treated_password 或密码解析器）")
				}
				treated, err = m.p2pPwd(uint32(devID))
				if err != nil {
					return nil, fmt.Errorf("解析 treated 密码: %w", err)
				}
			}
			if m.p2pDial == nil {
				return nil, fmt.Errorf("p2p-av 未接线（SetP2PDialer）")
			}
			sl.src = newP2PSource(uint32(devID), treated, m.p2pDial, log)
			sl.idleDur = m.cfg.Media.P2P.HardIdle
			if sl.idleDur <= 0 {
				sl.idleDur = m.cfg.Media.IdleStop
			}
		default:
			url, err := m.sourceURL(dev, p)
			if err != nil {
				return nil, err
			}
			sl.src = newRTSPSource(url, dev.RTSP.Transport, log)
			sl.idleDur = m.cfg.Media.IdleStop
		}
		sl.src.SetSink(func(md *description.Media, pkt *rtp.Packet) {
			// packets are consumed by the egress layer via stream writer
			// registered separately (see rtspout).
			if fn := m.writerFor(k); fn != nil {
				fn(md, pkt)
			}
		})
		hold := m.cfg.Media.Source == "p2p-av" && m.cfg.Media.P2P.SessionHold
		go func() {
			for st := range sl.src.States() {
				// 槽位字段统一持 sl.mu 写（锁序 m.mu → sl.mu），
				// 监听协程不再占 m.mu；refs 同临界区取走供下方
				// 会话保持死亡拆槽判定（stopSlot 内会再校验）。
				sl.mu.Lock()
				sl.state = st.State
				sl.lastErr = st.Err
				sl.desc = sl.src.Desc()
				sl.paused = st.State == StatePaused
				refs := sl.refs
				sl.mu.Unlock()
				m.emit(SourceState{Device: deviceID, Profile: p, State: st.State, Err: st.Err})
				// session_hold 保护：保持中的呼叫死亡（REJECT/链路断，
				// 源进入退避重拨）且无观众——拆掉槽位终止重拨循环，避免
				// 无人观看时无限重呼烧服务器节流窗；下次观看全新 Acquire。
				if hold && st.State == StateError && refs == 0 {
					m.stopSlot(k)
				}
			}
		}()
		m.slots[k] = sl
	}
	sl.mu.Lock()
	if sl.idleTimer != nil {
		sl.idleTimer.Stop()
		sl.idleTimer = nil
	}
	wasIdle := sl.refs == 0
	// session_hold：暂停保持中的源不重新 dial/Start——同一条活链路上
	// 重发 cmd=1 复活（实测；重呼会撞服务器 15-30 分钟节流窗）。
	resumeHold := wasIdle && sl.paused
	sl.paused = false
	sl.refs++
	sl.mu.Unlock()
	if resumeHold {
		if ps, ok := sl.src.(pausableSource); ok {
			ps.Resume()
		}
	} else {
		// Start 幂等（rtspSource running CAS / p2pSource 既有 CAS）：
		// 已在跑（含退避重连中）直接返回，不再依赖 state 判断——
		// 消掉锁外读 state 的判断窗口与重复起循环。
		sl.src.Start()
	}
	rel := func() { m.release(k) }
	return rel, nil
}

func (m *Manager) release(k string) {
	m.mu.Lock()
	sl, ok := m.slots[k]
	if !ok {
		m.mu.Unlock()
		return
	}
	sl.mu.Lock()
	if sl.refs > 0 {
		sl.refs--
	}
	zero := sl.refs == 0
	idle := sl.idleDur
	if zero && sl.idleTimer == nil && idle > 0 {
		sl.idleTimer = time.AfterFunc(idle, func() { m.stopSlot(k) })
	}
	sl.mu.Unlock()
	m.mu.Unlock()
}

func (m *Manager) stopSlot(k string) {
	m.mu.Lock()
	sl, ok := m.slots[k]
	if !ok {
		m.mu.Unlock()
		return
	}
	// C0-2：refs/state/idleTimer 读取并入同一 sl.mu 临界区（锁序
	// m.mu → sl.mu），消灭「读后放锁、判前被 Acquire」的 TOCTOU 窗口。
	sl.mu.Lock()
	if sl.refs != 0 {
		// 观众回流：放弃本次空闲拆除（清掉已触发的 timer 句柄），
		// 等下一轮 refs 归零再计时。
		sl.idleTimer = nil
		sl.mu.Unlock()
		m.mu.Unlock()
		return
	}
	// session_hold（media.p2p.session_hold，仅 p2p-av）：空闲到期且呼叫
	// 健康（Active）时不拆链——真机实证服务器对 CLOSE 拆链后的重呼有
	// 15-30 分钟节流（实测教训），改为暂停保持（KCP/meter 存活，
	// 同链路重发 cmd=1 即复活）；槽位与已合成描述保留，新观众 Acquire
	// 走 Resume。非健康态（未就绪/已错）维持既有拆链路径。
	var pauseSrc pausableSource
	if m.cfg.Media.P2P.SessionHold && sl.state == StateActive {
		if ps, ok := sl.src.(pausableSource); ok {
			pauseSrc = ps
			sl.idleTimer = nil
		}
	}
	sl.mu.Unlock()
	if pauseSrc != nil {
		m.mu.Unlock()
		pauseSrc.Pause()
		return
	}
	delete(m.slots, k)
	m.mu.Unlock()
	sl.src.Stop()
}

// Prewarm starts a source for a short window after a doorbell event even
// with no downstream consumer yet (DESIGN §5.2 event_stream_hold).
func (m *Manager) Prewarm(deviceID string) {
	rel, err := m.Acquire(deviceID, ProfileMain)
	if err != nil {
		return
	}
	time.AfterFunc(m.cfg.Media.EventStreamHold, rel)
}

func (m *Manager) lookupDevice(id string) (config.Device, bool) {
	for _, d := range m.cfg.Devices {
		if d.ID == id {
			return d, true
		}
	}
	return config.Device{}, false
}

// HasSlot 报告 (device, profile) 是否存在已建立的源槽位（rtspout 的
// DESCRIBE 等待语义用：有槽位=取流已发起、只是未就绪；无槽位立即 404）。
// p2p-av 的 sub 与 Acquire 同步归一到 main。
func (m *Manager) HasSlot(deviceID string, p Profile) bool {
	p = m.normProfile(p)
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.slots[deviceID+"|"+string(p)]
	return ok
}

// StateOf reports slot states for the API.
func (m *Manager) StateOf(deviceID string) map[Profile]State {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[Profile]State{}
	for _, p := range []Profile{ProfileMain, ProfileSub} {
		k := deviceID + "|" + string(p)
		if sl, ok := m.slots[k]; ok {
			sl.mu.Lock()
			out[p] = sl.state
			sl.mu.Unlock()
		} else {
			out[p] = StateStopped
		}
	}
	return out
}

// Desc returns the live upstream description (for egress DESCRIBE).
func (m *Manager) Desc(deviceID string, p Profile) *description.Session {
	p = m.normProfile(p)
	m.mu.Lock()
	defer m.mu.Unlock()
	k := deviceID + "|" + string(p)
	if sl, ok := m.slots[k]; ok {
		return sl.src.Desc()
	}
	return nil
}

func (m *Manager) registerWriter(k string, fn func(*description.Media, *rtp.Packet)) {
	m.wmu.Lock()
	m.writers[k] = fn
	m.wmu.Unlock()
}

func (m *Manager) writerFor(k string) func(*description.Media, *rtp.Packet) {
	m.wmu.Lock()
	defer m.wmu.Unlock()
	return m.writers[k]
}

// RegisterWriter installs the egress writer for one slot.
func (m *Manager) RegisterWriter(deviceID string, p Profile, fn func(*description.Media, *rtp.Packet)) {
	m.registerWriter(deviceID+"|"+string(m.normProfile(p)), fn)
}

// UnregisterWriter 移除槽位的 egress writer（rtspout 拆出口流条目时
// 对称调用，防止拆链尾包写入已关闭的 ServerStream）。
func (m *Manager) UnregisterWriter(deviceID string, p Profile) {
	k := deviceID + "|" + string(m.normProfile(p))
	m.wmu.Lock()
	delete(m.writers, k)
	m.wmu.Unlock()
}

// StopAll stops every slot (shutdown).
func (m *Manager) StopAll() {
	m.mu.Lock()
	slots := make([]*slot, 0, len(m.slots))
	for _, sl := range m.slots {
		slots = append(slots, sl)
	}
	m.slots = map[string]*slot{}
	m.mu.Unlock()
	for _, sl := range slots {
		sl.src.Stop()
	}
}

// Release drops one reference previously taken by Acquire.
func (m *Manager) Release(deviceID string, p Profile) {
	m.release(deviceID + "|" + string(m.normProfile(p)))
}
