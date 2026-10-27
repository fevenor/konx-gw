// KCP 双 conv 数据通道（与官方 SDK 参考实现一致）：
//
//	ctl/media conv = link_id|0x80000000（AvStreamCtl + TLV3 应答）
//	file conv      = link_id&0x7fffffff（舞蹈 cmd=6；实测设备 START(6)
//	                 与 TLV4 媒体也走此 conv）
//
// 双会话共享同一目的地集（NAT 4B 封包头 / relay 12B 封包头 / 裸发变体，
// 与真机验证的发送形态一致），入向按 KCP 头 conv u32 分流；参数与参考
// 实现一致：ikcp_nodelay(1,5,200,1)、窗口 64/64。
package av

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	kcp "github.com/xtaci/kcp-go/v5"
)

// Transport 是 av 呼叫的原始 UDP 通道（生产由 session 提供：与 gute 会话
// 共享同一 socket）。Send 发往 gute P2P 服务器，SendTo 定向设备 NAT/relay。
type Transport interface {
	Send(b []byte) error
	SendTo(b []byte, addr *net.UDPAddr) error
}

// ---------------------------------------------------------------- 出口

type kcpDest struct {
	addr  *net.UDPAddr
	relay bool
	raw   bool   // 无封包头直发（设备 meter 帧即裸帧；真机验证的发送变体之一）
	route uint64 // relay 转发键（term id）
	tag   string
}

// kcpEgress 持有目的地集合：双 conv 会话共享的出向封包出口。
type kcpEgress struct {
	tr  Transport
	mu  sync.Mutex
	dst []kcpDest

	txPkts atomic.Int64
}

func newKcpEgress(tr Transport) *kcpEgress { return &kcpEgress{tr: tr} }

func (e *kcpEgress) addDest(d kcpDest) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.dst = append(e.dst, d)
}

// write 把一个 KCP 段发送到全部目的地。
func (e *kcpEgress) write(p []byte) (int, error) {
	e.mu.Lock()
	dst := append([]kcpDest(nil), e.dst...)
	e.mu.Unlock()
	for _, d := range dst {
		var f []byte
		switch {
		case d.raw:
			f = p
		case d.relay:
			f = WrapKcpRelay(p, d.route)
		default:
			f = WrapKcp(p)
		}
		if f == nil {
			continue
		}
		if err := e.tr.SendTo(f, d.addr); err == nil {
			e.txPkts.Add(1)
		}
	}
	if len(dst) == 0 {
		return 0, errors.New("no kcp destinations")
	}
	return len(p), nil
}

// setNatAddr 把 nat* 目的地重定向到 MTPRES_ACK 交付的对端公网端点
// （wire 0x4C/0x50，§6A.8——权威于 meter 包源地址）。
func (e *kcpEgress) setNatAddr(a *net.UDPAddr) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for i := range e.dst {
		if strings.HasPrefix(e.dst[i].tag, "nat") {
			e.dst[i].addr = a
		}
	}
}

// ---------------------------------------------------------------- 会话视图

type kcpFeedItem struct {
	data []byte
	addr net.Addr
}

// mtpKcpConn 是单个 kcp-go 会话的 PacketConn 视图：入向队列由 conv 分流
// 器投递，出向委托共享出口。
type mtpKcpConn struct {
	eg    *kcpEgress
	ch    chan kcpFeedItem
	rdmu  sync.Mutex
	rdl   time.Time
	once  sync.Once
	clost chan struct{}
}

func newMtpKcpConn(eg *kcpEgress) *mtpKcpConn {
	return &mtpKcpConn{eg: eg, ch: make(chan kcpFeedItem, 4096), clost: make(chan struct{})}
}

func (c *mtpKcpConn) feed(data []byte, addr net.Addr) {
	cp := append([]byte(nil), data...)
	select {
	case c.ch <- kcpFeedItem{data: cp, addr: addr}:
	default:
	}
}

var errKcpTimeout = errors.New("i/o timeout")

func (c *mtpKcpConn) ReadFrom(p []byte) (int, net.Addr, error) {
	c.rdmu.Lock()
	dl := c.rdl
	c.rdmu.Unlock()
	var t <-chan time.Time
	if !dl.IsZero() {
		d := time.Until(dl)
		if d <= 0 {
			return 0, nil, errKcpTimeout
		}
		t = time.After(d)
	}
	select {
	case f := <-c.ch:
		n := copy(p, f.data)
		return n, f.addr, nil
	case <-t:
		return 0, nil, errKcpTimeout
	case <-c.clost:
		return 0, nil, net.ErrClosed
	}
}

func (c *mtpKcpConn) WriteTo(p []byte, _ net.Addr) (int, error) { return c.eg.write(p) }

// stubAddr 占位 LocalAddr（kcp-go 客户端会话不使用该值）。
type stubAddr struct{}

func (stubAddr) Network() string { return "udp" }
func (stubAddr) String() string  { return "mtp-kcp" }

func (c *mtpKcpConn) LocalAddr() net.Addr { return stubAddr{} }
func (c *mtpKcpConn) Close() error        { c.once.Do(func() { close(c.clost) }); return nil }
func (c *mtpKcpConn) SetDeadline(t time.Time) error {
	return c.SetReadDeadline(t)
}
func (c *mtpKcpConn) SetReadDeadline(t time.Time) error {
	c.rdmu.Lock()
	c.rdl = t
	c.rdmu.Unlock()
	return nil
}
func (c *mtpKcpConn) SetWriteDeadline(time.Time) error { return nil }

// ---------------------------------------------------------------- 分流

// kcpDemux 按 KCP 头 conv u32 路由入向段；未注册 conv 计数丢弃。
type kcpDemux struct {
	mu    sync.RWMutex
	flows map[uint32]*mtpKcpConn

	unrouted atomic.Int64
}

func newKcpDemux() *kcpDemux { return &kcpDemux{flows: map[uint32]*mtpKcpConn{}} }

func (d *kcpDemux) addFlow(conv uint32, ad *mtpKcpConn) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.flows[conv] = ad
}

func (d *kcpDemux) feed(seg []byte, addr net.Addr) bool {
	conv := KcpConvOf(seg)
	d.mu.RLock()
	ad := d.flows[conv]
	d.mu.RUnlock()
	if ad == nil {
		d.unrouted.Add(1)
		return false
	}
	ad.feed(seg, addr)
	return true
}

// ---------------------------------------------------------------- 双会话

// kcpPair 持有双 conv 会话及其共享出口。
type kcpPair struct {
	ctl   *kcp.UDPSession
	file  *kcp.UDPSession
	eg    *kcpEgress
	demux *kcpDemux
	log   *slog.Logger
}

// RelayEntry — MTPRES relay 候选（p2pu_add_mtpchn_with_mtpres @0x121d6c：
// payload+0x4E 计数字节，+0x58 起 16B 条目 {flags, u16 port BE, u32 ip BE,
// u32 extra}，钳到 32）。
type RelayEntry struct {
	IP   [4]byte
	Port int
	TCP  bool
}

// ParseMtpresRelays 解析 MTPRES_ACK payload 的 relay 列表。
func ParseMtpresRelays(payload []byte) []RelayEntry {
	if len(payload) < 0x58 {
		return nil
	}
	cnt := int(payload[0x4E])
	if cnt > 0x20 {
		cnt = 0x20
	}
	var out []RelayEntry
	for i := 0; i < cnt; i++ {
		e := payload[0x58+16*i:]
		if len(e) < 8 {
			break
		}
		out = append(out, RelayEntry{
			IP:   [4]byte{e[4], e[5], e[6], e[7]},
			Port: int(e[2])<<8 | int(e[3]), // BE
			TCP:  e[0]>>2&1 == 1,
		})
	}
	return out
}

// startKcpPair 建立（meter 门控打开后的）双 KCP 会话：目的地集与
// 真机验证形态一致（nat 4B / nat 裸发 / nat relay×{link,dev,uid} 路由键）。
// onBytes 收到任一会话重组出的字节流（TLV3 ctl 与 TLV4 媒体，实测
// 双 conv 均有）。
func startKcpPair(tr Transport, dev *net.UDPAddr, linkID, devID, uid uint32,
	onBytes func([]byte), log *slog.Logger) *kcpPair {
	eg := newKcpEgress(tr)
	eg.addDest(kcpDest{addr: dev, tag: "nat"})
	eg.addDest(kcpDest{addr: dev, raw: true, tag: "nat-raw"})
	eg.addDest(kcpDest{addr: dev, relay: true, route: uint64(linkID), tag: "nat-rly-link"})
	eg.addDest(kcpDest{addr: dev, relay: true, route: uint64(devID), tag: "nat-rly-dev"})
	eg.addDest(kcpDest{addr: dev, relay: true, route: uint64(uid), tag: "nat-rly-uid"})

	ctlAd := newMtpKcpConn(eg)
	fileAd := newMtpKcpConn(eg)
	demux := newKcpDemux()
	ctlConv := linkID | 0x80000000
	fileConv := linkID & 0x7fffffff
	demux.addFlow(ctlConv, ctlAd)
	demux.addFlow(fileConv, fileAd)

	kp := &kcpPair{eg: eg, demux: demux, log: log}
	ctl, err := kcp.NewConn3(ctlConv, dev, nil, 0, 0, ctlAd)
	if err != nil {
		log.Error("kcp ctl session", "err", err)
		return nil
	}
	ctl.SetNoDelay(1, 5, 200, 1)
	ctl.SetWindowSize(64, 64)
	file, ferr := kcp.NewConn3(fileConv, dev, nil, 0, 0, fileAd)
	if ferr != nil {
		log.Error("kcp file session (ACCEPT 舞蹈降级)", "err", ferr)
		file = nil
	} else {
		file.SetNoDelay(1, 5, 200, 1)
		file.SetWindowSize(64, 64)
	}
	kp.ctl, kp.file = ctl, file
	log.Info("kcp pair up", "ctl_conv", fmt.Sprintf("0x%08x", ctlConv),
		"file_conv", fmt.Sprintf("0x%08x", fileConv), "dev", dev.String())

	for _, s := range []*kcp.UDPSession{ctl, file} {
		if s == nil {
			continue
		}
		sess := s
		go func() {
			buf := make([]byte, 65536)
			for {
				n, err := sess.Read(buf)
				if err != nil {
					return
				}
				onBytes(append([]byte(nil), buf[:n]...))
			}
		}()
	}
	return kp
}

// feed 入向已解封的 KCP 段：按 conv 分流。
func (k *kcpPair) feed(seg []byte, addr net.Addr) { k.demux.feed(seg, addr) }

// setNatAddr 重定向 nat* 目的地（MTPRES 权威地址）。
func (k *kcpPair) setNatAddr(a *net.UDPAddr) { k.eg.setNatAddr(a) }

// addRelayDests 注册 MTPRES relay 候选：12B 封包头 × 双路由键 + 4B 直发。
func (k *kcpPair) addRelayDests(relays []RelayEntry, uid uint64, devID, linkID uint32) {
	for _, r := range relays {
		a, err := net.ResolveUDPAddr("udp4",
			fmt.Sprintf("%d.%d.%d.%d:%d", r.IP[0], r.IP[1], r.IP[2], r.IP[3], r.Port))
		if err != nil {
			continue
		}
		k.eg.addDest(kcpDest{addr: a, relay: true, route: uid, tag: "rly-uid"})
		k.eg.addDest(kcpDest{addr: a, relay: true, route: uint64(devID), tag: "rly-dev"})
		k.eg.addDest(kcpDest{addr: a, relay: true, route: uint64(linkID), tag: "rly-link"})
		k.eg.addDest(kcpDest{addr: a, tag: "rly-plain"})
	}
}

// danceSend 发送 ACCEPT 舞蹈帧（布局路由与官方 SDK 参考实现一致：
// cmd==6 → mtp+0x57c file conv；其余 → mtp+0x584 ctl conv）。
func (k *kcpPair) danceSend(sessionID, cmd uint32, token []byte, pwdEnc uint32) error {
	s := k.ctl
	if cmd == 6 {
		s = k.file
	}
	if s == nil {
		return fmt.Errorf("kcp %d-conv session unavailable", cmd)
	}
	if _, err := s.Write(BuildAvStreamCtl(sessionID, cmd, token, pwdEnc, 0)); err != nil {
		return err
	}
	return nil
}

// close 关闭两会话（自适应 nil file 会话）。
func (k *kcpPair) close() {
	if k.ctl != nil {
		_ = k.ctl.Close()
	}
	if k.file != nil {
		_ = k.file.Close()
	}
}
