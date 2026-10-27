// Package session implements the push-channel client state machine
// (protocol §4.1): login → list → detect/select → certify → initInfo →
// heart(5s) → CertifyResp-ACK → Ready, then NOTIFY intake with frame ACKs
// and self-healing reconnect.
package session

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/fevenor/konx-gw/internal/cloud"
	"github.com/fevenor/konx-gw/internal/p2p/av"
	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/list"
)

// State of the push session.
type State string

const (
	StateIdle         State = "idle"
	StateLoggingIn    State = "logging_in"
	StateListing      State = "listing"
	StateDetecting    State = "detecting"
	StateHandshaking  State = "handshaking"
	StateReady        State = "ready"
	StateReconnecting State = "reconnecting"
	StateStopped      State = "stopped"
)

// Notify is an inbound push notification (decoded application payload).
type Notify struct {
	MsgID uint16
	Name  string // e.g. "CheckLogin"; may be empty for binary bodies
	JSON  string // JSON body if present
	Body  []byte // full body after the 12-byte notify header
	SrcID uint32
}

// DetectResult scores one P2P server.
type DetectResult struct {
	Server list.Server
	RTT    time.Duration
}

// Options configures the session.
type Options struct {
	Login        func(ctx context.Context) (*cloud.Session, error)
	ListServers  []string      // list-service hostnames/IPs
	LocalPort    int           // local UDP bind start (51880)
	Heartbeat    time.Duration // default 5s
	ReconnectMin time.Duration
	ReconnectMax time.Duration
	DetectWindow time.Duration
	Trace        func(dir string, frameType byte, wire []byte)
	Log          *slog.Logger
}

// Events emitted by the session to the application.
type Events struct {
	OnState   func(from, to State)
	OnNotify  func(n Notify) // decoded push (frame ACK already sent)
	OnServers func(best list.Server, rtt time.Duration)
	OnLogin   func(s *cloud.Session) // credentials refreshed
	OnError   func(err error)        // recoverable; triggers reconnect
}

// Transport abstracts the UDP packet pipe for testability.
type Transport interface {
	Send(b []byte) error
	Recv(timeout time.Duration) ([]byte, error) // returns error on timeout
	LocalAddr() string
	Close() error
}

// RecvFromTransport 扩展 Transport 返回来源地址（AV tap 需要设备源地址
// 定向回包）。UDPTransport 实现；mock 不实现时 tap 收到 nil 地址。
type RecvFromTransport interface {
	RecvFrom(timeout time.Duration) ([]byte, *net.UDPAddr, error)
}

// UDPTransport is the production transport bound to one server.
type UDPTransport struct {
	conn *net.UDPConn
	dst  *net.UDPAddr
}

func NewUDPTransport(bindPort int, dst *net.UDPAddr) (*UDPTransport, error) {
	var conn *net.UDPConn
	var err error
	for p := bindPort; p < bindPort+100; p++ {
		conn, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: p})
		if err == nil {
			break
		}
	}
	if conn == nil || err != nil {
		return nil, fmt.Errorf("bind %d..: %w", bindPort, err)
	}
	return &UDPTransport{conn: conn, dst: dst}, nil
}

func (t *UDPTransport) Send(b []byte) error {
	_, err := t.conn.WriteToUDP(b, t.dst)
	return err
}

// SendTo 定向发送（AV 呼叫的设备 NAT/relay/meter 包，共享同一 socket）。
func (t *UDPTransport) SendTo(b []byte, addr *net.UDPAddr) error {
	_, err := t.conn.WriteToUDP(b, addr)
	return err
}

func (t *UDPTransport) Recv(timeout time.Duration) ([]byte, error) {
	_ = t.conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	n, _, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	return buf[:n], nil
}

func (t *UDPTransport) RecvFrom(timeout time.Duration) ([]byte, *net.UDPAddr, error) {
	_ = t.conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, 4096)
	n, from, err := t.conn.ReadFromUDP(buf)
	if err != nil {
		return nil, nil, err
	}
	return buf[:n], from, nil
}

func (t *UDPTransport) LocalAddr() string { return t.conn.LocalAddr().String() }
func (t *UDPTransport) Close() error      { return t.conn.Close() }

// Session drives the whole client lifecycle.
type Session struct {
	opts Options
	ev   Events
	log  *slog.Logger

	mu       sync.Mutex
	state    State
	cloudSes *cloud.Session
	best     *list.Server
	bestRTT  time.Duration
	backoff  time.Duration
	curTr    Transport // 当前连接周期的传输（AttachAV 用）
	avCaller *av.Caller

	cancel context.CancelFunc
	done   chan struct{}

	// test hooks (nil in production)
	dialTransport func(dst *net.UDPAddr) (Transport, error)
	queryList     func(ctx context.Context, host string, term uint64) ([]list.Server, error)
}

func New(opts Options, ev Events) *Session {
	if opts.Heartbeat <= 0 {
		opts.Heartbeat = 5 * time.Second
	}
	if opts.ReconnectMin <= 0 {
		opts.ReconnectMin = 5 * time.Second
	}
	if opts.ReconnectMax <= 0 {
		opts.ReconnectMax = 60 * time.Second
	}
	if opts.DetectWindow <= 0 {
		opts.DetectWindow = 800 * time.Millisecond
	}
	if len(opts.ListServers) == 0 {
		opts.ListServers = list.DefaultServers
	}
	if opts.LocalPort == 0 {
		opts.LocalPort = 51880
	}
	log := opts.Log
	if log == nil {
		log = slog.Default()
	}
	return &Session{
		opts:    opts,
		ev:      ev,
		log:     log,
		state:   StateIdle,
		backoff: opts.ReconnectMin,
		done:    make(chan struct{}),
	}
}

// State returns the current state.
func (s *Session) State() State { s.mu.Lock(); defer s.mu.Unlock(); return s.state }

// CloudSession returns the current cloud credentials snapshot.
func (s *Session) CloudSession() *cloud.Session { s.mu.Lock(); defer s.mu.Unlock(); return s.cloudSes }

// CurrentServer returns the selected P2P server, if any.
func (s *Session) CurrentServer() (list.Server, time.Duration, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.best == nil {
		return list.Server{}, 0, false
	}
	return *s.best, s.bestRTT, true
}

func (s *Session) setState(to State) {
	s.mu.Lock()
	from := s.state
	s.state = to
	s.mu.Unlock()
	if from != to && s.ev.OnState != nil {
		s.ev.OnState(from, to)
	}
}

// Run blocks until ctx is cancelled, maintaining the session.
func (s *Session) Run(ctx context.Context) {
	defer close(s.done)
	ctx, s.cancel = context.WithCancel(ctx)
	for {
		if ctx.Err() != nil {
			s.setState(StateStopped)
			return
		}
		err := s.runOnce(ctx)
		if ctx.Err() != nil {
			s.setState(StateStopped)
			return
		}
		if err != nil {
			s.log.Warn("p2p session cycle failed", "err", err)
			if s.ev.OnError != nil {
				s.ev.OnError(err)
			}
		}
		s.setState(StateReconnecting)
		s.mu.Lock()
		base := s.backoff
		s.backoff *= 2
		if s.backoff > s.opts.ReconnectMax {
			s.backoff = s.opts.ReconnectMax
		}
		s.mu.Unlock()
		// Jittered wait ∈ [base/2, base): avoids thundering reconnects
		// while still growing exponentially (min→max) on sustained failure.
		wait := base
		if half := base / 2; half > 0 {
			wait = half + time.Duration(rand.Int63n(int64(half)))
		}
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			s.setState(StateStopped)
			return
		}
	}
}

// Stop cancels the session and waits for Run to return.
func (s *Session) Stop() {
	if s.cancel != nil {
		s.cancel()
	}
	<-s.done
}

// runOnce performs one full connect cycle; returns error to trigger reconnect.
func (s *Session) runOnce(ctx context.Context) error {
	// 1. login
	s.setState(StateLoggingIn)
	cs, err := s.opts.Login(ctx)
	if err != nil {
		return fmt.Errorf("login: %w", err)
	}
	s.mu.Lock()
	s.cloudSes = cs
	s.mu.Unlock()
	if s.ev.OnLogin != nil {
		s.ev.OnLogin(cs)
	}

	// 2. list query
	s.setState(StateListing)
	servers, err := s.queryServers(ctx, cs)
	if err != nil {
		return fmt.Errorf("list: %w", err)
	}

	// 3. detect all + select best
	s.setState(StateDetecting)
	best, rtt, transport, err := s.detectAndSelect(ctx, cs, servers)
	if err != nil {
		return fmt.Errorf("detect: %w", err)
	}
	defer transport.Close()
	s.mu.Lock()
	b := best
	s.best = &b
	s.bestRTT = rtt
	s.curTr = transport
	s.mu.Unlock()
	// 重连（runOnce 退出）时先停 AV 呼叫再关传输：避免悬挂半会话
	// （AV Stop 的 CLOSE×3 挂断经同一 socket 发出）。
	defer func() {
		s.mu.Lock()
		c := s.avCaller
		s.avCaller = nil
		s.curTr = nil
		s.mu.Unlock()
		if c != nil {
			c.Stop()
		}
	}()
	if s.ev.OnServers != nil {
		s.ev.OnServers(best, rtt)
	}
	s.log.Info("p2p server selected", "addr", best.Addr(), "rtt", rtt)

	// 4. pipeline handshake
	s.setState(StateHandshaking)
	fb := frame.NewBuilder(cs.UID, cs.C1, cs.C2, cs.S1, cs.S2)
	det := fb.Detect(uint64(time.Now().UnixMilli()))
	s.trace("tx", frame.TypeDetect, det)
	if err := transport.Send(det); err != nil {
		return err
	}
	if err := transport.Send(fb.Certify()); err != nil {
		return err
	}
	initInfo := fb.InitInfo()
	s.trace("tx", frame.TypeInitInfo, initInfo)
	if err := transport.Send(initInfo); err != nil {
		return err
	}
	if err := transport.Send(fb.Heart()); err != nil {
		return err
	}

	// 5. main loop: heartbeats + receive
	// Fully established: reset the reconnect backoff base here (not at
	// login — post-login stages may still fail repeatedly and must keep
	// backing off).
	s.mu.Lock()
	s.backoff = s.opts.ReconnectMin
	s.mu.Unlock()
	s.setState(StateReady)
	err = s.loop(ctx, transport, fb)
	_ = transport.Close()
	return err
}

func (s *Session) loop(ctx context.Context, t Transport, fb *frame.Builder) error {
	heart := time.NewTicker(s.opts.Heartbeat)
	defer heart.Stop()
	var missedHearts int
	certifyRespAcked := false

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-heart.C:
			hf := fb.Heart()
			s.trace("tx", frame.TypeHeart, hf)
			if err := t.Send(hf); err != nil {
				return err
			}
			missedHearts++
			if missedHearts >= 3 {
				return fmt.Errorf("heartbeat lost x%d", missedHearts)
			}
		default:
		}
		pkt, from, err := s.recvFrom(t, 200*time.Millisecond)
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				continue
			}
			return err
		}
		s.trace("rx", 0, pkt)
		// AV tap 先行：meter/KCP 封包头族由 av 消费（含设备源地址定向），
		// 未消费走 gute 解析。
		s.mu.Lock()
		avc := s.avCaller
		s.mu.Unlock()
		if avc != nil && avc.Tap()(pkt, from) {
			// 实测（2026-10-26）：P2P 呼叫活跃期间服务器
			// 停止应答心跳（呼叫流量本身即会话存活证据，参考实现客户端兼容
			// 此行为）——tap 消费了包说明会话流量活跃，心跳失联计数清零，
			// 避免误杀健康取流（媒体正在流动时触发整会话重连，且呼叫次数
			// 有 15-30 分钟黑名单代价）。呼叫结束后 tap 不再消费，计数
			// 自然恢复；Caller 进入 closed 后 tap 返回 false，不影响。
			missedHearts = 0
			continue
		}
		p, err := frame.Parse(pkt)
		if err != nil {
			s.log.Debug("unparseable frame", "err", err)
			continue
		}
		switch p.Type {
		case frame.TypeDetectAck:
		case frame.TypeHeartAck:
			missedHearts = 0
		case frame.TypeCertifyRes:
			if p.Len > 0x18 && !certifyRespAcked {
				certifyRespAcked = true
				ack := fb.Ack(frame.TypeCertifyRes)
				s.trace("tx", frame.TypeCertifyRes, ack)
				_ = t.Send(ack)
			}
		case frame.TypeNotifyPush:
			ack := fb.Ack(frame.TypeNotifyPush)
			s.trace("tx", frame.TypeNotifyPush, ack)
			_ = t.Send(ack)
			if n, err := frame.ParseNotify(p); err == nil {
				dn := decodeNotify(n)
				// The server re-pushes CheckLogin every 2-8s until the
				// app-level 0x36 answer arrives (live capture) — send it
				// right after the frame-level ACK.
				if dn.Name == "CheckLogin" {
					reply := fb.CheckLoginReply()
					s.trace("tx", frame.TypeCheckLoginReply, reply)
					_ = t.Send(reply)
				}
				if s.ev.OnNotify != nil {
					s.ev.OnNotify(dn)
				}
			}
		default:
			// AV 呼叫消费 0x3A/0x24（MTPRES 协商由 av 驱动，含 ACK/0x23 应答）
			if p.Type == 0x3a || p.Type == 0x24 {
				s.mu.Lock()
				avc := s.avCaller
				s.mu.Unlock()
				if avc != nil && avc.HandleGute(p) {
					// 实测（2026-10-26）：呼叫建立后服务器以
					// ~3000-7000 帧/秒持续下发 MTPRES（实测同期收到
					// 104,639 条——呼叫态常态洪流，非重传），且此期间服务器
					// 暂停心跳应答。该洪流即会话活跃的直接证据，与 tap 消费
					// 同语义清零心跳失联计数，否则 15s 后健康呼叫被误杀
					// （三个连续周期死于 heartbeat lost x3 的根因）。
					missedHearts = 0
					continue
				}
			}
			// CertifyAck(0x03/24B), initInfo ACK(0x81/24B), 0x27 remote …
			s.log.Debug("frame", "type", fmt.Sprintf("0x%02x", p.Type), "len", p.Len)
		}
	}
}

// decodeNotify splits the notify body into message name + JSON body.
// Two live-observed shapes:
//
//	a) "Name{json}"          — early captures (name abuts JSON)
//	b) <binary hdr><Name>\0{json} — current servers: ~15B binary prefix
//	   before a NUL-terminated name; header contains a name-length byte
//
// The parser is structural: find the JSON '{', walk back over printable
// ASCII to recover the name; binary-only bodies keep their leading run.
func decodeNotify(n *frame.Notify) Notify {
	out := Notify{MsgID: n.MsgID, SrcID: n.SrcID, Body: n.Body}
	if jsonStart := bytes.IndexByte(n.Body, '{'); jsonStart > 0 {
		// name is the printable run ending just before the JSON, possibly
		// separated by NUL(s) (live shape: <hdr>Name\0{json})
		k := jsonStart
		for k > 0 && n.Body[k-1] == 0 {
			k--
		}
		j := k
		for j > 0 && n.Body[j-1] >= 0x20 && n.Body[j-1] < 0x7F {
			j--
		}
		name := string(n.Body[j:k])
		js := strings.TrimRight(string(n.Body[jsonStart:]), "\x00")
		if len(name) >= 3 && json.Valid([]byte(js)) {
			out.Name, out.JSON = name, js
		}
	}
	if out.Name == "" && len(n.Body) > 0 {
		end := 0
		for end < len(n.Body) && n.Body[end] >= 0x20 && n.Body[end] < 0x7F {
			end++
		}
		out.Name = string(n.Body[:end])
	}
	return out
}

func (s *Session) queryServers(ctx context.Context, cs *cloud.Session) ([]list.Server, error) {
	if s.queryList != nil {
		return s.queryList(ctx, "", cs.UID)
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	var lastErr error
	for _, host := range s.opts.ListServers {
		qctx, cancel := context.WithTimeout(ctx, 3*time.Second)
		servers, err := list.QueryUDPWithTerm(qctx, conn, host, cs.UID)
		cancel()
		if err == nil {
			return servers, nil
		}
		lastErr = err
	}
	return nil, lastErr
}

// detectAndSelect probes every listed server concurrently within the
// detect window and picks the lowest-RTT responder.
func (s *Session) detectAndSelect(ctx context.Context, cs *cloud.Session, servers []list.Server) (list.Server, time.Duration, Transport, error) {
	type result struct {
		srv list.Server
		rtt time.Duration
		tr  Transport
	}
	resCh := make(chan result, len(servers))
	var wg sync.WaitGroup
	for _, srv := range servers {
		wg.Add(1)
		go func(srv list.Server) {
			defer wg.Done()
			dst := &net.UDPAddr{IP: net.ParseIP(srv.IP), Port: srv.Port}
			var tr Transport
			var err error
			if s.dialTransport != nil {
				tr, err = s.dialTransport(dst)
			} else {
				tr, err = NewUDPTransport(s.opts.LocalPort, dst)
			}
			if err != nil {
				return
			}
			fb := frame.NewBuilder(cs.UID, cs.C1, cs.C2, cs.S1, cs.S2)
			start := time.Now()
			_ = tr.Send(fb.Detect(uint64(start.UnixMilli())))
			deadline := time.Now().Add(s.opts.DetectWindow * 2)
			for time.Now().Before(deadline) {
				pkt, err := tr.Recv(s.opts.DetectWindow)
				if err != nil {
					break
				}
				p, err := frame.Parse(pkt)
				if err == nil && p.Type == frame.TypeDetectAck {
					resCh <- result{srv: srv, rtt: time.Since(start), tr: tr}
					return
				}
			}
			_ = tr.Close()
		}(srv)
	}
	wg.Wait()
	close(resCh)
	var best result
	found := false
	for r := range resCh {
		if !found || r.rtt < best.rtt {
			if found {
				_ = best.tr.Close()
			}
			best = r
			found = true
		} else {
			_ = r.tr.Close()
		}
	}
	if !found {
		return list.Server{}, 0, nil, fmt.Errorf("no P2P server answered detect")
	}
	return best.srv, best.rtt, best.tr, nil
}

func (s *Session) trace(dir string, ft byte, wire []byte) {
	if s.opts.Trace != nil {
		s.opts.Trace(dir, ft, wire)
	}
}

// recvFrom 经 Transport 收包；支持 RecvFrom 的传输返回来源地址（AV tap
// 定向回包），否则地址为 nil（mock 传输）。
func (s *Session) recvFrom(t Transport, timeout time.Duration) ([]byte, *net.UDPAddr, error) {
	if rft, ok := t.(RecvFromTransport); ok {
		return rft.RecvFrom(timeout)
	}
	b, err := t.Recv(timeout)
	return b, nil, err
}

// ---------------------------------------------------------------------------
// AV 呼叫挂载（media.source=p2p-av）：与 gute 会话共享同一 UDP socket
// ---------------------------------------------------------------------------

// AttachAV 建立并启动一路设备 AV 呼叫（内部建 Caller、挂 tap、Start）。
// 要求会话 Ready（gute 管道已建立、云凭据在手）；重连周期退出时呼叫
// 自动完整挂断（CLOSE×3+拆链）并释放 tap。同一时刻仅允许一路呼叫。
func (s *Session) AttachAV(devID uint32, treatedPwd int) (*av.Caller, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state != StateReady {
		return nil, fmt.Errorf("p2p session not ready (state %s)", s.state)
	}
	// closed 的残留 caller 允许原地替换：生产路径呼叫结束后无人显式
	// DetachAV（重连 defer 只停呼叫不卸引用、媒体层 Acquire 复挂），若一
	// 律拒绝，同 gute 周期内所有重拨都会撞 already attached。旧 caller
	// 已 closed，无需再 Stop，直接覆盖引用即可。
	if s.avCaller != nil && s.avCaller.State() != av.StateClosed {
		return nil, fmt.Errorf("AV caller already attached (DetachAV first)")
	}
	if s.cloudSes == nil {
		return nil, fmt.Errorf("no cloud session")
	}
	tr, ok := s.curTr.(av.Transport)
	if !ok {
		return nil, fmt.Errorf("transport lacks SendTo (AV needs the UDP channel)")
	}
	cs := s.cloudSes
	c := av.New(devID, cs.UID, treatedPwd, av.Options{
		Transport: tr,
		Creds:     av.CloudCreds{C1: cs.C1, C2: cs.C2, S1: cs.S1, S2: cs.S2},
		Log:       s.log,
	})
	s.avCaller = c
	if err := c.Start(context.Background()); err != nil {
		s.avCaller = nil
		return nil, err
	}
	s.log.Info("AV caller attached", "dev", devID)
	return c, nil
}

// DetachAV 停止并卸下当前 AV 呼叫（完整挂断语义）。
func (s *Session) DetachAV() {
	s.mu.Lock()
	c := s.avCaller
	s.avCaller = nil
	s.mu.Unlock()
	if c != nil {
		c.Stop()
	}
}

// SetTestHooks installs transport/list overrides for tests (same-package
// fields; exported for the external test package).
func (s *Session) SetTestHooks(
	dial func(dst *net.UDPAddr) (Transport, error),
	query func(ctx context.Context, host string, term uint64) ([]list.Server, error),
) {
	s.dialTransport = dial
	s.queryList = query
}

// DecodeNotifyForTest exposes decodeNotify for verification tooling.
func DecodeNotifyForTest(n *frame.Notify) Notify { return decodeNotify(n) }
