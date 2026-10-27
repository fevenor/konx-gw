package session_test

import (
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/cloud"
	"github.com/fevenor/konx-gw/internal/p2p/av"
	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/list"
	"github.com/fevenor/konx-gw/internal/p2p/session"
)

// ---------------------------------------------------------------------------
// mock transport + mock P2P server (speaks the 7F01/02/03/04/81/21/22/35
// dialect exactly like the real server did during the PoC runs)
// ---------------------------------------------------------------------------

type mockTransport struct {
	mu     sync.Mutex
	inbox  chan []byte
	closed bool
	onSend func(b []byte)
}

func (t *mockTransport) Send(b []byte) error {
	t.mu.Lock()
	cb := t.onSend
	t.mu.Unlock()
	if cb != nil {
		cb(b)
	}
	return nil
}

// setOnSend 在会话运行中替换发送钩子（与 Send 互斥，-race 干净）。
func (t *mockTransport) setOnSend(fn func(b []byte)) {
	t.mu.Lock()
	t.onSend = fn
	t.mu.Unlock()
}
func (t *mockTransport) Recv(timeout time.Duration) ([]byte, error) {
	select {
	case b := <-t.inbox:
		return b, nil
	case <-time.After(timeout):
		return nil, &net.DNSError{IsTimeout: true}
	}
}
func (t *mockTransport) LocalAddr() string { return "mock:0" }
func (t *mockTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}
func (t *mockTransport) push(b []byte) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.closed {
		select {
		case t.inbox <- b:
		default:
		}
	}
}

// mockServer answers frames on one transport and can inject pushes.
type mockServer struct {
	mu   sync.Mutex
	tr   *mockTransport
	fb   *frame.Builder
	term uint64
}

func newMockServer(term uint64) *mockServer {
	m := &mockServer{fb: frame.NewBuilder(term, 1, 2, 3, 4), term: term}
	m.redial()
	return m
}

// redial replaces the current transport (the session closes transports on
// reconnect; a fresh one is wired each time).
func (m *mockServer) redial() *mockTransport {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tr = &mockTransport{inbox: make(chan []byte, 64), onSend: m.handleLocked}
	return m.tr
}

func (m *mockServer) handleLocked(b []byte) { m.handle(b) }

func (m *mockServer) handle(wire []byte) {
	p, err := frame.Parse(wire)
	if err != nil {
		return
	}
	switch p.Type {
	case frame.TypeDetect:
		m.tr.push(m.fb.Ack(frame.TypeDetectAck)) // 24B reply of same family
	case frame.TypeCertify:
		m.tr.push(m.fb.Ack(frame.TypeCertify)) // CertifyAck
		// CertifyResp: >24B frame of type 0x04 (pubkey zeros suffice)
		resp := m.fb.Certify()
		resp[1] = frame.TypeCertifyRes
		m.tr.push(resp)
	case frame.TypeInitInfo:
		m.tr.push(m.fb.Ack(frame.TypeInitInfo))
	case frame.TypeHeart:
		m.tr.push(m.fb.Ack(frame.TypeHeartAck))
	case frame.TypeCertifyRes:
		// client ACK of CertifyResp — fine
	}
}

func (m *mockServer) sendNotify(name string, jsonBody string) {
	m.sendNotifyRaw(name, []byte(jsonBody))
}

func (m *mockServer) sendNotifyRaw(name string, jsonTail []byte) {
	body := append([]byte(name), jsonTail...)
	payload := make([]byte, 0x0C+len(body))
	payload[0] = 1 // msg_id (LE u16) = 1
	payload[2] = 1 // flags
	// src/dst zero is fine for the test
	copy(payload[0x0C:], body)
	// build a push frame via heart-length trick: reuse builder with custom fill
	f := make([]byte, 0x18+len(payload))
	f[0] = 0x7F
	f[1] = frame.TypeNotifyPush
	f[2] = byte(len(f))
	f[3] = byte(len(f) >> 8)
	le64 := func(o int, v uint64) {
		for i := 0; i < 8; i++ {
			f[o+i] = byte(v >> (8 * i))
		}
	}
	le32 := func(o int, v uint32) {
		for i := 0; i < 4; i++ {
			f[o+i] = byte(v >> (8 * i))
		}
	}
	le64(0x04, m.term)
	le32(0x0C, 1)
	f[0x14] = 1
	le32(0x10, 0)
	copy(f[0x18:], payload)
	le32(0x10, chksum(f))
	m.tr.push(frame.Encrypt(f))
}

func chksum(f []byte) uint32 {
	var s uint32
	for _, off := range []int{0x00, 0x04, 0x08, 0x0C, 0x14} {
		var w uint32
		for i := 0; i < 4; i++ {
			w |= uint32(f[off+i]) << (8 * i)
		}
		s += w
	}
	return s
}

// ---------------------------------------------------------------------------
// test scaffolding
// ---------------------------------------------------------------------------

type recorder struct {
	mu       sync.Mutex
	states   []session.State
	notifies []session.Notify
	logins   int
	errors   []error
	servers  []string
}

func (r *recorder) rec() session.Events {
	return session.Events{
		OnState:  func(from, to session.State) { r.mu.Lock(); r.states = append(r.states, to); r.mu.Unlock() },
		OnNotify: func(n session.Notify) { r.mu.Lock(); r.notifies = append(r.notifies, n); r.mu.Unlock() },
		OnLogin:  func(*cloud.Session) { r.mu.Lock(); r.logins++; r.mu.Unlock() },
		OnError:  func(err error) { r.mu.Lock(); r.errors = append(r.errors, err); r.mu.Unlock() },
		OnServers: func(s list.Server, rtt time.Duration) {
			r.mu.Lock()
			r.servers = append(r.servers, s.Addr())
			r.mu.Unlock()
		},
	}
}

const testTerm = 2204264224

// loginAttempts 由登录回调（Run goroutine）写、waitFor（测试 goroutine）
// 读——必须原子（-race 干净）。
var loginAttempts atomic.Int32

func testSession(t *testing.T, srv *mockServer, loginFail error) (*session.Session, *recorder) {
	t.Helper()
	r := &recorder{}
	loginAttempts.Store(0)
	opts := session.Options{
		Login: func(ctx context.Context) (*cloud.Session, error) {
			loginAttempts.Add(1)
			if loginFail != nil {
				return nil, loginFail
			}
			return &cloud.Session{UID: testTerm, C1: 1, C2: 2, S1: 3, S2: 4}, nil
		},
		Heartbeat:    200 * time.Millisecond,
		ReconnectMin: 50 * time.Millisecond,
		ReconnectMax: 200 * time.Millisecond,
		DetectWindow: 300 * time.Millisecond,
	}
	s := session.New(opts, r.rec())
	s.SetTestHooks(
		func(*net.UDPAddr) (session.Transport, error) { return srv.redial(), nil },
		func(context.Context, string, uint64) ([]list.Server, error) {
			return []list.Server{{IP: "10.0.0.1", Port: 8000}}, nil
		},
	)
	return s, r
}

func TestSessionReachesReadyAndReceivesPush(t *testing.T) {
	srv := newMockServer(testTerm)
	s, r := testSession(t, srv, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })

	srv.sendNotify("CheckLogin", `{"cmd":1,"sessionID":497459251,"sessionID2":2395831484,"AppOS":3}`)
	waitFor(t, 3*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.notifies) == 1
	})
	r.mu.Lock()
	n := r.notifies[0]
	r.mu.Unlock()
	if n.Name != "CheckLogin" || n.JSON == "" || n.MsgID == 0 {
		t.Fatalf("notify not decoded: %+v", n)
	}
}

func TestSessionLoginFailureRetries(t *testing.T) {
	srv := newMockServer(testTerm)
	s, _ := testSession(t, srv, errors.New("10901025 bad credentials"))

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return loginAttempts.Load() >= 2 })
	if s.State() == session.StateReady {
		t.Fatal("ready despite login failure")
	}
}

func TestSessionHeartbeatLossReconnects(t *testing.T) {
	srv := newMockServer(testTerm)
	s, r := testSession(t, srv, nil)

	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })

	// stop answering heartbeats → 3 missed → error → reconnect cycle
	var silence atomic.Bool
	silence.Store(true)
	srv.tr.setOnSend(func(b []byte) {
		if !silence.Load() {
			srv.handle(b)
		}
	})

	waitFor(t, 5*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.errors) > 0
	})
	// after error it goes reconnecting and (login works, server answers
	// again) reaches ready a second time
	silence.Store(false)
	waitFor(t, 5*time.Second, func() bool { return s.State() == session.StateReady })
}

func TestNotifyDecodeNameAndJSON(t *testing.T) {
	srv := newMockServer(testTerm)
	srv.tr.onSend = srv.handle
	s, r := testSession(t, srv, nil)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()
	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })

	// binary alarm-style body without JSON must still surface
	body := make([]byte, 0x30)
	for i := 0; i < 8; i++ {
		body[0x04+i] = byte(uint64(10000001) >> (8 * i))
	}
	f := make([]byte, 0x18+0x0C+len(body))
	srv.sendNotifyRaw("ALARM_MSG_FROM_SRV", body)
	_ = f
	waitFor(t, 3*time.Second, func() bool {
		r.mu.Lock()
		defer r.mu.Unlock()
		return len(r.notifies) >= 1
	})
	r.mu.Lock()
	got := false
	for _, n := range r.notifies {
		if n.Name == "ALARM_MSG_FROM_SRV" {
			got = true
		}
	}
	r.mu.Unlock()
	if !got {
		t.Fatal("binary notify not surfaced")
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// ---------------------------------------------------------------------------
// 呼叫期心跳误杀修复：AV tap 消费包 = 会话流量活跃 → missedHearts 清零
// ---------------------------------------------------------------------------

// sendToWrap 给 mockTransport 补 AV 需要的定向发送（AttachAV 门控）。
type sendToWrap struct{ *mockTransport }

func (w sendToWrap) SendTo(b []byte, addr *net.UDPAddr) error { return w.Send(b) }

// deviceMeterReq 构造 72B 设备 meter_req（0xc8 族，tap 必消费）。
func deviceMeterReq(counter uint32) []byte {
	f := make([]byte, 72)
	f[0], f[1] = 0xc8, 0x09
	f[5] = 1
	f[6] = 0x44
	for i := 8; i < 12; i++ {
		f[i] = byte(0x11)
	}
	f[0x20] = byte(counter)
	return f
}

// newAVSession 构造带 SendTo 能力（av.Transport）的 mock 会话——
// AttachAV 系列测试的共同脚手架。
func newAVSession(srv *mockServer, heartbeat time.Duration) (*session.Session, *recorder) {
	r := &recorder{}
	loginAttempts.Store(0)
	opts := session.Options{
		Login: func(ctx context.Context) (*cloud.Session, error) {
			loginAttempts.Add(1)
			return &cloud.Session{UID: testTerm, C1: 1, C2: 2, S1: 3, S2: 4}, nil
		},
		Heartbeat:    heartbeat,
		ReconnectMin: 50 * time.Millisecond,
		ReconnectMax: 200 * time.Millisecond,
		DetectWindow: 300 * time.Millisecond,
		Log:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	s := session.New(opts, r.rec())
	s.SetTestHooks(
		func(*net.UDPAddr) (session.Transport, error) { return sendToWrap{srv.redial()}, nil },
		func(context.Context, string, uint64) ([]list.Server, error) {
			return []list.Server{{IP: "10.0.0.1", Port: 8000}}, nil
		},
	)
	return s, r
}

// TestSessionAVTrafficSuppressesHeartbeatLoss：心跳无 ACK 但 tap 持续消费
// 包 → 不触发 heartbeat lost；tap 停止消费后 3 个心跳周期正常触发重连。
func TestSessionAVTrafficSuppressesHeartbeatLoss(t *testing.T) {
	srv := newMockServer(testTerm)
	s, r := newAVSession(srv, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })
	c, err := s.AttachAV(10000001, 387648820)
	if err != nil {
		t.Fatalf("AttachAV: %v", err)
	}

	// 静默服务器一切应答（含心跳 ACK），仅持续注入设备 meter 包
	srv.tr.setOnSend(func(b []byte) {})
	var deviceActive atomic.Bool
	deviceActive.Store(true)
	go func() {
		var ctr uint32
		for deviceActive.Load() {
			srv.tr.push(deviceMeterReq(ctr))
			ctr++
			time.Sleep(30 * time.Millisecond)
		}
	}()

	// 7+ 个心跳周期无 ACK 但 tap 消费活跃 → 会话必须保持 Ready
	time.Sleep(800 * time.Millisecond)
	if s.State() != session.StateReady {
		t.Fatalf("呼叫活跃期被心跳误杀: state=%s", s.State())
	}
	r.mu.Lock()
	nerr := len(r.errors)
	r.mu.Unlock()
	if nerr != 0 {
		t.Fatalf("呼叫活跃期不应有会话错误: %v", nerr)
	}

	// 设备流量停止 → 3 个心跳周期后正常触发 heartbeat lost → 重连
	deviceActive.Store(false)
	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReconnecting })
	// 重连必须自动挂断 AV 呼叫（无悬挂半会话）
	waitFor(t, 3*time.Second, func() bool { return c.State() == av.StateClosed })
}

// TestSessionMtpresFloodSuppressesHeartbeatLoss：真机根因回归
// ——呼叫建立后服务器以 ~3000+/秒持续下发 MTPRES(0x24)（呼叫态
// 常态），期间心跳无 ACK。0x24 经 HandleGute 消费，
// 必须与 tap 消费同语义清零 missedHearts，否则 15s 后健康呼叫被误杀。
func TestSessionMtpresFloodSuppressesHeartbeatLoss(t *testing.T) {
	srv := newMockServer(testTerm)
	s, r := newAVSession(srv, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })
	if _, err := s.AttachAV(10000001, 387648820); err != nil {
		t.Fatalf("AttachAV: %v", err)
	}

	// 心跳静默 + MTPRES 洪流（真机 golden 帧，Parse 实证通过）
	srv.tr.setOnSend(func(b []byte) {})
	storm, err := hex.DecodeString(
		"7f24a8005c27e625ea9b07f1a040db3c66d73ced05bc0000310f199aea6a59b7422b6900d672ae2c574aa7a4973d1620db6e6957ee62c8791254d31af2f40f591254d31af2f40f59871fff5c437224ec5ed18e15f52c92ee1254d31af2f40f590cd5747a944e35cfe19e445671a7aa8626c6fa61bb44a5b5f3c7872898374ee5fad3ca4d8278b90120347094861b8c5acc3ba09da52a1a49a4d78f3e19f97e2549bac963a753c882")
	if err != nil {
		t.Fatalf("hex: %v", err)
	}
	var floodActive atomic.Bool
	floodActive.Store(true)
	go func() {
		for floodActive.Load() {
			srv.tr.push(storm)
			time.Sleep(30 * time.Millisecond)
		}
	}()

	// 7+ 个心跳周期无 ACK 但 0x24 洪流活跃 → 会话必须保持 Ready
	time.Sleep(800 * time.Millisecond)
	if s.State() != session.StateReady {
		t.Fatalf("MTPRES 洪流期被心跳误杀: state=%s", s.State())
	}
	r.mu.Lock()
	nerr := len(r.errors)
	r.mu.Unlock()
	if nerr != 0 {
		t.Fatalf("洪流期不应有会话错误: %v", nerr)
	}

	// 洪流停止 → 3 个心跳周期后正常 heartbeat lost → 重连
	floodActive.Store(false)
	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReconnecting })
}

// TestAttachAVAfterCallerClosed（P0 回归）：呼叫结束（closed）后 avCaller
// 残留是生产常态——没人显式 DetachAV；第二次观看必须在同一 gute 周期内
// 直接替换挂载成功，而不是撞 "already attached" 永远失败。
func TestAttachAVAfterCallerClosed(t *testing.T) {
	srv := newMockServer(testTerm)
	s, _ := newAVSession(srv, 200*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })

	c1, err := s.AttachAV(10000001, 387648820)
	if err != nil {
		t.Fatalf("first AttachAV: %v", err)
	}
	// 活跃呼叫期间重复挂载仍须拒绝（同一时刻仅一路）
	if _, err := s.AttachAV(10000001, 387648820); err == nil {
		t.Fatal("duplicate AttachAV while caller active must be rejected")
	}

	// 呼叫结束：closed 残留（生产路径无人调 DetachAV）
	c1.Stop()
	waitFor(t, 3*time.Second, func() bool { return c1.State() == av.StateClosed })

	// 第二次观看：closed caller 必须允许原地替换
	c2, err := s.AttachAV(10000001, 387648820)
	if err != nil {
		t.Fatalf("AttachAV after closed caller must succeed: %v", err)
	}
	c2.Stop()
}
