package api

// WebSocket 通道健壮性测试：写超时断开慢客户端、ping/pong 半开检测、
// 并发发布下 seq 严格有序、消息格式保持不变。

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

// newWSTestEnv 构造带真实 auth/engine/store 的测试服务端，返回 WS 连接地址。
// squeeze 为 true 时把服务端发送缓冲压到最小，便于在本地回环上模拟慢客户端。
func newWSTestEnv(t *testing.T, squeeze bool) (*Server, *events.Engine, *events.Store, string) {
	t.Helper()
	st, err := events.Open(t.TempDir() + "/ws-test.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := auth.New(st, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	tok, err := svc.CreateToken("ws-test", []string{auth.ScopeRead})
	if err != nil {
		t.Fatal(err)
	}
	eng := events.NewEngine(events.NewRegistry(), st)
	s := New(Deps{Cfg: &config.App{}, Auth: svc, Store: st, Engine: eng, Log: slog.Default()})
	srv := httptest.NewUnstartedServer(s.Handler())
	if squeeze {
		srv.Config.ConnState = func(nc net.Conn, st http.ConnState) {
			if st == http.StateNew {
				if tc, ok := nc.(*net.TCPConn); ok {
					_ = tc.SetWriteBuffer(2048)
				}
			}
		}
	}
	srv.Start()
	t.Cleanup(srv.Close)
	return s, eng, st, "ws" + strings.TrimPrefix(srv.URL, "http") + "/api/ws?token=" + tok.Secret
}

// dialWS 建立一个测试客户端连接。
func dialWS(t *testing.T, url string) *websocket.Conn {
	t.Helper()
	c, _, err := websocket.DefaultDialer.Dial(url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	return c
}

// uniqEvent 生成带唯一 MsgID/type 的事件，确保绕过引擎去重窗口。
func uniqEvent(i int) events.Event {
	return events.Event{
		MsgID: fmt.Sprintf("m%d", i), DeviceID: "cloud",
		Type: fmt.Sprintf("t%d", i), Payload: "{}",
	}
}

// wsConnCount 并发安全地读取当前连接数。
func wsConnCount(s *Server) int {
	s.wsMu.Lock()
	defer s.wsMu.Unlock()
	return len(s.wsConns)
}

// TestWSMessageFormat：初始快照无 seq 字段；事件消息带 seq 且逐条递增；
// 广播消息保持无 seq 的既有格式。
func TestWSMessageFormat(t *testing.T) {
	_, eng, _, url := newWSTestEnv(t, false)
	c := dialWS(t, url)
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(5 * time.Second))

	var m map[string]any
	if err := c.ReadJSON(&m); err != nil {
		t.Fatalf("读初始快照: %v", err)
	}
	if m["type"] != "state.changed" {
		t.Fatalf("首条消息类型 = %v", m["type"])
	}
	if _, has := m["seq"]; has {
		t.Fatalf("初始快照不应带 seq 字段: %v", m)
	}

	readEvent := func() int64 {
		t.Helper()
		if err := c.ReadJSON(&m); err != nil {
			t.Fatalf("读事件: %v", err)
		}
		if m["type"] != "event.new" {
			t.Fatalf("事件消息类型 = %v", m["type"])
		}
		seq, ok := m["seq"].(float64)
		if !ok || seq < 1 {
			t.Fatalf("事件缺少合法 seq: %v", m)
		}
		return int64(seq)
	}
	eng.Submit(uniqEvent(0))
	first := readEvent()
	eng.Submit(uniqEvent(1))
	if second := readEvent(); second != first+1 {
		t.Fatalf("seq 未按 1 递增: %d → %d", first, second)
	}
	data, _ := m["data"].(map[string]any)
	if data["device_id"] != "cloud" {
		t.Fatalf("事件 data.device_id = %v", data["device_id"])
	}
}

// TestWSSlowConsumerDroppedNotStallOthers（S4）：客户端把接收窗口压到极小
// 且不读，模拟慢/半开客户端。服务端写侧必须在写超时内断开该连接并注销，
// 且不影响正常客户端继续收事件。
func TestWSSlowConsumerDroppedNotStallOthers(t *testing.T) {
	oldWait := wsWriteWait
	wsWriteWait = 300 * time.Millisecond
	defer func() { wsWriteWait = oldWait }()

	s, eng, _, url := newWSTestEnv(t, true) // 压小服务端发送缓冲

	// 慢客户端：握手后不再读，接收缓冲压到最小 → 服务端写满即阻塞
	slow := dialWS(t, url)
	defer slow.Close()
	if tc, ok := slow.UnderlyingConn().(*net.TCPConn); ok {
		_ = tc.SetReadBuffer(1024)
	}

	// 正常客户端：持续读
	fast := dialWS(t, url)
	defer fast.Close()
	const total = 200
	fastDone := make(chan struct{})
	go func() {
		defer close(fastDone)
		fast.SetReadDeadline(time.Now().Add(10 * time.Second))
		got := 0
		for got < total {
			var m map[string]any
			if err := fast.ReadJSON(&m); err != nil {
				return
			}
			if m["type"] == "event.new" {
				got++
			}
		}
	}()

	for i := 0; i < total; i++ {
		eng.Submit(uniqEvent(i)) // Submit 即返回，证明发布方不被慢客户端拖住
	}

	// 慢客户端必须被断开注销（写超时 / 队列满两条路径任一生效）
	if !waitCond(t, 5*time.Second, func() bool { return wsConnCount(s) == 1 }) {
		t.Fatal("慢客户端未被断开，仍滞留在连接表")
	}
	// 正常客户端收齐全部事件，不受慢客户端影响
	select {
	case <-fastDone:
	case <-time.After(10 * time.Second):
		t.Fatal("正常客户端未能收齐事件")
	}
}

// TestWSHalfOpenPingTimeout（S5）：客户端完全不读也不回 pong（模拟 NAT 半开），
// 服务端周期 ping 后应在读超时内断开并注销该连接。
func TestWSHalfOpenPingTimeout(t *testing.T) {
	oldPing, oldPong := wsPingPeriod, wsPongWait
	wsPingPeriod, wsPongWait = 50*time.Millisecond, 300*time.Millisecond
	defer func() { wsPingPeriod, wsPongWait = oldPing, oldPong }()

	s, _, _, url := newWSTestEnv(t, false)
	c := dialWS(t, url)
	defer c.Close()

	if !waitCond(t, 5*time.Second, func() bool { return wsConnCount(s) == 0 }) {
		t.Fatal("半开死连接未被 ping/pong 机制清理")
	}
	// 客户端侧也应感知断开：读完缓冲中的残留消息（初始快照等）后
	// 必须在时限内得到读错误；ping/pong 属控制帧由库内部消化不上抛。
	c.SetReadDeadline(time.Now().Add(2 * time.Second))
	for i := 0; i < 8; i++ {
		var m map[string]any
		if err := c.ReadJSON(&m); err != nil {
			return // 读到断开错误，符合预期
		}
	}
	t.Fatal("连接应已断开，却仍持续读到应用消息")
}

// TestWSSeqOrderedUnderConcurrentPublish（C0-3）：多协程并发发布事件时，
// 单个连接收到的 event.new 的 seq 必须无乱序、无缺失（连续递增 1）。
func TestWSSeqOrderedUnderConcurrentPublish(t *testing.T) {
	_, eng, _, url := newWSTestEnv(t, false)
	c := dialWS(t, url)
	defer c.Close()

	const publishers, perPub = 8, 25
	total := publishers * perPub
	type seqMsg struct {
		seq int64
		ok  bool
	}
	seqs := make(chan seqMsg, total+8)
	readErr := make(chan error, 1)
	go func() {
		c.SetReadDeadline(time.Now().Add(15 * time.Second))
		for {
			var m map[string]any
			if err := c.ReadJSON(&m); err != nil {
				readErr <- err
				close(seqs)
				return
			}
			if m["type"] != "event.new" {
				continue // 跳过初始快照
			}
			if v, ok := m["seq"].(float64); ok {
				seqs <- seqMsg{seq: int64(v), ok: true}
			} else {
				seqs <- seqMsg{}
			}
		}
	}()

	// 探针事件：确认服务端 handler 已完成订阅注册（连接握手完成不代表
	// 订阅就绪），再开始并发提交，避免把注册前的事件误判为丢失。
	// 注册前发出的探针会被永久错过，故轮询重发直至一条到达。
	ready := false
	for attempt := 0; attempt < 50 && !ready; attempt++ {
		eng.Submit(uniqEvent(9000 + attempt))
		select {
		case m, ok := <-seqs:
			if ok && m.ok {
				ready = true
			}
		case <-time.After(200 * time.Millisecond):
		}
	}
	if !ready {
		t.Fatal("探针事件持续未到达，服务端订阅未就绪")
	}

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				eng.Submit(uniqEvent(base*perPub + i))
			}
		}(p)
	}
	wg.Wait()

	got := make([]int64, 0, total)
	deadline := time.After(15 * time.Second)
	for len(got) < total {
		select {
		case m, ok := <-seqs:
			if !ok {
				t.Fatalf("连接中断（%v），仅收到 %d/%d 条", <-readErr, len(got), total)
			}
			if !m.ok {
				t.Fatalf("event.new 缺少 seq 字段")
			}
			got = append(got, m.seq)
		case <-deadline:
			t.Fatalf("超时：仅收到 %d/%d 条", len(got), total)
		}
	}
	c.Close()

	for i := 1; i < len(got); i++ {
		if got[i] != got[i-1]+1 {
			t.Fatalf("seq 乱序或缺失：位置 %d 为 %d（前值 %d，应恰好 +1）", i, got[i], got[i-1])
		}
	}
}

// waitCond 轮询等待条件成立。
func waitCond(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestWSSinceReplayAndLiveHandoff（M2①）：?since=<事件id> 断线补拉——
// 回放按 id 升序完整送达；随后实时事件衔接，全量不重不漏、seq 严格递增。
func TestWSSinceReplayAndLiveHandoff(t *testing.T) {
	_, eng, st, url := newWSTestEnv(t, false)

	// 离线期间入库 A、B、C（模拟客户端断开时网关仍在收事件）
	ids := map[string]int64{}
	for i := 0; i < 3; i++ {
		ev := uniqEvent(i) // type = t0/t1/t2
		eng.Submit(ev)
		rows, err := st.Query("", ev.Type, 0, 1)
		if err != nil || len(rows) != 1 {
			t.Fatalf("离线事件 %d 未入库: err=%v len=%d", i, err, len(rows))
		}
		ids[ev.Type] = rows[0].ID
	}

	// 携带游标重连：从 A 之后补拉（B、C 应回放）
	c := dialWS(t, url+"&since="+fmt.Sprint(ids["t0"]))
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(15 * time.Second))

	var m map[string]any
	if err := c.ReadJSON(&m); err != nil || m["type"] != "state.changed" {
		t.Fatalf("首条应为初始快照: err=%v type=%v", err, m["type"])
	}

	type evMsg struct {
		seq int64
		id  int64
		typ string
		ord int
	}
	var got []evMsg
	readOne := func() evMsg {
		t.Helper()
		for {
			var m map[string]any
			if err := c.ReadJSON(&m); err != nil {
				t.Fatalf("读事件: %v（已收 %d）", err, len(got))
			}
			if m["type"] != "event.new" {
				continue // 跳过其他消息
			}
			data, _ := m["data"].(map[string]any)
			return evMsg{
				seq: int64(m["seq"].(float64)),
				id:  int64(data["id"].(float64)),
				typ: data["type"].(string),
				ord: len(got),
			}
		}
	}

	// 快照到达后再提交实时事件 D、E：快照入队先于订阅注册，
	// 此后落库的事件要么进实时流、要么进回放查询结果（订阅先于查询），
	// 两条路径必居其一，保证不漏。
	d := uniqEvent(100) // type = t100
	eng.Submit(d)
	e := uniqEvent(101) // type = t101
	eng.Submit(e)

	// 收齐 B、C、D、E 四条
	seen := map[string]int{}
	for len(seen) < 4 {
		m := readOne()
		got = append(got, m)
		seen[m.typ]++
	}

	// 全量断言：B、C、D、E 恰好各一条（不重不漏）
	for _, typ := range []string{"t1", "t2", "t100", "t101"} {
		if seen[typ] != 1 {
			t.Fatalf("事件 %s 出现 %d 次（应恰 1 次）", typ, seen[typ])
		}
	}
	// 回放段：B、C 在接收序中相邻且 B 在 C 前（回放临界区保证）
	bi, ci := -1, -1
	for i, m := range got {
		if m.typ == "t1" {
			bi = i
		}
		if m.typ == "t2" {
			ci = i
		}
	}
	if bi < 0 || ci != bi+1 {
		t.Fatalf("回放段 B、C 应相邻且升序: B@%d C@%d", bi, ci)
	}
	if got[bi].seq+1 != got[ci].seq {
		t.Fatalf("回放段 B、C 的 seq 应连续: B=%d C=%d", got[bi].seq, got[ci].seq)
	}
	// seq 全程严格递增（C0-3 语义在补拉路径同样成立）
	for i := 1; i < len(got); i++ {
		if got[i].seq <= got[i-1].seq {
			t.Fatalf("seq 非严格递增: %d → %d", got[i-1].seq, got[i].seq)
		}
	}
}

// 整个补拉窗口应能入队，客户端稍后开始读取也不会被当作慢消费者断开。
func TestWSFullReplayWindow(t *testing.T) {
	s, eng, _, url := newWSTestEnv(t, true)
	for i := 0; i < wsReplayLimit; i++ {
		ev := uniqEvent(i)
		ev.Payload = fmt.Sprintf(`{"data":%q}`, strings.Repeat("x", 4096))
		eng.Submit(ev)
	}
	c := dialWS(t, url+"&since=0")
	defer c.Close()
	c.SetReadDeadline(time.Now().Add(10 * time.Second))
	// 暂不消费，等待服务端积压超过实时队列容量，或观察到连接已被错误关闭。
	if !waitCond(t, 3*time.Second, func() bool {
		s.wsMu.Lock()
		defer s.wsMu.Unlock()
		if len(s.wsConns) == 0 {
			return true
		}
		for conn := range s.wsConns {
			if len(conn.out) > wsQueueSize {
				return true
			}
		}
		return false
	}) {
		t.Fatal("历史补拉未就绪")
	}
	for i := 0; i <= wsReplayLimit; i++ {
		var msg map[string]any
		if err := c.ReadJSON(&msg); err != nil {
			t.Fatalf("整批补拉在第 %d 条中断: %v", i, err)
		}
		if i == 0 {
			if msg["type"] != "state.changed" {
				t.Fatalf("首条缺少状态快照: %v", msg["type"])
			}
			continue
		}
		data, _ := msg["data"].(map[string]any)
		if msg["type"] != "event.new" || data["type"] != fmt.Sprintf("t%d", i-1) {
			t.Fatalf("第 %d 条事件丢失或乱序", i)
		}
	}
}
