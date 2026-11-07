package api

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/events"
)

// WebSocket 连接参数（var 便于测试中缩短时限）：
//   - 每次写都设写超时，写超时即断开该连接，单个慢/半开客户端不会拖停全局；
//   - 服务端周期 ping + 读超时 + PongHandler，NAT 半开死连接会被及时清理；
//   - 每连接带缓冲发送队列 + 唯一发送协程，队列已满即断开该慢消费者。
var (
	wsWriteWait  = 10 * time.Second // 单帧写超时，超时即断开该连接
	wsPingPeriod = 30 * time.Second // 服务端 ping 周期
	wsPongWait   = 70 * time.Second // 读超时（大于 ping 周期，容忍一次 ping 丢失）
	wsQueueSize  = 64               // 每连接发送队列容量
)

// wsReplayLimit 是 ?since= 断线补拉的有界回放窗口：最多回放最近 500 条，
// 更早的事件不再经 WS 回放（客户端转 REST /api/events 查询兜底）。
const wsReplayLimit = 500

// wsOutMsg 是发送队列中的一条待写消息：数据帧或 ping 控制帧。
type wsOutMsg struct {
	ping bool
	data []byte
}

// wsConn is one live dashboard/websocket consumer.
// 所有写操作经 out 队列交给唯一发送协程串行完成（FIFO 保序，无并发写冲突）；
// 读侧超时、写侧超时、队列溢出任一发生都会经 shutdown 统一断开并注销。
type wsConn struct {
	c        *websocket.Conn
	mu       sync.Mutex // 保护 shutdown 只执行一次
	out      chan wsOutMsg
	done     chan struct{}
	clientID string // 客户端注册表 id（每连接唯一，断开时精确注销）
}

var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// handleWS streams state.changed / event.new / client.changed messages.
// Sequence numbers let clients resume with ?since=<事件id> after a drop:
// 连接建立时先从事件库回放该连接缺漏区间（有界），再进入实时流。
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	// token may come from the query string for browser contexts
	if tok := r.URL.Query().Get("token"); tok != "" && r.Header.Get("Authorization") == "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	// 断线补拉游标：?since=<最后收到的事件 id>（REST /api/events 同一语义）
	replaySince := int64(-1)
	if v := r.URL.Query().Get("since"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			replaySince = n
		}
	}
	s.d.Auth.Require(auth.ScopeRead, func(w http.ResponseWriter, r *http.Request) {
		c, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		queueSize := wsQueueSize
		if replaySince >= 0 && queueSize < wsReplayLimit+1 {
			// 首次状态加整批补拉可立即入队，避免正常历史回放被当作慢客户端。
			queueSize = wsReplayLimit + 1
		}
		conn := &wsConn{
			c:    c,
			out:  make(chan wsOutMsg, queueSize),
			done: make(chan struct{}),
		}
		s.wsMu.Lock()
		s.wsSeq++
		// 每连接唯一注册表 id：多看板连接各自成行，断开只注销自身
		conn.clientID = fmt.Sprintf("web-%d", s.wsSeq)
		s.wsConns[conn] = struct{}{}
		s.wsMu.Unlock()
		defer func() {
			// 断开路径统一注销（并发安全）：客户端注册表置为断开
			conn.shutdown()
			s.wsMu.Lock()
			delete(s.wsConns, conn)
			s.wsMu.Unlock()
			c.Close()
			s.storeUnregister(conn.clientID)
		}()
		s.storeRegister(conn.clientID)

		// 读超时 + PongHandler 刷新：超时未收到 pong 即判定半开/死连接（S5）
		_ = c.SetReadDeadline(time.Now().Add(wsPongWait))
		c.SetPongHandler(func(string) error {
			return c.SetReadDeadline(time.Now().Add(wsPongWait))
		})

		// 唯一发送协程：串行写出数据帧与 ping 帧（S4/S5）
		go s.wsWriter(conn)

		// initial state burst
		s.wsEnqueue(conn, 0, "state.changed", s.snapshot())

		// replaySeen 记录补拉期间已通过实时流收到的事件 id（补放回放
		// 时据此跳过，保证回放与实时流衔接不重不漏）；回放完成后置 nil。
		var replaySeen map[int64]bool
		if replaySince >= 0 {
			replaySeen = make(map[int64]bool)
		}
		unsub := s.d.Engine.Subscribe(func(ev events.Event) {
			// seq 分配与入队必须在同一临界区原子完成，保证该连接内
			// 消息按 seq 严格递增有序送达（C0-3）。入队为非阻塞尝试，
			// 持锁安全，慢消费者只会断开自身。
			s.wsMu.Lock()
			s.wsSeq++
			s.wsEnqueue(conn, s.wsSeq, "event.new", ev)
			if replaySeen != nil {
				replaySeen[ev.ID] = true
			}
			s.wsMu.Unlock()
		})
		defer unsub()

		// 断线补拉：先订阅再查库——订阅前入库的事件只出现在查询结果里，
		// 订阅后入库的事件既在实时流也会出现在查询结果里（跳过），
		// 两者合并恰好覆盖 since 之后的全部事件，不重不漏。
		if replaySince >= 0 {
			missed, qerr := s.d.Store.QuerySince(replaySince, wsReplayLimit)
			if qerr == nil {
				s.wsMu.Lock()
				for _, ev := range missed { // QuerySince 已按 id 升序
					if replaySeen[ev.ID] {
						continue // 已由实时流送达，跳过避免重复
					}
					s.wsSeq++
					s.wsEnqueue(conn, s.wsSeq, "event.new", ev)
				}
				replaySeen = nil
				s.wsMu.Unlock()
			} else {
				slog.Default().Warn("断线补拉查询失败，跳过回放", "since", replaySince, "err", qerr)
				s.wsMu.Lock()
				replaySeen = nil
				s.wsMu.Unlock()
			}
		}

		// pump: discard client messages, detect close
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
			// 收到任何客户端数据同样证明连接存活，一并刷新读超时
			_ = c.SetReadDeadline(time.Now().Add(wsPongWait))
		}
	})(w, r)
}

// shutdown 幂等断开：通知发送协程退出并关闭底层连接，
// 读侧随即因 ReadMessage 出错返回，走统一注销路径。
func (conn *wsConn) shutdown() {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	select {
	case <-conn.done:
		return // 已断开
	default:
	}
	close(conn.done)
	conn.c.Close()
}

// tryEnqueue 把消息放入发送队列；队列已满说明该客户端消费过慢，
// 立即断开该连接。全程非阻塞，绝不拖住调用方（S4）。
func (conn *wsConn) tryEnqueue(m wsOutMsg) {
	select {
	case <-conn.done:
		return
	default:
	}
	select {
	case conn.out <- m:
	default:
		conn.shutdown()
	}
}

// wsEnqueue 序列化一条消息并入队；seq 为 0 表示不带 seq 字段的消息。
func (s *Server) wsEnqueue(conn *wsConn, seq uint64, typ string, data any) {
	conn.tryEnqueue(wsOutMsg{data: wsMarshal(seq, typ, data)})
}

// wsMarshal 序列化保持既有消息格式：{"seq":n,"type":t,"data":d}，无 seq 时省略。
func wsMarshal(seq uint64, typ string, data any) []byte {
	m := map[string]any{"type": typ, "data": data}
	if seq != 0 {
		m["seq"] = seq
	}
	b, err := json.Marshal(m)
	if err != nil {
		return nil
	}
	return b
}

// wsWriter 是每连接唯一的发送协程：串行写出队列消息并周期性发送 ping；
// 写失败/写超时即断开该连接并退出。ping 周期顺带刷新客户端注册表的
// last_active，保证活跃连接不会被 TTL 兜底清理误判为僵尸行。
func (s *Server) wsWriter(conn *wsConn) {
	ticker := time.NewTicker(wsPingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-conn.done:
			return
		case <-ticker.C:
			_ = s.d.Store.TouchClient(conn.clientID)
			if !s.wsWriteFrame(conn, wsOutMsg{ping: true}) {
				return
			}
		case m, ok := <-conn.out:
			if !ok {
				return
			}
			if !s.wsWriteFrame(conn, m) {
				return
			}
		}
	}
}

// wsWriteFrame 执行一次带写超时的写（S4）；返回 false 表示连接应断开。
func (s *Server) wsWriteFrame(conn *wsConn, m wsOutMsg) bool {
	_ = conn.c.SetWriteDeadline(time.Now().Add(wsWriteWait))
	var err error
	if m.ping {
		err = conn.c.WriteMessage(websocket.PingMessage, nil)
	} else {
		err = conn.c.WriteMessage(websocket.TextMessage, m.data)
	}
	if err != nil {
		// 写超时或写失败：断开该连接，防止慢客户端拖停全局
		conn.shutdown()
		return false
	}
	return true
}

// wsConnList 返回当前连接列表快照（并发安全）。
func (s *Server) wsConnList() []*wsConn {
	s.wsMu.Lock()
	conns := make([]*wsConn, 0, len(s.wsConns))
	for c := range s.wsConns {
		conns = append(conns, c)
	}
	s.wsMu.Unlock()
	return conns
}

// BroadcastState pushes a fresh snapshot to every connected client.
func (s *Server) BroadcastState() {
	snapshot := s.snapshot()
	for _, conn := range s.wsConnList() {
		s.wsEnqueue(conn, 0, "state.changed", snapshot)
	}
}

// BroadcastClientChange notifies client.changed.
func (s *Server) BroadcastClientChange() {
	counts, _ := s.d.Store.ClientCounts()
	for _, conn := range s.wsConnList() {
		s.wsEnqueue(conn, 0, "client.changed", counts)
	}
}

// storeRegister 登记一个已连接的 web 客户端（置 active=1）。
func (s *Server) storeRegister(id string) {
	_ = s.d.Store.UpsertClient(id, "web", "", "")
}

// storeUnregister 注销客户端（置 active=0，含断开时刻）。
func (s *Server) storeUnregister(id string) {
	_ = s.d.Store.CloseClient(id)
}
