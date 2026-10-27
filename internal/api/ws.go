package api

import (
	"net/http"
	"sync"

	"github.com/gorilla/websocket"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/events"
)

// wsConn is one live dashboard/websocket consumer.
type wsConn struct {
	c  *websocket.Conn
	mu sync.Mutex
}

var wsUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

// handleWS streams state.changed / event.new / client.changed messages.
// Sequence numbers let clients resume with ?since= after a drop.
func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	// token may come from the query string for browser contexts
	if tok := r.URL.Query().Get("token"); tok != "" && r.Header.Get("Authorization") == "" {
		r.Header.Set("Authorization", "Bearer "+tok)
	}
	s.d.Auth.Require(auth.ScopeRead, func(w http.ResponseWriter, r *http.Request) {
		c, err := wsUpgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		conn := &wsConn{c: c}
		s.wsMu.Lock()
		s.wsSeq++
		s.wsConns[conn] = struct{}{}
		s.wsMu.Unlock()
		defer func() {
			s.wsMu.Lock()
			delete(s.wsConns, conn)
			s.wsMu.Unlock()
			c.Close()
			s.storeTouch("web")
		}()
		s.storeTouch("web")

		// initial state burst
		s.wsSend(conn, "state.changed", s.snapshot())

		unsub := s.d.Engine.Subscribe(func(ev events.Event) {
			s.wsMu.Lock()
			s.wsSeq++
			seq := s.wsSeq
			s.wsMu.Unlock()
			s.wsSendSeq(conn, seq, "event.new", ev)
		})
		defer unsub()
		// pump: discard client messages, detect close
		for {
			if _, _, err := c.ReadMessage(); err != nil {
				return
			}
		}
	})(w, r)
}

func (s *Server) wsSend(conn *wsConn, typ string, data any) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	_ = conn.c.WriteJSON(map[string]any{"type": typ, "data": data})
}

func (s *Server) wsSendSeq(conn *wsConn, seq uint64, typ string, data any) {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	_ = conn.c.WriteJSON(map[string]any{"seq": seq, "type": typ, "data": data})
}

// BroadcastState pushes a fresh snapshot to every connected client.
func (s *Server) BroadcastState() {
	snapshot := s.snapshot()
	s.wsMu.Lock()
	conns := make([]*wsConn, 0, len(s.wsConns))
	for c := range s.wsConns {
		conns = append(conns, c)
	}
	s.wsMu.Unlock()
	for _, c := range conns {
		s.wsSend(c, "state.changed", snapshot)
	}
}

// BroadcastClientChange notifies client.changed.
func (s *Server) BroadcastClientChange() {
	counts, _ := s.d.Store.ClientCounts()
	s.wsMu.Lock()
	conns := make([]*wsConn, 0, len(s.wsConns))
	for c := range s.wsConns {
		conns = append(conns, c)
	}
	s.wsMu.Unlock()
	for _, c := range conns {
		s.wsSend(c, "client.changed", counts)
	}
}

func (s *Server) storeTouch(kind string) {
	_ = s.d.Store.UpsertClient("web-"+kind, "web", "", "")
}
