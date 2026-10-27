// Package api implements the REST + WebSocket surface (DESIGN §10) and
// serves the embedded web UI.
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
	"github.com/fevenor/konx-gw/internal/media"
	"github.com/fevenor/konx-gw/internal/p2p/session"
	"github.com/fevenor/konx-gw/internal/rtspout"
	"github.com/fevenor/konx-gw/internal/talk"
	webui "github.com/fevenor/konx-gw/internal/web"
)

// Deps wires the API to the app internals.
type Deps struct {
	Cfg    *config.App
	Auth   *auth.Service
	Store  *events.Store
	Engine *events.Engine
	Sess   *session.Session
	Mgr    *media.Manager
	Rtsp   *rtspout.Server
	Talk   *talk.Manager
	HLS    *media.HLSManager
	FF     *media.FFmpeg
	Log    *slog.Logger
}

type Server struct {
	d   Deps
	mux *http.ServeMux

	wsMu    sync.Mutex
	wsSeq   uint64
	wsConns map[*wsConn]struct{}
}

// StateSnapshot is the /api/state document and the WS state.changed body.
type StateSnapshot struct {
	Cloud     string         `json:"cloud"`
	P2P       string         `json:"p2p"`
	P2PServer string         `json:"p2p_server,omitempty"`
	P2PRTTms  int            `json:"p2p_rtt_ms,omitempty"`
	Version   string         `json:"version"`
	Uptime    int64          `json:"uptime"`
	Devices   []DeviceState  `json:"devices"`
	Clients   map[string]int `json:"clients"`
	Source    string         `json:"source"`
}

type DeviceState struct {
	ID     string                        `json:"id"`
	Name   string                        `json:"name"`
	Online bool                          `json:"online"`
	Source map[media.Profile]media.State `json:"source"`
}

func New(d Deps) *Server {
	s := &Server{d: d, mux: http.NewServeMux(), wsConns: map[*wsConn]struct{}{}}
	s.routes()
	return s
}

func (s *Server) routes() {
	A := s.d.Auth
	m := s.mux

	m.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("ok"))
	})
	m.HandleFunc("/metrics", s.metrics)

	// state (also used by config_flow validation)
	m.Handle("/api/state", A.Require(auth.ScopeRead, s.handleState))

	// events
	m.Handle("/api/events/raw", A.Require(auth.ScopeRead, s.handleEventRaw))
	m.HandleFunc("/api/events", s.methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:    A.Require(auth.ScopeRead, s.handleEvents),
		http.MethodDelete: A.Require(auth.ScopeAdmin, s.handleEventsDelete),
	}))

	// devices & media
	m.HandleFunc("/api/devices", s.methodRouter(map[string]http.HandlerFunc{
		http.MethodGet: A.Require(auth.ScopeRead, s.handleDevices),
	}))
	m.Handle("/api/devices/{id}/snapshot", A.Require(auth.ScopeRead, s.handleSnapshot))
	m.Handle("/api/devices/{id}/stream.mjpeg", A.Require(auth.ScopeRead, s.handleMJPEG))
	m.Handle("/api/devices/{id}/hls/{profile}/{file}", A.Require(auth.ScopeRead, s.handleHLSFile))
	m.Handle("/api/devices/{id}/hls-start", A.Require(auth.ScopeRead, s.handleHLSStart))

	// talk
	m.Handle("/api/devices/{id}/talk/start", A.Require(auth.ScopeTalk, s.handleTalkStart))
	m.Handle("/api/devices/{id}/talk/stop", A.Require(auth.ScopeTalk, s.handleTalkStop))
	m.Handle("/api/devices/{id}/talk/audio", A.Require(auth.ScopeTalk, s.handleTalkAudio))
	m.Handle("/api/devices/{id}/talk/ws", A.Require(auth.ScopeTalk, s.handleTalkWS))

	// clients
	m.Handle("/api/clients", A.Require(auth.ScopeRead, s.handleClients))

	// tokens (admin)
	m.HandleFunc("/api/tokens", s.methodRouter(map[string]http.HandlerFunc{
		http.MethodGet:  A.Require(auth.ScopeAdmin, s.handleTokenList),
		http.MethodPost: A.Require(auth.ScopeAdmin, s.handleTokenCreate),
	}))
	m.Handle("/api/tokens/{id}", A.Require(auth.ScopeAdmin, s.handleTokenDelete))

	// websocket
	m.HandleFunc("/api/ws", s.handleWS)

	// web login
	m.HandleFunc("/api/login", s.handleLogin)

	// embedded UI
	m.HandleFunc("/", s.handleUI)
}

func (s *Server) methodRouter(m map[string]http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if h, ok := m[r.Method]; ok {
			h(w, r)
			return
		}
		writeJSONErr(w, 405, "method_not_allowed", r.Method)
	}
}

// Handler returns the root handler.
func (s *Server) Handler() http.Handler { return s.mux }

// ---------------------------------------------------------------------------
// state
// ---------------------------------------------------------------------------

var startAt = time.Now()

const Version = "1.0.0"

func (s *Server) snapshot() StateSnapshot {
	snap := StateSnapshot{
		Version: Version,
		Uptime:  int64(time.Since(startAt).Seconds()),
		Clients: map[string]int{},
		Source:  s.d.Cfg.Media.Source,
	}
	if s.d.Sess != nil {
		snap.P2P = string(s.d.Sess.State())
		if srv, rtt, ok := s.d.Sess.CurrentServer(); ok {
			snap.P2PServer = srv.Addr()
			snap.P2PRTTms = int(rtt.Milliseconds())
		}
		if cs := s.d.Sess.CloudSession(); cs != nil {
			snap.Cloud = "up"
		} else {
			snap.Cloud = "down"
		}
	} else {
		snap.P2P = "disabled"
		snap.Cloud = "down"
	}
	for _, d := range s.d.Cfg.Devices {
		snap.Devices = append(snap.Devices, DeviceState{
			ID: d.ID, Name: d.Name, Online: true, Source: s.d.Mgr.StateOf(d.ID),
		})
	}
	if counts, err := s.d.Store.ClientCounts(); err == nil {
		snap.Clients = counts
	}
	return snap
}

func (s *Server) handleState(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, s.snapshot())
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	since, _ := strconv.ParseInt(q.Get("since"), 10, 64)
	limit, _ := strconv.Atoi(q.Get("limit"))
	evs, err := s.d.Store.Query(q.Get("device"), q.Get("type"), since, limit)
	if err != nil {
		writeJSONErr(w, 500, "store_error", err.Error())
		return
	}
	if q.Get("scope") != "" {
		filtered := evs[:0]
		for _, ev := range evs {
			if s.scopeOf(ev) == q.Get("scope") {
				filtered = append(filtered, ev)
			}
		}
		evs = filtered
	}
	out := make([]map[string]any, len(evs))
	for i, ev := range evs {
		m, _ := json.Marshal(ev)
		var row map[string]any
		_ = json.Unmarshal(m, &row)
		row["scope"] = s.scopeOf(ev)
		row["device_name"] = s.deviceName(ev.DeviceID)
		out[i] = row
	}
	writeJSON(w, 200, map[string]any{"events": out})
}

// scopeOf classifies an event: device-level vs cloud(account)-level vs
// gateway-level (user request 2026-10-24: 事件需要区分账号事件/设备事件).
func (s *Server) scopeOf(ev events.Event) string {
	for _, d := range s.d.Cfg.Devices {
		if ev.DeviceID == d.ID {
			return "device"
		}
	}
	switch ev.DeviceID {
	case "cloud":
		return "cloud"
	default:
		return "gw"
	}
}

func (s *Server) deviceName(id string) string {
	for _, d := range s.d.Cfg.Devices {
		if d.ID == id {
			return d.Name
		}
	}
	return ""
}

func (s *Server) handleEventsDelete(w http.ResponseWriter, r *http.Request) {
	days := s.d.Cfg.Events.RetentionDays
	if v := r.URL.Query().Get("days"); v != "" {
		days, _ = strconv.Atoi(v)
	}
	if err := s.d.Store.Cleanup(days, s.d.Cfg.Events.MaxRows); err != nil {
		writeJSONErr(w, 500, "store_error", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "cleaned"})
}

func (s *Server) handleEventRaw(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.URL.Query().Get("id"), 10, 64)
	hexStr, err := s.d.Store.RawHex(id)
	if err != nil {
		writeJSONErr(w, 404, "not_found", "no raw payload")
		return
	}
	writeJSON(w, 200, map[string]string{"id": r.URL.Query().Get("id"), "raw_hex": hexStr})
}

func (s *Server) handleDevices(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]any{"devices": s.snapshot().Devices})
}

// ---------------------------------------------------------------------------
// snapshot / mjpeg / hls
// ---------------------------------------------------------------------------

func (s *Server) handleSnapshot(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prof := media.ProfileMain
	if r.URL.Query().Get("stream") == "sub" {
		prof = media.ProfileSub
	}
	rel, err := s.d.Mgr.Acquire(id, prof)
	if err != nil {
		writeJSONErr(w, 404, "not_found", err.Error())
		return
	}
	defer rel()
	// give the source a moment to warm up, retry a few times
	lu, lp := s.d.Auth.Loopback()
	var jpg []byte
	for i := 0; i < 6; i++ {
		url := s.d.Rtsp.LoopbackURL(id, prof, lu, lp)
		jpg, err = s.d.FF.Snapshot(r.Context(), url, 5*time.Second)
		if err == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if err != nil {
		writeJSONErr(w, 503, "snapshot_failed", err.Error())
		return
	}
	w.Header().Set("Content-Type", "image/jpeg")
	_, _ = w.Write(jpg)
}

func (s *Server) handleMJPEG(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	rel, err := s.d.Mgr.Acquire(id, media.ProfileSub)
	if err != nil {
		writeJSONErr(w, 404, "not_found", err.Error())
		return
	}
	defer rel()
	lu, lp := s.d.Auth.Loopback()
	w.Header().Set("Content-Type", "multipart/x-mixed-replace; boundary=konxgw")
	stop := r.Context().Done()
	if err := s.d.FF.MJPEGHandler(w, s.d.Rtsp.LoopbackURL(id, media.ProfileSub, lu, lp), stop); err != nil {
		s.d.Log.Debug("mjpeg ended", "err", err)
	}
}

func (s *Server) handleHLSStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prof := media.ProfileMain
	if r.URL.Query().Get("stream") == "sub" {
		prof = media.ProfileSub
	}
	if _, err := s.d.Mgr.Acquire(id, prof); err != nil {
		writeJSONErr(w, 404, "not_found", err.Error())
		return
	}
	lu, lp := s.d.Auth.Loopback()
	key := id + "|" + string(prof)
	if _, err := s.d.HLS.Ensure(key, s.d.Rtsp.LoopbackURL(id, prof, lu, lp)); err != nil {
		writeJSONErr(w, 500, "hls_failed", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"playlist": fmt.Sprintf("/api/devices/%s/hls/%s/index.m3u8", id, prof)})
}

func (s *Server) handleHLSFile(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prof := media.Profile(r.PathValue("profile"))
	file := r.PathValue("file")
	if file != "index.m3u8" && !strings.HasSuffix(file, ".ts") {
		http.NotFound(w, r)
		return
	}
	key := id + "|" + string(prof)
	playlist, _ := s.d.HLS.Ensure(key, "")
	http.ServeFile(w, r, playlist[:len(playlist)-len("index.m3u8")]+file)
}

// ---------------------------------------------------------------------------
// talk
// ---------------------------------------------------------------------------

func (s *Server) handleTalkStart(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, err := s.d.Talk.Start(r.Context(), id, "api")
	if err != nil {
		if err == talk.ErrUnavailable {
			writeJSONErr(w, 501, "talk_unavailable", err.Error()+"; reason=awaiting_p2p_av_module")
			return
		}
		writeJSONErr(w, 409, "talk_busy", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "started"})
}

func (s *Server) handleTalkStop(w http.ResponseWriter, r *http.Request) {
	s.d.Talk.Stop(r.PathValue("id"))
	writeJSON(w, 200, map[string]string{"status": "stopped"})
}

func (s *Server) handleTalkAudio(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		writeJSONErr(w, 400, "bad_request", err.Error())
		return
	}
	// accept audio/L16 raw or WAV (strip 44-byte header if present)
	if len(body) > 44 && body[0] == 'R' && body[1] == 'I' && body[2] == 'F' && body[3] == 'F' {
		body = body[44:]
	}
	if err := s.d.Talk.WritePCM16(id, body); err != nil {
		writeJSONErr(w, 409, "talk_failed", err.Error())
		return
	}
	s.d.Talk.Ping(id)
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

var talkUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *Server) handleTalkWS(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if _, err := s.d.Talk.Start(r.Context(), id, "web"); err != nil {
		if err == talk.ErrUnavailable {
			writeJSONErr(w, 501, "talk_unavailable", err.Error()+"; reason=awaiting_p2p_av_module")
			return
		}
		writeJSONErr(w, 409, "talk_busy", err.Error())
		return
	}
	defer s.d.Talk.Stop(id)
	c, err := talkUpgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer c.Close()
	go func() {
		// ping loop keeps the session heartbeat alive
		for {
			time.Sleep(time.Second)
			s.d.Talk.Ping(id)
		}
	}()
	for {
		mt, data, err := c.ReadMessage()
		if err != nil {
			return
		}
		if mt == websocket.BinaryMessage {
			if err := s.d.Talk.WritePCM16(id, data); err != nil {
				_ = c.WriteJSON(map[string]string{"error": err.Error()})
				return
			}
		}
	}
}

// ---------------------------------------------------------------------------
// clients / tokens / login
// ---------------------------------------------------------------------------

func (s *Server) handleClients(w http.ResponseWriter, r *http.Request) {
	clients, err := s.d.Store.ActiveClients()
	if err != nil {
		writeJSONErr(w, 500, "store_error", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"clients": clients})
}

func (s *Server) handleTokenList(w http.ResponseWriter, r *http.Request) {
	toks, err := s.d.Auth.ListTokens()
	if err != nil {
		writeJSONErr(w, 500, "store_error", err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"tokens": toks})
}

func (s *Server) handleTokenCreate(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Name   string   `json:"name"`
		Scopes []string `json:"scopes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONErr(w, 400, "bad_request", err.Error())
		return
	}
	if len(req.Scopes) == 0 {
		req.Scopes = []string{auth.ScopeRead}
	}
	tok, err := s.d.Auth.CreateToken(req.Name, req.Scopes)
	if err != nil {
		writeJSONErr(w, 400, "bad_request", err.Error())
		return
	}
	writeJSON(w, 201, tok)
}

func (s *Server) handleTokenDelete(w http.ResponseWriter, r *http.Request) {
	if err := s.d.Auth.DeleteToken(r.PathValue("id")); err != nil {
		writeJSONErr(w, 500, "store_error", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"status": "deleted"})
}

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	ip := r.RemoteAddr
	if i := strings.LastIndex(ip, ":"); i > 0 {
		ip = ip[:i]
	}
	if !s.d.Auth.LoginRateLimited(ip) {
		writeJSONErr(w, 429, "rate_limited", "too many attempts")
		return
	}
	var req struct {
		Password string `json:"password"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if !s.d.Auth.CheckAdmin(req.Password) {
		writeJSONErr(w, 401, "bad_credentials", "wrong password")
		return
	}
	// mint a short-lived session token (admin scope)
	tok, err := s.d.Auth.CreateToken("web-session", []string{auth.ScopeAdmin})
	if err != nil {
		writeJSONErr(w, 500, "internal", err.Error())
		return
	}
	writeJSON(w, 200, map[string]string{"token": tok.Secret})
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeJSONErr(w http.ResponseWriter, code int, ecode, msg string) {
	writeJSON(w, code, map[string]any{"error": map[string]string{"code": ecode, "message": msg}})
}

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

var _ = context.Background

// handleUI serves the embedded dashboard; media endpoints accept a token
// query parameter because <img> tags cannot set headers.
func (s *Server) handleUI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		// static assets if any
		http.FileServer(http.FS(webFS())).ServeHTTP(w, r)
		return
	}
	b, err := fs.ReadFile(webFS(), "index.html")
	if err != nil {
		http.Error(w, "ui missing", 500)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(b)
}

func webFS() fs.FS { return webui.FS() }
