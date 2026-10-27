// Package api implements the REST + WebSocket surface (DESIGN §10) and
// serves the embedded web UI.
package api

import (
	"context"
	"encoding/binary"
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

	// hlsMu 保护 hlsRefs：key=device|profile → hls-start 时 Acquire 得
	// 到的源释放函数。hls-start 幂等（已登记不重复 Acquire），配套
	// hls-stop 释放；修复旧实现每次 hls-start 丢弃 Acquire 的释放
	// 函数导致源引用只增不减（源永不空闲停止）的泄漏。
	hlsMu   sync.Mutex
	hlsRefs map[string]func()

	// snapMu 保护 snapLast：快照冷却缓存（M6，见 snapshotCooldown）。
	snapMu   sync.Mutex
	snapLast map[string]snapResult
}

// snapshotCooldown 服务端快照冷却窗（M6）：session_hold=false（默认）
// 时每次快照 Acquire 都是一次完整 P2P 呼叫，而云端对高频呼叫有
// 15-30 分钟节流惩罚窗——冷却窗内的重复请求直接返回最近一次结果
// （成功回放缓存的 JPEG，失败回放 503），不再触发挥源/呼叫。
const snapshotCooldown = 5 * time.Second

// snapResult 是快照冷却缓存的一条记录。
type snapResult struct {
	jpg []byte
	err error
	at  time.Time
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
	s := &Server{
		d:        d,
		mux:      http.NewServeMux(),
		wsConns:  map[*wsConn]struct{}{},
		hlsRefs:  map[string]func(){},
		snapLast: map[string]snapResult{},
	}
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
	// /metrics 与 API 同源鉴权（默认开启，web.metrics_auth 可关，供可信
	// 内网抓取）；关闭后指标含设备/客户端规模信息，只应在隔离网络使用。
	if s.metricsAuthEnabled() {
		m.Handle("/metrics", A.Require(auth.ScopeRead, s.metrics))
	} else {
		m.HandleFunc("/metrics", s.metrics)
	}

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
	m.Handle("/api/devices/{id}/hls-stop", A.Require(auth.ScopeRead, s.handleHLSStop))

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

// metricsAuthEnabled 报告 /metrics 是否要求鉴权；未配置（nil）默认
// 开启，杜绝零值配置意外把指标暴露出去。
func (s *Server) metricsAuthEnabled() bool {
	if s.d.Cfg == nil {
		return true
	}
	ma := s.d.Cfg.Web.MetricsAuth
	return ma == nil || *ma
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
			ID: d.ID, Name: d.Name, Online: s.deviceOnline(d.ID), Source: s.d.Mgr.StateOf(d.ID),
		})
	}
	if counts, err := s.d.Store.ClientCounts(); err == nil {
		snap.Clients = counts
	}
	return snap
}

// deviceOnline 基于 Manager 的媒体源槽位状态推导设备在线（C2-4）：
// 设备自身的在网/心跳上报要等 p2p-av 设备会话模块落地，当前取值语义
// 见 onlineFromStates——简言之"任一 profile 的取流链路存活（含建立中
// 与 session_hold 保持态）即在线"，无槽位/已拆链/源错误即离线。
func (s *Server) deviceOnline(id string) bool {
	return onlineFromStates(s.d.Mgr.StateOf(id))
}

// onlineFromStates 的取值语义：
//   - starting：取流已发起（源握手/呼叫建立中）→ 在线；
//   - active：媒体链路确认存活 → 在线；
//   - paused：session_hold 保持态（链路未拆、可复活）→ 在线；
//   - stopped（无槽位或已空闲拆链）/ error（源错误）→ 离线。
func onlineFromStates(states map[media.Profile]media.State) bool {
	for _, st := range states {
		switch st {
		case media.StateStarting, media.StateActive, media.StatePaused:
			return true
		}
	}
	return false
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
	// 快照冷却（M6）：冷却窗内的重复请求直接回放最近一次结果，不再次
	// Acquire——session_hold=false 默认下每次快照都是一次完整 P2P 呼叫，
	// HA 侧轮询会撞上云端 15-30 分钟呼叫节流窗。
	key := id + "|" + string(prof)
	if res, ok := s.snapshotCacheGet(key); ok {
		if res.err != nil {
			writeJSONErr(w, 503, "snapshot_cooldown", res.err.Error())
			return
		}
		w.Header().Set("Content-Type", "image/jpeg")
		_, _ = w.Write(res.jpg)
		return
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
		s.snapshotCachePut(key, snapResult{err: err, at: time.Now()})
		writeJSONErr(w, 503, "snapshot_failed", err.Error())
		return
	}
	s.snapshotCachePut(key, snapResult{jpg: jpg, at: time.Now()})
	w.Header().Set("Content-Type", "image/jpeg")
	_, _ = w.Write(jpg)
}

// snapshotCacheGet 取冷却窗内的最近一次快照结果；过期或无记录返回 miss。
func (s *Server) snapshotCacheGet(key string) (snapResult, bool) {
	s.snapMu.Lock()
	defer s.snapMu.Unlock()
	res, ok := s.snapLast[key]
	if !ok || time.Since(res.at) >= snapshotCooldown {
		return snapResult{}, false
	}
	return res, true
}

// snapshotCachePut 记录一次快照结果（成功缓存 JPEG，失败缓存错误）。
func (s *Server) snapshotCachePut(key string, res snapResult) {
	s.snapMu.Lock()
	s.snapLast[key] = res
	s.snapMu.Unlock()
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
	key := id + "|" + string(prof)
	playlist := fmt.Sprintf("/api/devices/%s/hls/%s/index.m3u8", id, prof)
	lu, lp := s.d.Auth.Loopback()

	// 整个"查重 → Acquire → Ensure → 登记"持锁完成：已登记（已启动）
	// 直接幂等返回播放列表，不再重复 Acquire——旧实现每次调用都丢弃
	// Acquire 返回的释放函数，源引用只增不减，源永不空闲停止（C1-2）。
	s.hlsMu.Lock()
	if _, ok := s.hlsRefs[key]; ok {
		s.hlsMu.Unlock()
		writeJSON(w, 200, map[string]string{"playlist": playlist})
		return
	}
	rel, err := s.d.Mgr.Acquire(id, prof)
	if err != nil {
		s.hlsMu.Unlock()
		writeJSONErr(w, 404, "not_found", err.Error())
		return
	}
	if _, err := s.d.HLS.Ensure(key, s.d.Rtsp.LoopbackURL(id, prof, lu, lp)); err != nil {
		rel() // HLS 起不来：立即归还刚取的源引用
		s.hlsMu.Unlock()
		writeJSONErr(w, 500, "hls_failed", err.Error())
		return
	}
	s.hlsRefs[key] = rel
	s.hlsMu.Unlock()
	writeJSON(w, 200, map[string]string{"playlist": playlist})
}

// handleHLSStop 是 hls-start 的配套释放端点：归还源引用并停掉 HLS
// 管线（未启动时为无害的幂等操作）。
func (s *Server) handleHLSStop(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	prof := media.ProfileMain
	if r.URL.Query().Get("stream") == "sub" {
		prof = media.ProfileSub
	}
	key := id + "|" + string(prof)
	s.hlsMu.Lock()
	rel, ok := s.hlsRefs[key]
	delete(s.hlsRefs, key)
	s.hlsMu.Unlock()
	if ok {
		rel()
	}
	if s.d.HLS != nil {
		s.d.HLS.Stop(key)
	}
	writeJSON(w, 200, map[string]string{"status": "stopped"})
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
	// 会话生命周期独立于本次 HTTP 请求（M5）：REST 触发的对讲不能随
	// 响应返回而消亡，回收靠显式 talk/stop 或心跳看门狗超时。故传
	// context.Background，不绑 r.Context()。
	_, err := s.d.Talk.Start(context.Background(), id, "api")
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
	// 接受 audio/L16 裸 PCM 或 WAV 容器：按 RIFF/WAVE chunk 遍历定位
	// data 块后再剥离（旧实现固定剥 44 字节，遇到带 LIST/_fact 等附加
	// 块的非标准头会把有效音频一起切掉）；非 WAV 数据（无 RIFF 魔数）
	// 原样透传（M8）。
	body = stripWAVHeader(body)
	if err := s.d.Talk.WritePCM16(id, body); err != nil {
		writeJSONErr(w, 409, "talk_failed", err.Error())
		return
	}
	s.d.Talk.Ping(id)
	writeJSON(w, 200, map[string]string{"status": "ok"})
}

// stripWAVHeader 解析 WAV（RIFF/WAVE）容器头并返回 data 块载荷：
//   - 标准/非标准头长（fmt 之后跟 LIST、_fact 等附加块）都能正确剥离；
//   - data 块声明长度异常（0 或越界，部分流式编码器如此）时取剩余全部；
//   - 无 RIFF 魔数（裸 PCM 上传）或结构损坏时原样透传，交由下游处理。
func stripWAVHeader(b []byte) []byte {
	if len(b) < 12 || string(b[0:4]) != "RIFF" || string(b[8:12]) != "WAVE" {
		return b
	}
	off := 12
	for off+8 <= len(b) {
		id := string(b[off : off+4])
		size := int(binary.LittleEndian.Uint32(b[off+4 : off+8]))
		off += 8
		if id == "data" {
			end := off + size
			if size == 0 || end > len(b) {
				end = len(b)
			}
			return b[off:end]
		}
		off += size
		if size%2 == 1 {
			off++ // RIFF chunk 按 2 字节对齐（奇数长度后有 1 填充字节）
		}
	}
	return b
}

var talkUpgrader = websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

func (s *Server) handleTalkWS(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	// 同 M5：会话生命周期跟 WS 连接走（下方 defer Stop），不绑请求 ctx。
	sess, err := s.d.Talk.Start(context.Background(), id, "web")
	if err != nil {
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
	done := make(chan struct{})
	defer close(done)
	go func() {
		// 心跳保活循环（C1-1）：必须在连接关闭（done）、会话被停止或被
		// 他人重建（Active 不再是本会话）时退出——旧实现 for+sleep 无
		// 退出条件，每条 talk WS 连接泄漏一个常驻协程。
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				if s.d.Talk.Active(id) != sess {
					return
				}
				s.d.Talk.Ping(id)
			}
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
	// 铸造带 TTL 的网页登录会话令牌（admin 权限）：有效期由
	// web.session_ttl 配置（默认 24h），到期在校验时惰性吊销——见
	// auth.CreateSessionToken。旧实现铸造的是永久 admin 令牌且无清理。
	tok, err := s.d.Auth.CreateSessionToken(s.d.Cfg.Web.SessionTTL)
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
