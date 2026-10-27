// Package rtspout is the RTSP re-distribution service on :8554
// (DESIGN §5.2). Paths: /konx/{device}/{main|sub}. Downstream auth is
// HTTP Basic against the gateway credential table; loopback pipelines
// (ffmpeg snapshot/HLS/MJPEG) authenticate with the internal loopback
// identity.
package rtspout

import (
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/media"
)

var _ sourceManager = (*media.Manager)(nil)

// Authorizer checks RTSP Basic credentials.
type Authorizer interface {
	CheckRTSP(user, pass string) bool
}

// ClientHook is called on session open/close for the client registry.
type ClientHook func(kind string, up bool)

// sourceManager 是出口层对媒体源管理器的依赖缝（生产 *media.Manager，
// 测试注入计数 fake）。出口层所有路径统一传「归一化 profile」，键格式
// device|profile。
type sourceManager interface {
	Acquire(deviceID string, p media.Profile) (func(), error)
	Release(deviceID string, p media.Profile)
	Desc(deviceID string, p media.Profile) *description.Session
	HasSlot(deviceID string, p media.Profile) bool
	Prewarm(deviceID string)
	NormProfile(p media.Profile) media.Profile
	RegisterWriter(deviceID string, p media.Profile, fn func(*description.Media, *rtp.Packet))
	UnregisterWriter(deviceID string, p media.Profile)
}

// streamEntry 是一个 (device, 归一化 profile) 的共享出口流：多个观众
// 会话复用同一 ServerStream；viewers 为持有该键的会话数（INV1：
// viewers==持有该键的会话数；INV2：viewers>0 ⟹ 源槽存活——每个
// 记账会话在 SETUP 时恰 Acquire 一次源引用）。
type streamEntry struct {
	ss      *gortsplib.ServerStream
	viewers int
}

// sessionRec 是一个观众会话的出口记账：keys 为该会话 SETUP 成功记过
// 账的出口键集合（同会话多 track 去重，每键恰一次 Acquire/Release）；
// played 标记是否进入过 PLAY（hook 下行计数以 played 为准）。
type sessionRec struct {
	keys   map[string]bool
	played bool
}

type Server struct {
	srv  *gortsplib.Server
	mgr  sourceManager
	auth Authorizer
	hook ClientHook
	log  *slog.Logger

	// DescribeWait 是 DESCRIBE/SETUP 在“源已发起但描述未就绪”（p2p-av
	// 呼叫建立 ~5-8s）时的最长等待；仅当 Manager 已有该 (device,profile)
	// 槽位时等待，否则立即 404。0 = 默认 10s。
	DescribeWait time.Duration

	// prewarmCooldown 是 OnDescribe 404 分支对同一设备触发 Prewarm 的
	// 最小间隔：认证已通过的 DESCRIBE 视为观看意图，允许触发 p2p 呼叫
	// （无凭据请求在 authorize 即被 401 拦截，不会烧呼叫节流窗）；
	// 冷却只防合法凭据持有者的 DESCRIBE 轮询风暴反复拆建链。
	prewarmCooldown time.Duration

	prewarmedAt map[string]time.Time

	// 出口 per-session 记账。INV3：条目的建立（Initialize+RegisterWriter）
	// 与销毁（Close+UnregisterWriter）全部在 mu 临界区内配对完成；
	// INV4 锁序 mu → Manager.mu(Desc/Acquire/Release) → writer 锁，
	// Manager 不在持自身锁时回调出口层（ss.Close 仅异步取消 reader，
	// 持 mu 调用无死锁）。
	mu       sync.Mutex
	streams  map[string]*streamEntry // key device|profile（归一化）
	sessions map[*gortsplib.ServerSession]*sessionRec
}

func New(listen string, mgr sourceManager, auth Authorizer, hook ClientHook, log *slog.Logger) *Server {
	s := &Server{
		mgr: mgr, auth: auth, hook: hook, log: log,
		streams:     map[string]*streamEntry{},
		sessions:    map[*gortsplib.ServerSession]*sessionRec{},
		prewarmedAt: map[string]time.Time{},
	}
	s.srv = &gortsplib.Server{
		Handler:     s,
		RTSPAddress: listen,
	}
	return s
}

// Start begins listening; returns error like ListenAndServe.
func (s *Server) Start() error { return s.srv.Start() }

// Wait blocks until the server fails.
func (s *Server) Wait() error { return s.srv.Wait() }

// Close shuts the server down.
func (s *Server) Close() { s.srv.Close() }

// parsePath extracts device+profile from /konx/{dev}/{profile}.
func parsePath(p string) (dev string, prof media.Profile, ok bool) {
	p = strings.Trim(p, "/")
	parts := strings.Split(p, "/")
	if len(parts) != 3 || parts[0] != "konx" {
		return "", "", false
	}
	if parts[2] != "main" && parts[2] != "sub" {
		return "", "", false
	}
	return parts[1], media.Profile(parts[2]), true
}

func (s *Server) authorize(req *base.Request) bool {
	if s.auth == nil {
		return true
	}
	var h string
	if hv, ok := req.Header["Authorization"]; ok && len(hv) > 0 {
		h = hv[0]
	}
	const prefix = "Basic "
	if !strings.HasPrefix(h, prefix) {
		return false
	}
	user, pass, ok := parseBasic(h[len(prefix):])
	if !ok {
		return false
	}
	return s.auth.CheckRTSP(user, pass)
}

func (s *Server) challenge(resp *base.Response) *base.Response {
	resp.StatusCode = base.StatusUnauthorized
	if resp.Header == nil {
		resp.Header = base.Header{}
	}
	resp.Header["WWW-Authenticate"] = base.HeaderValue{`Basic realm="konx-gw"`}
	return resp
}

// errNotReady 表示源描述尚未就绪（槽位在建或呼叫建立中）。
var errNotReady = errors.New("stream not ready")

// streamFor 查找或建立 (device, 归一化 profile) 的出口流条目。查-建-存
// 全程持 mu（消灭并发双建窗口）；命中/新建均不改变 viewers、不占源
// 引用——DESCRIBE 复用保持预热语义，源引用计数只由 SETUP 持有。
func (s *Server) streamFor(dev string, prof media.Profile) (*streamEntry, error) {
	key := dev + "|" + string(prof)
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.streams[key]; ok {
		return e, nil
	}
	desc := s.mgr.Desc(dev, prof)
	if desc == nil {
		return nil, errNotReady
	}
	ss := &gortsplib.ServerStream{Server: s.srv, Desc: desc}
	if err := ss.Initialize(); err != nil {
		return nil, err
	}
	// route upstream packets into the server stream
	s.mgr.RegisterWriter(dev, prof, func(md *description.Media, pkt *rtp.Packet) {
		// gortsplib v4 ServerStream 在 Close 后再写会内部 panic（nil
		// deref）：观众断开→拆条目与呼叫拆链尾包竞态属正常时序，
		// egress 是尽力而为路径——吞掉已关闭流的写入 panic。
		defer func() { _ = recover() }()
		if err := ss.WritePacketRTP(md, pkt); err != nil {
			s.log.Debug("write rtp", "err", err)
		}
	})
	e := &streamEntry{ss: ss}
	s.streams[key] = e
	return e, nil
}

// closeEntryLocked 拆除一个出口流条目（调用方须持 mu）。幂等：条目不
// 存在直接返回。与 streamFor 的建立路径在同一锁内配对（INV3）；
// ss.Close 仅异步取消 reader 会话（其 OnSessionClose 在本临界区外由
// 服务循环排队执行），持锁调用无死锁（INV4）。
func (s *Server) closeEntryLocked(key, dev string, prof media.Profile) {
	e, ok := s.streams[key]
	if !ok {
		return
	}
	delete(s.streams, key)
	e.ss.Close()
	s.mgr.UnregisterWriter(dev, prof)
}

// splitKey 拆解 device|profile 键（出口记账键的逆运算）。
func splitKey(key string) (dev string, prof media.Profile, ok bool) {
	d, p, found := strings.Cut(key, "|")
	if !found || d == "" {
		return "", "", false
	}
	if mp := media.Profile(p); mp == media.ProfileMain || mp == media.ProfileSub {
		return d, mp, true
	}
	return "", "", false
}

// --- gortsplib handlers ---

func (s *Server) OnConnOpen(*gortsplib.ServerHandlerOnConnOpenCtx) {}

func (s *Server) OnConnClose(*gortsplib.ServerHandlerOnConnCloseCtx) {}

func (s *Server) OnSessionOpen(*gortsplib.ServerHandlerOnSessionOpenCtx) {}

// OnSessionClose 是观众会话的统一收尾（TEARDOWN/断开/超时/被动关闭都
// 走这里）：删会话记账；对每个持有键做 Release+viewers--，viewers 归
// 零才拆共享流条目（多观众互不拆台，修「任一观众断开杀死全体流」）。
func (s *Server) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	s.mu.Lock()
	rec, ok := s.sessions[ctx.Session]
	delete(s.sessions, ctx.Session)
	if ok {
		for key := range rec.keys {
			if e, exists := s.streams[key]; exists {
				if e.viewers > 0 {
					e.viewers--
				}
				if e.viewers == 0 {
					if dev, prof, parsed := splitKey(key); parsed {
						s.closeEntryLocked(key, dev, prof)
					}
				}
			}
		}
	}
	s.mu.Unlock()
	if !ok {
		return
	}
	// hook 下行计数以 played 为准（SETUP 未 PLAY 的会话不计观众）
	if rec.played && s.hook != nil {
		s.hook("rtsp", false)
	}
	// 释放该会话在 SETUP 时取得的源引用（每键恰一次，锁外调 Manager）
	for key := range rec.keys {
		if dev, prof, parsed := splitKey(key); parsed {
			s.mgr.Release(dev, prof)
		}
	}
}

// defaultDescribeWait 是 DescribeWait 未配置时的默认等待（p2p 呼叫建立
// 实测 ~5-8s，留余量）。
const defaultDescribeWait = 10 * time.Second

// waitStreamReady 等待 (device, profile) 源的描述就绪（DESCRIBE/SETUP
// 早到竞态修复）：仅当 Manager 已有该槽位（取流已发起）时轮询等待，
// 返回 true 表示描述已就绪。无槽位（Acquire 从未发生）立即返回 false
// ——不空等陌生设备。
func (s *Server) waitStreamReady(dev string, prof media.Profile) bool {
	wait := s.DescribeWait
	if wait <= 0 {
		wait = defaultDescribeWait
	}
	if !s.mgr.HasSlot(dev, prof) {
		return false
	}
	deadline := time.Now().Add(wait)
	for time.Now().Before(deadline) {
		time.Sleep(200 * time.Millisecond)
		if s.mgr.Desc(dev, prof) != nil {
			return true
		}
	}
	return false
}

// maybePrewarm 在认证 DESCRIBE 命中 404 分支时按每设备冷却触发预热
// （p2p-av 下即真实 P2P 呼叫）。冷却窗外的首次/重复触发放行，窗内
// 轮询不重复拆建链。默认 30s。
func (s *Server) maybePrewarm(dev string) {
	cd := s.prewarmCooldown
	if cd <= 0 {
		cd = 30 * time.Second
	}
	s.mu.Lock()
	last, seen := s.prewarmedAt[dev]
	now := time.Now()
	if seen && now.Sub(last) < cd {
		s.mu.Unlock()
		return
	}
	s.prewarmedAt[dev] = now
	s.mu.Unlock()
	s.mgr.Prewarm(dev)
}

func (s *Server) OnDescribe(ctx *gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	dev, prof, ok := parsePath(ctx.Path)
	if !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, fmt.Errorf("bad path")
	}
	if !s.authorize(ctx.Request) {
		return s.challenge(&base.Response{}), nil, nil
	}
	// 键统一归一后再查/建流（与 SETUP/记账/writer 注册同键）
	prof = s.mgr.NormProfile(prof)
	e, err := s.streamFor(dev, prof)
	if err != nil {
		// lazy source: 认证 DESCRIBE = 观看意图，冷却窗内触发预热呼叫；
		// NVR-style clients 重试 DESCRIBE/SETUP。无凭据请求已在 authorize
		// 被 401 拦截，不会触发 p2p 呼叫（不烧节流窗）。
		s.maybePrewarm(dev)
		// 早到竞态：源已发起但描述未就绪（p2p 呼叫建立 ~5-8s）——有槽位
		// 则最多 DescribeWait 内等描述合成，超时仍 404。
		if s.waitStreamReady(dev, prof) {
			e, err = s.streamFor(dev, prof)
		}
	}
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, e.ss, nil
}

func (s *Server) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	dev, prof, ok := parsePath(ctx.Path)
	if !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, fmt.Errorf("bad path")
	}
	if !s.authorize(ctx.Request) {
		return s.challenge(&base.Response{}), nil, nil
	}
	// 键统一归一（修 writer 覆盖互踩）：streams/sessions 记账与
	// RegisterWriter/UnregisterWriter 全部使用归一化 profile。
	prof = s.mgr.NormProfile(prof)
	key := dev + "|" + string(prof)

	// ① 同会话多 track 去重：该键已记账 → 直接复用既有流（不重复
	// Acquire，修「每 track 一次 Acquire 只配一次 Release」的引用泄漏）
	s.mu.Lock()
	if rec, held := s.sessions[ctx.Session]; held && rec.keys[key] {
		e, exists := s.streams[key]
		s.mu.Unlock()
		if exists {
			return &base.Response{StatusCode: base.StatusOK}, e.ss, nil
		}
		// 已记账但条目已被源停拆（防御路径）：源引用仍由会话记账持
		// 有，这里不重建条目——让客户端按 NVR 语义重试。
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
	}
	s.mu.Unlock()

	// ② 取源引用（SETUP 即记账；设备未知 → 404）
	if _, err := s.mgr.Acquire(dev, prof); err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	// ③ 建流（查-建同临界区）；早到竞态：Acquire 已建槽位，等待描述
	// 就绪后重试（同 DESCRIBE 语义）
	e, err := s.streamFor(dev, prof)
	if err != nil {
		if s.waitStreamReady(dev, prof) {
			e, err = s.streamFor(dev, prof)
		}
	}
	if err != nil {
		// 回滚 ② 取得的源引用（Acquire/Release 严格配对）
		s.mgr.Release(dev, prof)
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
	}
	// ④ 记账：会话键集合 + viewers++。校验条目同一性——③④ 之间条目
	// 被源停拆（OnSourceState）时不把引用记到陈旧条目上。
	s.mu.Lock()
	if cur, exists := s.streams[key]; exists && cur == e {
		rec := s.sessions[ctx.Session]
		if rec == nil {
			rec = &sessionRec{keys: map[string]bool{}}
			s.sessions[ctx.Session] = rec
		}
		rec.keys[key] = true
		e.viewers++
		s.mu.Unlock()
		return &base.Response{StatusCode: base.StatusOK}, e.ss, nil
	}
	s.mu.Unlock()
	// 条目在 ③④ 之间失效：回滚引用，503
	s.mgr.Release(dev, prof)
	return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
}

func (s *Server) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	if _, _, ok := parsePath(ctx.Path); !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil
	}
	if !s.authorize(ctx.Request) {
		return s.challenge(&base.Response{}), nil
	}
	s.mu.Lock()
	rec, seen := s.sessions[ctx.Session]
	first := seen && !rec.played
	if first {
		rec.played = true
	}
	s.mu.Unlock()
	if first && s.hook != nil {
		s.hook("rtsp", true)
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// OnSourceState 订阅媒体源状态变迁（app 经 Mgr.OnState 接线）：仅处理
// StateStopped——使该设备该归一化 profile 的出口流条目失效。幂等；
// 覆盖两类陈旧：DESCRIBE-only 的孤儿条目（viewers=0 无人负责拆除）
// 与源重建后缓存的旧 SDP 条目。条目关闭会异步联动关闭其 reader
// 会话，各自走 OnSessionClose 释放源引用。
func (s *Server) OnSourceState(ss media.SourceState) {
	if ss.State != media.StateStopped {
		return
	}
	prof := s.mgr.NormProfile(ss.Profile)
	key := ss.Device + "|" + string(prof)
	s.mu.Lock()
	s.closeEntryLocked(key, ss.Device, prof)
	s.mu.Unlock()
}

// LoopbackURL builds the internal loopback source URL for ffmpeg
// pipelines (snapshot/HLS/MJPEG) so they traverse the same egress path.
func (s *Server) LoopbackURL(dev string, prof media.Profile, user, pass string) string {
	_, port, _ := net.SplitHostPort(s.srv.RTSPAddress)
	if port == "" {
		port = "8554"
	}
	return fmt.Sprintf("rtsp://%s:%s@127.0.0.1:%s/konx/%s/%s", user, pass, port, dev, prof)
}

func parseBasic(b64 string) (string, string, bool) {
	// minimal base64 decode
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var vals []byte
	var buf, bits int
	for _, c := range b64 {
		if c == '=' {
			break
		}
		idx := -1
		for i, tc := range tbl {
			if tc == c {
				idx = i
				break
			}
		}
		if idx < 0 {
			continue
		}
		buf = buf<<6 | idx
		bits += 6
		if bits >= 8 {
			bits -= 8
			vals = append(vals, byte(buf>>bits))
		}
	}
	cred := string(vals)
	if i := strings.Index(cred, ":"); i > 0 {
		return cred[:i], cred[i+1:], true
	}
	return "", "", false
}
