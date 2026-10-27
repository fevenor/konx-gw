// Package rtspout is the RTSP re-distribution service on :8554
// (DESIGN §5.2). Paths: /konx/{device}/{main|sub}. Downstream auth is
// HTTP Basic against the gateway credential table; loopback pipelines
// (ffmpeg snapshot/HLS/MJPEG) authenticate with the internal loopback
// identity.
package rtspout

import (
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

// Authorizer checks RTSP Basic credentials.
type Authorizer interface {
	CheckRTSP(user, pass string) bool
}

// ClientHook is called on session open/close for the client registry.
type ClientHook func(kind string, up bool)

type Server struct {
	srv     *gortsplib.Server
	mgr     *media.Manager
	auth    Authorizer
	hook    ClientHook
	log     *slog.Logger
	advPort int

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

	mu       sync.Mutex
	streams  map[string]*gortsplib.ServerStream // key device|profile
	sessions map[*gortsplib.ServerSession]string
}

func New(listen string, mgr *media.Manager, auth Authorizer, hook ClientHook, log *slog.Logger, advertisedPort int) *Server {
	s := &Server{
		mgr: mgr, auth: auth, hook: hook, log: log, advPort: advertisedPort,
		streams:     map[string]*gortsplib.ServerStream{},
		sessions:    map[*gortsplib.ServerSession]string{},
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

// stream returns (or lazily builds) the ServerStream for a slot.
func (s *Server) stream(dev string, prof media.Profile) (*gortsplib.ServerStream, error) {
	key := dev + "|" + string(prof)
	s.mu.Lock()
	if ss, ok := s.streams[key]; ok {
		s.mu.Unlock()
		return ss, nil
	}
	s.mu.Unlock()

	desc := s.mgr.Desc(dev, prof)
	if desc == nil {
		return nil, fmt.Errorf("stream not ready")
	}
	ss := &gortsplib.ServerStream{Server: s.srv, Desc: desc}
	if err := ss.Initialize(); err != nil {
		return nil, err
	}
	// route upstream packets into the server stream
	s.mgr.RegisterWriter(dev, prof, func(md *description.Media, pkt *rtp.Packet) {
		// gortsplib v4 ServerStream 在 Close 后再写会内部 panic（nil
		// deref）：观众断开→dropStream 与呼叫拆链尾包竞态属正常时序，
		// egress 是尽力而为路径——吞掉已关闭流的写入 panic。
		defer func() { _ = recover() }()
		if err := ss.WritePacketRTP(md, pkt); err != nil {
			s.log.Debug("write rtp", "err", err)
		}
	})
	s.mu.Lock()
	s.streams[key] = ss
	s.mu.Unlock()
	return ss, nil
}

func (s *Server) dropStream(dev string, prof media.Profile) {
	key := dev + "|" + string(prof)
	s.mu.Lock()
	ss, ok := s.streams[key]
	delete(s.streams, key)
	s.mu.Unlock()
	if ok {
		ss.Close()
	}
	// 对称注销 egress writer：避免呼叫拆链尾包继续喂已关闭的流
	s.mgr.UnregisterWriter(dev, prof)
}

// --- gortsplib handlers ---

func (s *Server) OnConnOpen(*gortsplib.ServerHandlerOnConnOpenCtx) {}

func (s *Server) OnConnClose(*gortsplib.ServerHandlerOnConnCloseCtx) {}

func (s *Server) OnSessionOpen(*gortsplib.ServerHandlerOnSessionOpenCtx) {}

func (s *Server) OnSessionClose(ctx *gortsplib.ServerHandlerOnSessionCloseCtx) {
	s.mu.Lock()
	id, ok := s.sessions[ctx.Session]
	delete(s.sessions, ctx.Session)
	s.mu.Unlock()
	if !ok {
		return
	}
	if s.hook != nil {
		s.hook("rtsp", false)
	}
	// release the source reference acquired at SETUP
	if v := strings.TrimPrefix(id, "rtsp:"); v != id {
		if parts := strings.SplitN(v, "/", 2); len(parts) == 2 {
			s.mgr.Release(parts[0], media.Profile(parts[1]))
			s.dropStream(parts[0], media.Profile(parts[1]))
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
	ss, err := s.stream(dev, prof)
	if err != nil {
		// lazy source: 认证 DESCRIBE = 观看意图，冷却窗内触发预热呼叫；
		// NVR-style clients 重试 DESCRIBE/SETUP。无凭据请求已在 authorize
		// 被 401 拦截，不会触发 p2p 呼叫（不烧节流窗）。
		s.maybePrewarm(dev)
		// 早到竞态：源已发起但描述未就绪（p2p 呼叫建立 ~5-8s）——有槽位
		// 则最多 DescribeWait 内等描述合成，超时仍 404。
		if s.waitStreamReady(dev, prof) {
			ss, err = s.stream(dev, prof)
		}
	}
	if err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, ss, nil
}

func (s *Server) OnSetup(ctx *gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	dev, prof, ok := parsePath(ctx.Path)
	if !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, fmt.Errorf("bad path")
	}
	if !s.authorize(ctx.Request) {
		return s.challenge(&base.Response{}), nil, nil
	}
	if _, err := s.mgr.Acquire(dev, prof); err != nil {
		return &base.Response{StatusCode: base.StatusNotFound}, nil, nil
	}
	ss, err := s.stream(dev, prof)
	if err != nil {
		// source still starting; NVR-style clients retry DESCRIBE/SETUP
		// 早到竞态：Acquire 已建槽位，等待描述就绪（同 DESCRIBE 语义）
		if s.waitStreamReady(dev, prof) {
			ss, err = s.stream(dev, prof)
		}
	}
	if err != nil {
		return &base.Response{StatusCode: base.StatusServiceUnavailable}, nil, nil
	}
	return &base.Response{StatusCode: base.StatusOK}, ss, nil
}

func (s *Server) OnPlay(ctx *gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	dev, prof, ok := parsePath(ctx.Path)
	if !ok {
		return &base.Response{StatusCode: base.StatusNotFound}, nil
	}
	if !s.authorize(ctx.Request) {
		return s.challenge(&base.Response{}), nil
	}
	s.mu.Lock()
	if _, seen := s.sessions[ctx.Session]; !seen {
		s.sessions[ctx.Session] = "rtsp:" + dev + "/" + string(prof)
	}
	s.mu.Unlock()
	if s.hook != nil {
		s.hook("rtsp", true)
	}
	return &base.Response{StatusCode: base.StatusOK}, nil
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
