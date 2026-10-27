package api

// 本批（api/auth/config 层修复）新增行为的单元测试：
//   - C3-4：/metrics 鉴权默认开启、可配置关闭；事件指标为 O(1) 总量；
//   - C2-4：设备在线由媒体源槽位状态推导；
//   - C1-2：hls-start 幂等（不重复 Acquire），hls-stop 归还引用；
//   - M5：talk REST 会话不随 HTTP 响应消亡；
//   - M6：快照冷却缓存命中/过期；
//   - M8：WAV 头按 RIFF/WAVE chunk 实际长度剥离。

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
	"github.com/fevenor/konx-gw/internal/media"
	"github.com/fevenor/konx-gw/internal/rtspout"
	"github.com/fevenor/konx-gw/internal/talk"
)

// newAPITestEnv 构造带真实 auth/store 的最小服务端（不接线媒体/P2P）。
func newAPITestEnv(t *testing.T, cfg *config.App) (*Server, *httptest.Server, *events.Store, string) {
	t.Helper()
	st, err := events.Open(filepath.Join(t.TempDir(), "api-test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	svc, err := auth.New(st, slog.Default())
	if err != nil {
		t.Fatalf("new auth: %v", err)
	}
	tok, err := svc.CreateToken("api-test", []string{auth.ScopeRead, auth.ScopeTalk})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	s := New(Deps{Cfg: cfg, Auth: svc, Store: st, Log: slog.Default()})
	srv := httptest.NewServer(s.Handler())
	t.Cleanup(srv.Close)
	return s, srv, st, tok.Secret
}

func doGet(t *testing.T, url, token string) (*http.Response, string) {
	t.Helper()
	req, _ := http.NewRequest("GET", url, nil)
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("get %s: %v", url, err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	return resp, string(b)
}

// TestMetricsAuthDefault：默认（未配置 metrics_auth）时 /metrics 必须
// 鉴权——匿名 401，read 令牌 200。
func TestMetricsAuthDefault(t *testing.T) {
	_, srv, _, tok := newAPITestEnv(t, &config.App{})
	resp, body := doGet(t, srv.URL+"/metrics", "")
	if resp.StatusCode != 401 {
		t.Fatalf("匿名抓取 /metrics 期望 401 得到 %d：%.200s", resp.StatusCode, body)
	}
	resp, body = doGet(t, srv.URL+"/metrics", tok)
	if resp.StatusCode != 200 || !strings.Contains(body, "konx_p2p_up") {
		t.Fatalf("带令牌抓取 /metrics 失败: %d %.200s", resp.StatusCode, body)
	}
}

// TestMetricsAuthDisabled：metrics_auth: false（可信内网抓取）时匿名可读。
func TestMetricsAuthDisabled(t *testing.T) {
	off := false
	st, err := events.Open(filepath.Join(t.TempDir(), "m.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := auth.New(st, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	cfg := &config.App{}
	cfg.Web.MetricsAuth = &off
	s := New(Deps{Cfg: cfg, Auth: svc, Store: st, Log: slog.Default()})
	hs := httptest.NewServer(s.Handler())
	defer hs.Close()
	resp, body := doGet(t, hs.URL+"/metrics", "")
	if resp.StatusCode != 200 || !strings.Contains(body, "konx_p2p_up") {
		t.Fatalf("关闭鉴权后匿名抓取失败: %d %.200s", resp.StatusCode, body)
	}
}

// TestMetricsEventsTotal：事件指标为存量总量（COUNT(*)），而非"最近
// 1000 条窗口条数"。
func TestMetricsEventsTotal(t *testing.T) {
	_, srv, st, tok := newAPITestEnv(t, &config.App{})
	const n = 5
	for i := 0; i < n; i++ {
		if err := st.Insert(&events.Event{DeviceID: "cloud", Type: "t", TSRecv: time.Now().Unix(), Payload: "{}"}); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	_, body := doGet(t, srv.URL+"/metrics", tok)
	if !strings.Contains(body, fmt.Sprintf("konx_events_total %d", n)) {
		t.Fatalf("konx_events_total 期望 %d：%.300s", n, body)
	}
	if strings.Contains(body, "konx_events_recent") {
		t.Fatalf("旧指标名 konx_events_recent 仍存在：%.300s", body)
	}
}

// TestOnlineFromStates：在线推导取值语义（C2-4）。
func TestOnlineFromStates(t *testing.T) {
	cases := []struct {
		name   string
		states map[media.Profile]media.State
		want   bool
	}{
		{"无槽位", map[media.Profile]media.State{media.ProfileMain: media.StateStopped, media.ProfileSub: media.StateStopped}, false},
		{"建立中", map[media.Profile]media.State{media.ProfileMain: media.StateStarting}, true},
		{"活跃", map[media.Profile]media.State{media.ProfileMain: media.StateActive}, true},
		{"会话保持", map[media.Profile]media.State{media.ProfileMain: media.StatePaused}, true},
		{"源错误", map[media.Profile]media.State{media.ProfileMain: media.StateError}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := onlineFromStates(tc.states); got != tc.want {
				t.Fatalf("%s: 期望 %v 得到 %v", tc.name, tc.want, got)
			}
		})
	}
}

// TestStripWAVHeader：按 chunk 实际长度剥离 WAV 头（M8）。
func TestStripWAVHeader(t *testing.T) {
	chunk := func(id string, size int, body []byte) []byte {
		h := make([]byte, 8)
		copy(h, id)
		h[4] = byte(size)
		h[5] = byte(size >> 8)
		h[6] = byte(size >> 16)
		h[7] = byte(size >> 24)
		return append(h, body...)
	}
	riff := func(chunks ...[]byte) []byte {
		var body []byte
		for _, c := range chunks {
			body = append(body, c...)
		}
		out := []byte("RIFF\x00\x00\x00\x00WAVE")
		return append(out, body...)
	}
	pcm := []byte{0x01, 0x02, 0x03, 0x04}

	t.Run("标准 44 字节头", func(t *testing.T) {
		wav := riff(chunk("fmt ", 16, make([]byte, 16)), chunk("data", len(pcm), pcm))
		if got := stripWAVHeader(wav); string(got) != string(pcm) {
			t.Fatalf("剥离结果 = %v 期望 %v", got, pcm)
		}
	})
	t.Run("带 LIST 附加块的非标准头", func(t *testing.T) {
		wav := riff(chunk("fmt ", 16, make([]byte, 16)),
			chunk("LIST", 10, make([]byte, 10)),
			chunk("data", len(pcm), pcm))
		if got := stripWAVHeader(wav); string(got) != string(pcm) {
			t.Fatalf("非标准头剥离结果 = %v 期望 %v", got, pcm)
		}
	})
	t.Run("data 长度越界取剩余全部", func(t *testing.T) {
		wav := riff(chunk("fmt ", 16, make([]byte, 16)), chunk("data", 0xFFFF, pcm))
		if got := stripWAVHeader(wav); string(got) != string(pcm) {
			t.Fatalf("越界 data 剥离结果 = %v 期望 %v", got, pcm)
		}
	})
	t.Run("裸 PCM 原样透传", func(t *testing.T) {
		if got := stripWAVHeader(pcm); string(got) != string(pcm) {
			t.Fatalf("裸 PCM 被改动: %v", got)
		}
	})
	t.Run("截断的 RIFF 原样透传", func(t *testing.T) {
		wav := riff(chunk("fmt ", 16, make([]byte, 4))) // fmt 声明 16 实给 4
		if got := stripWAVHeader(wav); string(got) != string(wav) {
			t.Fatalf("损坏 WAV 应原样透传: %v", got)
		}
	})
}

// TestSnapshotCooldownCache：冷却窗内命中缓存、过期后放行（M6）。
func TestSnapshotCooldownCache(t *testing.T) {
	s, _, _, _ := newAPITestEnv(t, &config.App{})
	key := "10000001|main"
	s.snapshotCachePut(key, snapResult{jpg: []byte("JPEG"), at: time.Now()})
	res, ok := s.snapshotCacheGet(key)
	if !ok || string(res.jpg) != "JPEG" {
		t.Fatalf("冷却窗内应命中缓存: ok=%v res=%v", ok, res)
	}
	// 回放失败结果：错误也应命中（冷却窗内不重试完整呼叫）
	s.snapshotCachePut(key, snapResult{err: context.Canceled, at: time.Now()})
	if res, ok := s.snapshotCacheGet(key); !ok || res.err == nil {
		t.Fatalf("冷却窗内的失败结果应命中: ok=%v res=%v", ok, res)
	}
	// 过期后必须放行真实抓取
	s.snapshotCachePut(key, snapResult{jpg: []byte("OLD"), at: time.Now().Add(-snapshotCooldown - time.Second)})
	if _, ok := s.snapshotCacheGet(key); ok {
		t.Fatal("过期缓存不应命中")
	}
}

// fakeTalkSink 是可观测的测试对讲端点。
type fakeTalkSink struct {
	mu      sync.Mutex
	started int
	written int
	stopped int
}

func (f *fakeTalkSink) Start(context.Context, string, string) error {
	f.mu.Lock()
	f.started++
	f.mu.Unlock()
	return nil
}
func (f *fakeTalkSink) Write(b []byte) error {
	f.mu.Lock()
	f.written += len(b)
	f.mu.Unlock()
	return nil
}
func (f *fakeTalkSink) Stop() error {
	f.mu.Lock()
	f.stopped++
	f.mu.Unlock()
	return nil
}

// TestTalkStartSurvivesRequestScope：REST 触发的 talk 会话不能随 HTTP
// 响应返回而消亡（M5）——响应之后会话仍存活，直到显式 Stop。
func TestTalkStartSurvivesRequestScope(t *testing.T) {
	tm := talk.NewManager()
	sink := &fakeTalkSink{}
	tm.SetSinkFactory(func(string) talk.Sink { return sink })

	st, err := events.Open(filepath.Join(t.TempDir(), "talk.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := auth.New(st, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	tok, err := svc.CreateToken("talk", []string{auth.ScopeTalk})
	if err != nil {
		t.Fatal(err)
	}
	s := New(Deps{Cfg: &config.App{}, Auth: svc, Store: st, Talk: tm, Log: slog.Default()})
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	req, _ := http.NewRequest("POST", srv.URL+"/api/devices/10000001/talk/start", nil)
	req.Header.Set("Authorization", "Bearer "+tok.Secret)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("talk/start 期望 200 得到 %d: %s", resp.StatusCode, b)
	}
	// 此时 handler 已返回：若会话仍绑请求 ctx，这里 Active 必为 nil
	if tm.Active("10000001") == nil {
		t.Fatal("talk 会话随 HTTP 响应消亡（仍绑定了请求 ctx）")
	}
	tm.Stop("10000001")
	if tm.Active("10000001") != nil {
		t.Fatal("显式 Stop 后会话应清除")
	}
}

// TestHLSStartIdempotentAndStop：重复 hls-start 不重复 Acquire（引用
// 记账唯一），hls-stop 归还引用（C1-2）。
func TestHLSStartIdempotentAndStop(t *testing.T) {
	cfg := &config.App{}
	cfg.Media.Source = "external-rtsp"
	cfg.Media.ExternalURL = "rtsp://127.0.0.1:1/none" // 不可达：仅验证记账
	cfg.Devices = []config.Device{{ID: "10000001"}}

	st, err := events.Open(filepath.Join(t.TempDir(), "hls.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	svc, err := auth.New(st, slog.Default())
	if err != nil {
		t.Fatal(err)
	}
	tok, err := svc.CreateToken("hls", []string{auth.ScopeRead})
	if err != nil {
		t.Fatal(err)
	}
	mgr := media.NewManager(cfg, slog.Default(), nil)
	rt := rtspout.New(":8554", mgr, svc, nil, slog.Default())
	ff := &media.FFmpeg{Path: "/bin/true"} // 只验证管线记账，不做真实转码
	hls := media.NewHLSManager(ff, t.TempDir())

	s := New(Deps{Cfg: cfg, Auth: svc, Store: st, Mgr: mgr, Rtsp: rt, HLS: hls, FF: ff, Log: slog.Default()})
	srv := httptest.NewServer(s.Handler())
	defer srv.Close()

	post := func(path string) int {
		req, _ := http.NewRequest("POST", srv.URL+path, nil)
		req.Header.Set("Authorization", "Bearer "+tok.Secret)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post %s: %v", path, err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode
	}

	if c := post("/api/devices/10000001/hls-start"); c != 200 {
		t.Fatalf("首次 hls-start 期望 200 得到 %d", c)
	}
	if c := post("/api/devices/10000001/hls-start"); c != 200 {
		t.Fatalf("重复 hls-start 应幂等 200 得到 %d", c)
	}
	s.hlsMu.Lock()
	refs := len(s.hlsRefs)
	s.hlsMu.Unlock()
	if refs != 1 {
		t.Fatalf("重复 hls-start 后源引用记账 = %d，期望恒为 1（旧实现每次调用泄漏一个引用）", refs)
	}
	if c := post("/api/devices/10000001/hls-stop"); c != 200 {
		t.Fatalf("hls-stop 期望 200 得到 %d", c)
	}
	s.hlsMu.Lock()
	refs = len(s.hlsRefs)
	s.hlsMu.Unlock()
	if refs != 0 {
		t.Fatalf("hls-stop 后源引用记账 = %d，期望 0", refs)
	}
}
