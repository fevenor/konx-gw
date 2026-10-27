package auth

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/fevenor/konx-gw/internal/events"
)

// newTestService 搭建带临时数据库的鉴权服务。
func newTestService(t *testing.T) *Service {
	t.Helper()
	store, err := events.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	svc, err := New(store, slog.Default())
	if err != nil {
		t.Fatalf("new auth: %v", err)
	}
	return svc
}

// TestRequireQueryToken 验证流端点的 query 令牌回退：浏览器 <img> 标签
// 无法携带 Authorization 头，stream.mjpeg 等端点依赖 tok/token 查询参数。
func TestRequireQueryToken(t *testing.T) {
	svc := newTestService(t)
	tok, err := svc.CreateToken("web-session", []string{ScopeRead})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}
	handler := svc.Require(ScopeRead, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	cases := []struct {
		name string
		url  string
		code int
	}{
		{"query tok", "/stream?tok=" + tok.Secret, 200},
		{"query token", "/stream?token=" + tok.Secret, 200},
		{"无凭据", "/stream", 401},
		{"坏令牌", "/stream?tok=badbadbad", 401},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler(rec, httptest.NewRequest("GET", tc.url, nil))
			if rec.Code != tc.code {
				t.Fatalf("%s: 期望 %d 得到 %d", tc.name, tc.code, rec.Code)
			}
		})
	}
}

// TestRequireHeaderStillWorks 验证标准 Bearer 头路径不受回退影响。
func TestRequireHeaderStillWorks(t *testing.T) {
	svc := newTestService(t)
	tok, _ := svc.CreateToken("h", []string{ScopeRead})
	handler := svc.Require(ScopeRead, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/stream", nil)
	req.Header.Set("Authorization", "Bearer "+tok.Secret)
	handler(rec, req)
	if rec.Code != 200 {
		t.Fatalf("header 路径期望 200 得到 %d", rec.Code)
	}
}
