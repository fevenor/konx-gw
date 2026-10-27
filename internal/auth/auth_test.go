package auth

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"testing"
	"time"

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

// TestSessionTokenTTL（C3-1）：登录会话令牌带 TTL、过期即惰性吊销；
// 长期 API 令牌（CreateToken）不受影响。
func TestSessionTokenTTL(t *testing.T) {
	svc := newTestService(t)

	// 长期令牌：无 TTL，恒有效
	perm, err := svc.CreateToken("ha-integration", []string{ScopeRead})
	if err != nil {
		t.Fatalf("create token: %v", err)
	}

	// 会话令牌：TTL 内有效，且是 admin 权限
	sess, err := svc.CreateSessionToken(time.Hour)
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}
	scopes, ok := svc.CheckToken(sess.Secret)
	if !ok {
		t.Fatal("TTL 内会话令牌应有效")
	}
	if len(scopes) != 1 || scopes[0] != ScopeAdmin {
		t.Fatalf("会话令牌权限 = %v，期望 [admin]", scopes)
	}

	// 把到期时间改写到过去（直接操纵 KV 记录模拟时间流逝）
	if err := svc.store.KVSet(tokenExpKVPrefix+sess.ID, "1"); err != nil {
		t.Fatalf("backdate expiry: %v", err)
	}
	if _, ok := svc.CheckToken(sess.Secret); ok {
		t.Fatal("过期会话令牌应被拒绝")
	}
	// 惰性淘汰：过期校验后令牌应从表中删除
	toks, err := svc.ListTokens()
	if err != nil {
		t.Fatalf("list tokens: %v", err)
	}
	for _, tk := range toks {
		if tk["id"] == sess.ID {
			t.Fatal("过期会话令牌未被惰性淘汰，仍留在令牌表中")
		}
	}

	// 长期令牌不受会话淘汰机制影响
	if _, ok := svc.CheckToken(perm.Secret); !ok {
		t.Fatal("长期 API 令牌不应受 TTL 影响")
	}
}

// TestSessionTokenTTLDefault：ttl<=0 时回落默认 24h。
func TestSessionTokenTTLDefault(t *testing.T) {
	svc := newTestService(t)
	sess, err := svc.CreateSessionToken(0)
	if err != nil {
		t.Fatalf("create session token: %v", err)
	}
	v, err := svc.store.KVGet(tokenExpKVPrefix + sess.ID)
	if err != nil {
		t.Fatalf("kv get: %v", err)
	}
	exp, _ := strconv.ParseInt(v, 10, 64)
	got := time.Until(time.Unix(exp, 0))
	if got < 23*time.Hour || got > 25*time.Hour {
		t.Fatalf("默认 TTL 应约 24h，得到 %v", got)
	}
}
