package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSnapshotMediaCredentialsRemainScoped(t *testing.T) {
	svc := newTestService(t)
	defer svc.store.Close()
	svc.SetUser("nvr", "demo")
	tok, err := svc.CreateToken("测试", []string{ScopeRead})
	if err != nil {
		t.Fatal(err)
	}
	ok := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }
	for _, tc := range []struct {
		name, pass, token string
		code              int
	}{
		{"合法媒体凭据", "demo", "", 204},
		{"错误媒体凭据", "wrong", "", 401},
		{"匿名请求", "", "", 401},
		{"原有令牌", "", tok.Secret, 204},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", "/api/devices/10000001/snapshot", nil)
			if tc.pass != "" {
				r.SetBasicAuth("nvr", tc.pass)
			}
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			svc.RequireSnapshot(ok)(w, r)
			if w.Code != tc.code {
				t.Fatalf("状态码=%d，期望=%d", w.Code, tc.code)
			}
			if tc.code == 401 && w.Header().Get("WWW-Authenticate") == "" {
				t.Fatal("缺少 Basic 鉴权提示")
			}
		})
	}
	r := httptest.NewRequest("GET", "/api/tokens", nil)
	r.SetBasicAuth("nvr", "demo")
	w := httptest.NewRecorder()
	svc.Require(ScopeAdmin, ok)(w, r)
	if w.Code != 401 {
		t.Fatal("媒体凭据不应获得管理 API 权限")
	}
}
