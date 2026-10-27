// 应用身份覆盖单测（假云，不访问真云）：
//   - 默认（不配置）保持 GwellDemo 内置身份 → 向后兼容
//   - Client.AppID/AppToken 覆盖后，Login/SearchBindDev/LoadDeviceInfo
//     三个端点的请求参数全部切换到覆盖值
package cloud

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// identitySink 记录每个端点实际收到的 AppID/AppToken。
type identitySink struct {
	mu   sync.Mutex
	vals map[string]string // path|param -> value
}

func (s *identitySink) record(r *http.Request) {
	_ = r.ParseForm()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, k := range []string{"AppID", "AppToken", "AppVersion", "AppName", "PackageName"} {
		s.vals[r.URL.Path+"|"+k] = r.Form.Get(k)
	}
}

func (s *identitySink) get(path, k string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.vals[path+"|"+k]
}

// newIdentityCloud 起一个假云，三个端点全部回成功应答。
func newIdentityCloud(t *testing.T) (*Client, *identitySink) {
	t.Helper()
	sink := &identitySink{vals: map[string]string{}}
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sink.record(r)
		switch r.URL.Path {
		case "/Users/LoginCheck.ashx":
			_, _ = w.Write([]byte(`{"error_code":"0","error":"success","UserID":"1234567",` +
				`"P2PVerifyCode1":"1","P2PVerifyCode2":"2","SessionID":"3","SessionID2":"4"}`))
		case "/Relationship/LoadDeviceInfo.ashx":
			_, _ = w.Write([]byte(ldiJSON(fixtureDoorbellRec)))
		case "/Account/Bind/SearchBindDev.ashx":
			_, _ = w.Write([]byte(sbdJSON(nil)))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient(strings.TrimPrefix(srv.URL, "https://"))
	c.HTTP = srv.Client()
	return c, sink
}

// TestIdentityDefaultIsDemo：未配置覆盖时三个端点都发 demo 身份。
func TestIdentityDefaultIsDemo(t *testing.T) {
	c, sink := newIdentityCloud(t)
	if _, err := c.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := c.LoadDeviceInfo(context.Background(), testSess()); err != nil {
		t.Fatalf("LoadDeviceInfo: %v", err)
	}
	if _, err := c.SearchBindDev(context.Background(), testSess()); err != nil {
		t.Fatalf("SearchBindDev: %v", err)
	}
	for _, path := range []string{"/Users/LoginCheck.ashx", "/Relationship/LoadDeviceInfo.ashx", "/Account/Bind/SearchBindDev.ashx"} {
		if got := sink.get(path, "AppID"); got != DemoAppID {
			t.Errorf("%s AppID = %q, want demo %q", path, got, DemoAppID)
		}
		if got := sink.get(path, "AppToken"); got != DemoAppToken {
			t.Errorf("%s AppToken = %q, want demo", path, got)
		}
	}
}

// TestIdentityOverrideApplied：覆盖后三个端点全部切换到生产身份。
func TestIdentityOverrideApplied(t *testing.T) {
	const prodID = "0123456789abcdef0123456789abcdef"
	const prodToken = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	c, sink := newIdentityCloud(t)
	c.AppID = prodID
	c.AppToken = prodToken
	if _, err := c.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if _, err := c.LoadDeviceInfo(context.Background(), testSess()); err != nil {
		t.Fatalf("LoadDeviceInfo: %v", err)
	}
	if _, err := c.SearchBindDev(context.Background(), testSess()); err != nil {
		t.Fatalf("SearchBindDev: %v", err)
	}
	for _, path := range []string{"/Users/LoginCheck.ashx", "/Relationship/LoadDeviceInfo.ashx", "/Account/Bind/SearchBindDev.ashx"} {
		if got := sink.get(path, "AppID"); got != prodID {
			t.Errorf("%s AppID = %q, want override %q", path, got, prodID)
		}
		if got := sink.get(path, "AppToken"); got != prodToken {
			t.Errorf("%s AppToken = %q, want override", path, got)
		}
	}
}

// TestIdentityPartialOverride：仅覆盖 AppID 时 AppToken 仍回退 demo
// （服务器对两者各自校验，允许单独替换排查）。
func TestIdentityPartialOverride(t *testing.T) {
	c, sink := newIdentityCloud(t)
	c.AppID = "0123456789abcdef0123456789abcdef"
	if _, err := c.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got := sink.get("/Users/LoginCheck.ashx", "AppID"); got != "0123456789abcdef0123456789abcdef" {
		t.Errorf("AppID = %q, want override", got)
	}
	if got := sink.get("/Users/LoginCheck.ashx", "AppToken"); got != DemoAppToken {
		t.Errorf("AppToken = %q, want demo 回退", got)
	}
}

// TestIdentityAppVersionOverride：AppVersion 覆盖切换到自定义位压缩版本号
// （纯数字），缺省仍为 "1"。
func TestIdentityAppVersionOverride(t *testing.T) {
	c, sink := newIdentityCloud(t)
	if _, err := c.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	c2, sink2 := newIdentityCloud(t)
	c2.AppVersion = "2000000"
	if _, err := c2.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	_ = sink
	if got := sink2.vals["/Users/LoginCheck.ashx|AppVersion"]; got != "2000000" {
		t.Errorf("AppVersion = %q, want 2000000", got)
	}
}

// TestIdentityAppPackageOverride：AppPackage 覆盖切换 AppName/PackageName
// （生产 App 为自身包名），缺省仍为 demo 包。
func TestIdentityAppPackageOverride(t *testing.T) {
	c, sink := newIdentityCloud(t)
	c.AppPackage = "com.konx.example"
	if _, err := c.Login(context.Background(), Credentials{User: "u", PassMD5: "d"}); err != nil {
		t.Fatalf("Login: %v", err)
	}
	if got := sink.vals["/Users/LoginCheck.ashx|AppName"]; got != "com.konx.example" {
		t.Errorf("AppName = %q, want com.konx.example", got)
	}
	if got := sink.vals["/Users/LoginCheck.ashx|PackageName"]; got != "com.konx.example" {
		t.Errorf("PackageName = %q, want com.konx.example", got)
	}
}
