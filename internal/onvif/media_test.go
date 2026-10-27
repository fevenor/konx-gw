package onvif

import (
	"net/http"
	"strings"
	"testing"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/media"
)

// newMediaTestServer 构造双设备测试服务：StreamUri 按设备/档位生成，
// 用于验证多设备环境下不发生静默回退。
func newMediaTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Defaults()
	cfg.ONVIF.Listen = "127.0.0.1:8000"
	cfg.Advertise.Host = "127.0.0.1"
	cfg.Devices = []config.Device{{ID: "dev-a"}, {ID: "dev-b"}}
	return New(Env{
		Cfg: cfg,
		StreamURL: func(d string, p media.Profile) string {
			return "rtsp://127.0.0.1:8554/konx/" + d + "/" + string(p)
		},
		SnapshotURL: func(d string, p media.Profile) string {
			return "http://127.0.0.1:8000/snapshot/" + d + "/" + string(p)
		},
	})
}

// TestGetStreamUriElementProfileToken：M1 回归防护——元素式
// ProfileToken（规范形态，含命名空间前缀或缺省）必须被解析到指定
// 设备，多设备下不得静默回退第一台。
func TestGetStreamUriElementProfileToken(t *testing.T) {
	s := newMediaTestServer(t)

	reqs := []struct {
		name string
		body string
		want string
	}{
		{
			"元素式无前缀",
			`<s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"><ProfileToken>profile_dev-b_main</ProfileToken></GetStreamUri></s:Body>`,
			"/konx/dev-b/main",
		},
		{
			"元素式带前缀",
			`<s:Body><trt:GetStreamUri xmlns:trt="http://www.onvif.org/ver10/media/wsdl"><trt:ProfileToken>profile_dev-b_sub</trt:ProfileToken></trt:GetStreamUri></s:Body>`,
			"/konx/dev-b/sub",
		},
		{
			"属性式（旧客户端兼容）",
			`<s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl" ProfileToken="profile_dev-b_main"/></s:Body>`,
			"/konx/dev-b/main",
		},
	}
	for _, rq := range reqs {
		rec := postSOAP(t, s, "/onvif/media_service", "GetStreamUri", rq.body)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: 状态码 = %d, 期望 200; body: %.300s", rq.name, rec.Code, rec.Body.String())
			continue
		}
		if !strings.Contains(rec.Body.String(), rq.want) {
			t.Errorf("%s: 应答应指向 %s（不得回退 dev-a）, body: %.300s", rq.name, rq.want, rec.Body.String())
		}
	}
}

// TestGetStreamUriFaults：token 缺失或无法解析时返回明确错误，而非
// 静默回退第一台设备返回错误设备的流。
func TestGetStreamUriFaults(t *testing.T) {
	s := newMediaTestServer(t)

	cases := []struct {
		name string
		body string
		want string
	}{
		{"缺失 token", `<s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"/></s:Body>`, "missing profile token"},
		{"垃圾 token", `<s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"><ProfileToken>bogus_token</ProfileToken></GetStreamUri></s:Body>`, "unknown profile token"},
		{"空 token 元素", `<s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"><ProfileToken></ProfileToken></GetStreamUri></s:Body>`, "missing profile token"},
	}
	for _, c := range cases {
		rec := postSOAP(t, s, "/onvif/media_service", "GetStreamUri", c.body)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("%s: 状态码 = %d, 期望 400; body: %.300s", c.name, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), c.want) {
			t.Errorf("%s: 应包含 %q; body: %.300s", c.name, c.want, rec.Body.String())
		}
	}
}

// TestGetSnapshotUriElementToken：GetSnapshotUri 与 GetStreamUri 同路径
// 修复。
func TestGetSnapshotUriElementToken(t *testing.T) {
	s := newMediaTestServer(t)
	rec := postSOAP(t, s, "/onvif/media_service", "GetSnapshotUri",
		`<s:Body><GetSnapshotUri xmlns="http://www.onvif.org/ver10/media/wsdl"><ProfileToken>profile_dev-b_main</ProfileToken></GetSnapshotUri></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200; body: %.300s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "/snapshot/dev-b/main") {
		t.Errorf("快照地址应指向 dev-b, body: %.300s", rec.Body.String())
	}
}

// TestGetVideoEncoderConfigurationsFallback：该操作规范上不带
// ProfileToken，保留回退第一台配置设备的行为。
func TestGetVideoEncoderConfigurationsFallback(t *testing.T) {
	s := newMediaTestServer(t)
	rec := postSOAP(t, s, "/onvif/media_service", "GetVideoEncoderConfigurations",
		`<s:Body><GetVideoEncoderConfigurations xmlns="http://www.onvif.org/ver10/media/wsdl"/></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "dev-a-main") {
		t.Errorf("无 token 时应回退第一台设备 dev-a, body: %.300s", rec.Body.String())
	}
}
