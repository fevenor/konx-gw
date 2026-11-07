package onvif

import (
	"net/http/httptest"
	"strings"
	"testing"
)

// 使用完整 SOAPAction URI 时，也应返回可供客户端拉流的地址。
func TestRegressionSOAPActionURI(t *testing.T) {
	s := newMediaTestServer(t)
	body := `<s:Body><trt:GetStreamUri xmlns:trt="http://www.onvif.org/ver10/media/wsdl"><trt:ProfileToken>profile_dev-b_main</trt:ProfileToken></trt:GetStreamUri></s:Body>`
	r := httptest.NewRequest("POST", "/onvif/media_service", strings.NewReader(body))
	r.Header.Set("SOAPAction", `"http://www.onvif.org/ver10/media/wsdl/GetStreamUri"`)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), "/konx/dev-b/main") {
		t.Fatalf("完整 SOAPAction 未返回流地址：%s", w.Body.String())
	}
}

// 查询单个配置也必须返回 SOAP Body 与 GetProfileResponse 包装。
func TestRegressionGetProfileEnvelope(t *testing.T) {
	s := newMediaTestServer(t)
	r := httptest.NewRequest("POST", "/onvif/media_service", strings.NewReader(`<s:Body><GetProfile><ProfileToken>profile_dev-b_main</ProfileToken></GetProfile></s:Body>`))
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), "<s:Body>") || !strings.Contains(w.Body.String(), "GetProfileResponse") {
		t.Fatalf("单配置查询缺少 SOAP 响应包装：%s", w.Body.String())
	}
}
