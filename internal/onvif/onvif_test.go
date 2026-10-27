package onvif

import (
	"net/http"
	"strings"
	"testing"
)

// TestSOAPBodySizeLimit：D6 回归防护——超限请求体（>1MB）必须被拒
// 绝（413），不得无上限读入内存。
func TestSOAPBodySizeLimit(t *testing.T) {
	s := newDeviceTestServer(t)

	big := "<?xml version=\"1.0\"?>" + strings.Repeat("<!--", 300*1024) + "x"
	if len(big) <= 1<<20 {
		t.Fatalf("测试体应大于 1MB, 实际 %d 字节", len(big))
	}
	rec := postSOAP(t, s, "/onvif/device_service", "GetCapabilities", big)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("超限请求体: 状态码 = %d, 期望 413; body: %.200s", rec.Code, rec.Body.String())
	}

	// 上限内的正常请求不受影响
	rec = postSOAP(t, s, "/onvif/device_service", "GetCapabilities",
		`<s:Body><tds:GetCapabilities Category="All"/></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Errorf("正常大小请求: 状态码 = %d, 期望 200", rec.Code)
	}
}
