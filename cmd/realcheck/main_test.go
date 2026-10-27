package main

import (
	"fmt"
	"strings"
	"testing"
)

// D13 回归守护：ONVIF 探测请求必须由真实 CRLF 组成请求行/头，
// 不得再出现字面量 "\r\n" 四字符序列（原始字符串反引号写法的历史缺陷）。
func TestOnvifProbeRequestCRLF(t *testing.T) {
	body := `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetDeviceInformation xmlns="http://www.onvif.org/ver10/device/wsdl"/></s:Body></s:Envelope>`
	req := onvifProbeRequest(body)

	// 1) 请求行以真实 CRLF 结尾
	if !strings.HasPrefix(req, "POST /onvif/device_service HTTP/1.1\r\n") {
		t.Fatalf("请求行缺少真实 CRLF，前 60 字节: %q", firstN(req, 60))
	}
	// 2) 全文不得含字面量反斜杠 r/n 序列
	if strings.Contains(req, `\r\n`) {
		t.Fatalf("请求中含有字面量 \\r\\n 序列（原始字符串缺陷未修复），前 60 字节: %q", firstN(req, 60))
	}
	// 3) 各头部均以 CRLF 分隔，且头部与正文之间为空行 CRLFCRLF
	if !strings.Contains(req, "Content-Type: application/soap+xml\r\n") {
		t.Fatalf("Content-Type 头未以 CRLF 结尾")
	}
	if !strings.Contains(req, fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)) {
		t.Fatalf("Content-Length 或头部/正文空行分隔不正确")
	}
}
