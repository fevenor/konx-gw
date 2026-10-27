// realcheck: one-shot live verification of the Go protocol port against
// the real list service (no credentials required) and the device ONVIF
// endpoint (single gentle probe). Not part of the gateway runtime.
package main

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/list"
)

func main() {
	// 1. real list service (protocol §2.2)
	term := uint64(2204264224)
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	for _, srv := range []string{"p2p1.cloudlinks.cn", "p2p5.cloudlinks.cn"} {
		ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
		servers, err := list.QueryUDPWithTerm(ctx, conn, srv, term)
		cancel()
		if err != nil {
			fmt.Printf("LIST %-22s FAIL: %v\n", srv, err)
			continue
		}
		fmt.Printf("LIST %-22s OK:", srv)
		for _, s := range servers {
			fmt.Printf(" %s:%d", s.IP, s.Port)
		}
		fmt.Println()
		break
	}
	// 2. device ONVIF: single gentle probe (F10 rules)
	d := net.Dialer{Timeout: 3 * time.Second}
	c, err := d.Dial("tcp", "192.168.1.100:5000")
	if err != nil {
		fmt.Println("DEVICE 192.168.1.100:5000 unreachable:", err)
		return
	}
	defer c.Close()
	body := `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetDeviceInformation xmlns="http://www.onvif.org/ver10/device/wsdl"/></s:Body></s:Envelope>`
	c.SetDeadline(time.Now().Add(4 * time.Second))
	// 注意：请求模板必须用解释字符串（双引号），\r\n 才是真实 CRLF；
	// 反引号原始字符串会把 \r\n 变成字面量四个字符，导致请求行/头全部畸形。
	fmt.Fprint(c, onvifProbeRequest(body))
	buf := make([]byte, 4096)
	n, _ := c.Read(buf)
	resp := string(buf[:n])
	if contains(resp, "Technology") {
		fmt.Println("DEVICE ONVIF OK: Technology IPC")
	} else {
		fmt.Println("DEVICE ONVIF resp head:", firstN(resp, 120))
	}
}

// onvifProbeRequest 构造完整的 ONVIF GetDeviceInformation 探测请求。
// 模板使用双引号解释字符串，保证 \r\n 为真实 CRLF（D13 修复点，有测试守护）。
func onvifProbeRequest(body string) string {
	return fmt.Sprintf("POST /onvif/device_service HTTP/1.1\r\nHost: 192.168.1.100:5000\r\nContent-Type: application/soap+xml\r\nContent-Length: %d\r\n\r\n%s", len(body), body)
}

func contains(s, sub string) bool { return len(s) >= len(sub) && indexOf(s, sub) >= 0 }
func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
func firstN(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
