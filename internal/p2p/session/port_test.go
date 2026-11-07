package session

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/cloud"
	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/list"
)

func TestDetectUsesFixedSessionPort(t *testing.T) {
	server, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() {
		buf := make([]byte, 1500)
		for {
			_, from, err := server.ReadFromUDP(buf)
			if err != nil {
				return
			}
			_, _ = server.WriteToUDP(frame.NewBuilder(1, 1, 2, 3, 4).Ack(frame.TypeDetectAck), from)
		}
	}()
	reserved, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	port := reserved.LocalAddr().(*net.UDPAddr).Port
	reserved.Close()
	s := New(Options{LocalPort: port, DetectWindow: 50 * time.Millisecond}, Events{})
	servers := []list.Server{{IP: "127.0.0.1", Port: server.LocalAddr().(*net.UDPAddr).Port}}
	cs := &cloud.Session{UID: 1, C1: 1, C2: 2, S1: 3, S2: 4}
	_, _, tr, err := s.detectAndSelect(context.Background(), cs, servers)
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	addr, err := net.ResolveUDPAddr("udp4", tr.LocalAddr())
	if err != nil || addr.Port != port {
		t.Fatalf("正式会话未使用配置端口：%v，错误 %v", addr, err)
	}
	// 固定端口被占用时必须失败，不能悄悄切到未发布的端口。
	if other, err := NewUDPTransport(port, server.LocalAddr().(*net.UDPAddr)); err == nil {
		other.Close()
		t.Fatal("端口冲突不应自动改用下一端口")
	}
}
