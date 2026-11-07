package av

import (
	"net"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
)

// 设备首包可能被 NAT 丢弃，收到服务器地址后必须主动探测。
func TestMeterStartsFromServerAddresses(t *testing.T) {
	for _, tc := range []struct {
		name     string
		peer     bool
		relay    bool
		observed bool
		want     []string
	}{
		{"服务器提供设备地址", true, false, false, []string{"192.0.2.10:5000"}},
		{"设备与中继同时探测", true, true, false, []string{"192.0.2.10:5000", "192.0.2.20:6000"}},
		{"仅有中继地址", false, true, false, []string{"192.0.2.20:6000"}},
		{"设备已发来首包", true, false, true, []string{"192.0.2.30:7000"}},
		{"尚无可用地址", false, false, false, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tr := &fakeTransport{}
			c := New(10000001, 3221225473, 123, Options{
				Transport: tr, Log: quietLog(), MeterTicks: 1, MeterTickEvery: time.Millisecond,
			})
			payload := make([]byte, 0x68)
			if tc.peer {
				payload[0x34], payload[0x35] = 0x13, 0x88
				copy(payload[0x38:], net.ParseIP("192.0.2.10").To4())
			}
			if tc.relay {
				payload[0x4E] = 1
				payload[0x5A], payload[0x5B] = 0x17, 0x70
				copy(payload[0x5C:], net.ParseIP("192.0.2.20").To4())
			}
			c.HandleGute(&frame.Parsed{Type: 0x24, Payload: payload})
			if tc.observed {
				from := &net.UDPAddr{IP: net.ParseIP("192.0.2.30"), Port: 7000}
				c.Tap()(deviceMeterReq(c.linkID, c.devID, uint32(c.uid), 1), from)
			}
			// 忽略信令应答，仅检查主动探测的目标与会话归属。
			tr.sent, tr.addrs = nil, nil
			c.callSent = time.Now()
			c.meterPump()
			if len(tr.addrs) != len(tc.want) {
				t.Fatalf("主动探测目标数为 %d，期望 %d", len(tr.addrs), len(tc.want))
			}
			for i, want := range tc.want {
				if got := tr.addrs[i].String(); got != want {
					t.Errorf("探测目标为 %s，期望 %s", got, want)
				}
				if sub, ok := ClassifyMeter(tr.sent[i]); !ok || sub != MeterSubReq || MeterLinkID(tr.sent[i]) != c.linkID {
					t.Fatal("主动探测必须使用当前呼叫的 meter 请求")
				}
			}
		})
	}
}
