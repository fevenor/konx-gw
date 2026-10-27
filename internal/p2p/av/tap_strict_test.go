// tap 归属校验测试（M4）：meter 族须 link_id@+0x08 匹配本呼叫、KCP 段
// 须 conv 匹配本呼叫双 conv（ctl=linkID|0x80000000、file=linkID&0x7fff_ffff）
// 才被消费——杂散包不得作为会话心跳存活证据（假活根源）。
package av

import (
	"net"
	"testing"
	"time"
)

func TestTapOwnershipStrict(t *testing.T) {
	tr := &fakeTransport{}
	c := New(10000001, 3221225473, 387648820, Options{
		Transport: tr, Creds: CloudCreds{C1: 1, C2: 2, S1: 3, S2: 4},
		Log: quietLog(), MeterTickEvery: 10 * time.Millisecond,
	})
	linkID, _ := c.IDs()
	from, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:55555")

	// 归属 meter 包：消费
	if !c.Tap()(deviceMeterReq(linkID, 10000001, 3221225473, 1), from) {
		t.Fatal("本呼叫 meter 包必须被 tap 消费")
	}
	// 杂散 meter（link_id 不匹配）：不得消费
	if c.Tap()(deviceMeterReq(linkID+1, 10000001, 3221225473, 2), from) {
		t.Fatal("杂散 meter 包不得消费（假活根源）")
	}

	// 本呼叫双 conv 的 KCP 段：消费（ctl 门未开也消费，gute 不见它们）
	if !c.Tap()(WrapKcp(ForgeKcpPush(linkID|0x80000000, 1, []byte("x"))), from) {
		t.Fatal("本呼叫 ctl conv KCP 段必须被消费")
	}
	if !c.Tap()(WrapKcp(ForgeKcpPush(linkID&0x7fffffff, 1, []byte("x"))), from) {
		t.Fatal("本呼叫 file conv KCP 段必须被消费")
	}
	// 杂散 conv：不得消费
	if c.Tap()(WrapKcp(ForgeKcpPush(0xDEADBEEF, 2, []byte("x"))), from) {
		t.Fatal("杂散 conv KCP 段不得消费（假活根源）")
	}
}
