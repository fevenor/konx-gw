// 会话保持（session_hold）测试：Pause 停 cmd=1 重发且丢 TLV4 但链路
// （meter/KCP 应答）存活；Resume 同链路重发 cmd=1，设备再 ACCEPT（实测
// ground truth：ctl=[6,2,2]）后舞蹈 [6,1] 重放、媒体恢复；未暂停的
// Resume no-op；closed 优先于一切。
package av

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"
)

func waitForCond(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %v: %s", timeout, msg)
}

// waitForState 轮询等待 Caller 状态。
func waitForState(t *testing.T, c *Caller, want State, timeout time.Duration) {
	t.Helper()
	waitForCond(t, timeout, func() bool { return c.State() == want }, "state "+string(want))
}

// ctlSentOf 读 cmd=1 累计写入计数（不含 kcp-go 底层重传，观测干净）。
func ctlSentOf(c *Caller) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctlSent
}

func acceptedOf(c *Caller) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.accepted
}

func meterAckCount(c *Caller) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.meterAcks
}

// wireCtl 统计已上线（KCP PUSH 载荷）的指定 cmd AvStreamCtl 帧份数。
// 每次 Write 向 5 个目的地发送 + 底层重传，故仅用于“增长”断言。
func wireCtl(tr *fakeTransport, c *Caller, cmd uint32) int {
	want := BuildAvStreamCtl(c.sessionID, cmd, c.token, c.pwdEnc, 0)
	tr.mu.Lock()
	defer tr.mu.Unlock()
	n := 0
	for _, w := range tr.sent {
		if !IsKcpFrame(w) {
			continue
		}
		seg, ok := UnwrapKcp(w)
		if !ok || len(seg) < 24+len(want) || SegCmd(seg) != 81 {
			continue
		}
		if bytes.Equal(seg[24:24+len(want)], want) {
			n++
		}
	}
	return n
}

// newHeldCaller 建立 Caller 并推到 StateCtl：meter 门控开、KCP 双 conv
// 建立、cmd=1 重发循环在跑（复用 TestCallerMeterGateAndTeardown 驱动法）。
func newHeldCaller(t *testing.T, tr *fakeTransport) *Caller {
	t.Helper()
	c := New(10000001, 3221225473, 387648820, Options{
		Transport: tr, Creds: CloudCreds{C1: 1, C2: 2, S1: 3, S2: 4},
		Log: quietLog(), MeterTickEvery: 10 * time.Millisecond,
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() { cancel(); c.Stop() })
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	linkID, _ := c.IDs()
	from, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:55555")
	// 设备 meter_req ×7（我方逐个回 ack）+ 设备 ack ×6（单侧门控保底）
	for i := 1; i <= 7; i++ {
		c.Tap()(deviceMeterReq(linkID, 10000001, 3221225473, uint32(i)), from)
	}
	ack := deviceMeterReq(linkID, 10000001, 3221225473, 1)
	ack[5] = 2
	Put32(ack, 0x0C, 0)
	for i := 0; i < 6; i++ {
		c.Tap()(ack, from)
	}
	waitForState(t, c, StateCtl, 3*time.Second)
	return c
}

// feedTlv3 注入一个设备 TLV3 应答（直接喂 KCP 重组字节流边界）。
func feedTlv3(c *Caller, cmd, echo uint32) {
	p := make([]byte, 0x1C)
	binary.LittleEndian.PutUint32(p[0:4], echo)
	binary.LittleEndian.PutUint32(p[4:8], cmd)
	copy(p[0xC:], bytes.Repeat([]byte{0x5A}, 16))
	c.onKcpBytes(mkTlv(3, 0, p, false))
}

// feedTlv4Nal 把一个 Annex-B NAL 装进 GW AV 容器作为 TLV4 喂入（容器
// 剥离由 onTlv4Locked 完成，与生产同路）。
func feedTlv4Nal(c *Caller, nal []byte, ts uint64) {
	c.onKcpBytes(mkTlv(4, 0, gwAVFrame(nal, ts), false))
}

func mkNal(tag string) []byte {
	return append([]byte{0, 0, 0, 1, 0x41}, []byte(tag)...)
}

// TestCallerPauseResumeSessionHold：完整保持周期——
// ctl 发送中 Pause（循环即停、meter/KCP 仍消费）→ Resume（cmd=1 重发）
// → 伪 ACCEPT（舞蹈 cmd=6 重放）→ START → 媒体喂出 → 再 Pause（TLV4
// 丢弃）→ 再 Resume + 再 ACCEPT（舞蹈重置后再跳）→ 媒体恢复。
func TestCallerPauseResumeSessionHold(t *testing.T) {
	tr := &fakeTransport{}
	c := newHeldCaller(t, tr)
	linkID, sid := c.IDs()
	from, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:55555")

	// ctl 循环在发送
	n1 := ctlSentOf(c)
	waitForCond(t, 2*time.Second, func() bool { return ctlSentOf(c) > n1 }, "ctl cycle sending")

	// ---- Pause：循环停止（kick 即退）、状态叠加 paused ----
	c.Pause()
	if c.State() != StatePaused {
		t.Fatalf("state after Pause = %s, want paused", c.State())
	}
	n := ctlSentOf(c)
	time.Sleep(600 * time.Millisecond)
	if got := ctlSentOf(c); got != n {
		t.Fatalf("暂停期 cmd=1 不得继续重发: %d → %d", n, got)
	}

	// 暂停期 meter/KCP 照常消费且应答（gute 会话心跳抑制依赖此行为）
	if !c.Tap()(deviceMeterReq(linkID, 10000001, 3221225473, 99), from) {
		t.Fatal("暂停期 meter 包必须仍被 tap 消费")
	}
	acks := meterAckCount(c)
	if acks < 1 {
		t.Fatal("暂停期必须仍应答设备 meter_req")
	}
	if !c.Tap()(WrapKcp(ForgeKcpPush(linkID|0x80000000, 5000, []byte("x"))), from) {
		t.Fatal("暂停期 KCP 包必须仍被 tap 消费")
	}

	// ---- Resume：同链路 cmd=1 重发 ----
	c.Resume()
	waitForCond(t, 2*time.Second, func() bool { return ctlSentOf(c) > n }, "resume 后 cmd=1 未重发")

	// 伪 ACCEPT（实测：会话存活期设备对重复 cmd=1 再次 ACCEPT）→
	// 舞蹈 [6,1] 重放（cmd=6 走 file conv 上线）
	d6 := wireCtl(tr, c, CmdStart)
	feedTlv3(c, CmdAccept, sid)
	waitForCond(t, 2*time.Second, func() bool { return wireCtl(tr, c, CmdStart) > d6 }, "舞蹈 cmd=6 未重放")
	waitForCond(t, 2*time.Second, func() bool { return acceptedOf(c) }, "未记录 ACCEPT")

	// START → streaming，媒体喂出
	feedTlv3(c, CmdStart, sid)
	waitForState(t, c, StateStreaming, 3*time.Second)
	var mu sync.Mutex
	var got [][]byte
	c.OnH264(func(nal []byte) { mu.Lock(); got = append(got, append([]byte(nil), nal...)); mu.Unlock() })
	feedTlv4Nal(c, mkNal("N1"), 1)
	feedTlv4Nal(c, mkNal("F1"), 2) // 边界驱动：F1 起始码冲出 N1
	waitForCond(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) == 1 && bytes.Contains(got[0], []byte("N1"))
	}, "恢复后媒体未喂出")

	// ---- 再 Pause：TLV4 丢弃（不进 onH264Fn）----
	c.Pause()
	if c.State() != StatePaused {
		t.Fatalf("streaming 上叠加暂停失败: %s", c.State())
	}
	feedTlv4Nal(c, mkNal("N2"), 3)
	feedTlv4Nal(c, mkNal("F2"), 4)
	time.Sleep(300 * time.Millisecond)
	mu.Lock()
	nGot := len(got)
	mu.Unlock()
	if nGot != 1 {
		t.Fatalf("暂停期 TLV4 不得喂出 NAL（got=%d）", nGot)
	}
	if !c.Tap()(deviceMeterReq(linkID, 10000001, 3221225473, 100), from) {
		t.Fatal("streaming 暂停期 meter 包必须仍被消费")
	}

	// ---- 再 Resume + 再 ACCEPT：舞蹈重置后再跳，媒体恢复 ----
	d6b := wireCtl(tr, c, CmdStart)
	c.Resume()
	feedTlv3(c, CmdAccept, sid)
	waitForCond(t, 2*time.Second, func() bool { return wireCtl(tr, c, CmdStart) > d6b }, "二次 ACCEPT 未重放舞蹈")
	feedTlv3(c, CmdStart, sid)
	feedTlv4Nal(c, mkNal("N3"), 5)
	feedTlv4Nal(c, mkNal("F3"), 6)
	waitForCond(t, 2*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(got) >= 2 && bytes.Contains(got[len(got)-1], []byte("N3"))
	}, "二次恢复后媒体未重新喂出")
}

// TestCallerResumeNoopAndClosedSafety：未 pause 的 Resume 是 no-op（不
// 重启 cmd=1 循环）；closed 优先于一切——暂停中 Stop 即 closed，其后
// Pause/Resume 均安全且不复活。
func TestCallerResumeNoopAndClosedSafety(t *testing.T) {
	tr := &fakeTransport{}
	c := newHeldCaller(t, tr)
	_, sid := c.IDs()

	// 先到达 accepted（循环收敛、计数稳定）
	feedTlv3(c, CmdAccept, sid)
	waitForCond(t, 2*time.Second, func() bool { return acceptedOf(c) }, "未记录 ACCEPT")
	n := ctlSentOf(c)
	time.Sleep(600 * time.Millisecond)
	if got := ctlSentOf(c); got != n {
		t.Fatalf("accepted 后循环应已停: %d → %d", n, got)
	}

	// 未 pause 的 Resume：no-op
	c.Resume()
	time.Sleep(600 * time.Millisecond)
	if got := ctlSentOf(c); got != n {
		t.Fatalf("未暂停的 Resume 不得重启 cmd=1 循环: %d → %d", n, got)
	}
	if c.State() == StatePaused {
		t.Fatal("no-op Resume 不得进入 paused")
	}

	// 暂停中 Stop → closed 优先；随后 Pause/Resume 安全
	c.Pause()
	if c.State() != StatePaused {
		t.Fatalf("state after Pause = %s", c.State())
	}
	c.Stop()
	if c.State() != StateClosed {
		t.Fatalf("closed 优先于 paused: %s", c.State())
	}
	c.Pause()
	c.Resume()
	if c.State() != StateClosed {
		t.Fatalf("closed 后 Pause/Resume 不得改变状态: %s", c.State())
	}
}
