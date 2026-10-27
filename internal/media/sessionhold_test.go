// session_hold（media.p2p.session_hold）空闲路径测试：HardIdle 到期不拆
// 呼叫改 Pause（槽位/描述保留），新观众 Acquire 走 Resume 不重新 dial；
// 保持中的呼叫死亡且无观众时拆槽终止重拨，下次观看全新 Acquire。
package media

import (
	"sync"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/config"
)

// TestManagerP2PSessionHoldIdlePath：Acquire→Active→release→HardIdle 到期
// →StatePaused（槽位与 Desc 保留、无重拨）；再 Acquire→Resume（dial 仍
// 1 次）；呼叫死亡→槽位拆除→再观看全新 dial。
func TestManagerP2PSessionHoldIdlePath(t *testing.T) {
	cfg := config.Defaults()
	cfg.Media.Source = "p2p-av"
	cfg.Media.P2P.TreatedPassword = 387648820
	cfg.Media.P2P.SessionHold = true
	cfg.Media.P2P.HardIdle = 80 * time.Millisecond
	cfg.Devices = []config.Device{{ID: "10000001"}}
	m := NewManager(cfg, quietLog(), nil)

	// dialer 在源 goroutine 内被调（calls 需互斥保护）。
	var dmu sync.Mutex
	var calls []*fakeCaller
	m.SetP2PDialer(func(uint32, int) (P2PCaller, error) {
		fc := &fakeCaller{}
		dmu.Lock()
		calls = append(calls, fc)
		dmu.Unlock()
		return fc, nil
	})
	cur := func() *fakeCaller {
		dmu.Lock()
		defer dmu.Unlock()
		if len(calls) == 0 {
			return nil
		}
		return calls[len(calls)-1]
	}
	nDials := func() int {
		dmu.Lock()
		defer dmu.Unlock()
		return len(calls)
	}
	defer m.StopAll()

	// 第一位观众：建槽 →（等 dial + OnH264 注册后）SPS/PPS → Active
	rel, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondMedia(t, 3*time.Second, func() bool {
		if cur() == nil {
			return false
		}
		// 持续补喂（覆盖 dial→OnH264 注册的窗口），Active 即停
		cur().feed(nal(0x67, 12)) // SPS
		cur().feed(nal(0x68, 6))  // PPS
		return m.StateOf("10000001")[ProfileMain] == StateActive
	}, "源未到 Active")
	if m.Desc("10000001", ProfileMain) == nil {
		t.Fatal("描述未合成")
	}

	// 观众离开 → HardIdle 到期 → 暂停保持（不拆链：槽位/描述保留）
	rel()
	waitForCondMedia(t, 2*time.Second, func() bool {
		return m.StateOf("10000001")[ProfileMain] == StatePaused
	}, "空闲到期未进入 paused")
	if nDials() != 1 {
		t.Fatalf("暂停不得重新 dial（calls=%d）", nDials())
	}
	if cur().pauseN.Load() != 1 {
		t.Fatalf("caller.Pause 调用 %d 次", cur().pauseN.Load())
	}
	if !m.HasSlot("10000001", ProfileMain) {
		t.Fatal("暂停保持必须保留槽位")
	}
	if m.Desc("10000001", ProfileMain) == nil {
		t.Fatal("暂停保持必须保留描述缓存")
	}

	// 新观众：同链路 Resume，不重新 dial
	rel2, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondMedia(t, 2*time.Second, func() bool {
		return m.StateOf("10000001")[ProfileMain] == StateActive
	}, "Resume 后未回 Active")
	if cur().resumeN.Load() != 1 {
		t.Fatalf("caller.Resume 调用 %d 次", cur().resumeN.Load())
	}
	if nDials() != 1 {
		t.Fatalf("新观众必须走 Resume 而非重新 dial（calls=%d）", nDials())
	}

	// 再次空闲 → 再暂停；保持中的呼叫死亡（设备 CLOSE）且无观众 →
	// 拆槽终止退避重拨（不无限重呼烧节流窗）
	rel2()
	waitForCondMedia(t, 2*time.Second, func() bool {
		return cur().pauseN.Load() == 2
	}, "第二次空闲未再暂停")
	cur().Stop() // 模拟设备挂断
	waitForCondMedia(t, 2*time.Second, func() bool {
		return !m.HasSlot("10000001", ProfileMain)
	}, "保持中呼叫死亡且无观众必须拆槽")
	if nDials() != 1 {
		t.Fatalf("死亡拆槽不得触发重拨（calls=%d）", nDials())
	}

	// 下次观看：全新 Acquire → 重新 dial
	rel3, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	defer rel3()
	waitForCondMedia(t, 2*time.Second, func() bool {
		return nDials() == 2
	}, "拆槽后新观看必须重新 dial")
}

func waitForCondMedia(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
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
