// 源生命周期重做测试：rtspSource Start 幂等与 Stop 关闭状态通道
// （S10/S11）、槽位监听协程不泄漏（C0-2）、session_hold 多观众引用
// 计数（观众清零才 Pause）。
package media

import (
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/config"
)

// deadPortURL 指向本地必然拒连的端口（无监听）：pull 立即失败进入退避，
// 用于验证 Start 幂等与拆槽路径的协程回收。
const deadPortURL = "rtsp://127.0.0.1:1/{profile}"

// TestRTSPSourceStartIdempotent：连续多次 Start 只起一个拉流循环（恰
// 一次 StateStarting）；退避期内再 Start 不新增；Stop 后状态通道关闭。
func TestRTSPSourceStartIdempotent(t *testing.T) {
	src := newRTSPSource("rtsp://127.0.0.1:1/dead", "tcp", quietLog())
	src.Start()
	src.Start()

	var starting atomic.Int32
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for st := range src.States() {
			if st.State == StateStarting {
				starting.Add(1)
			}
		}
	}()
	// 覆盖「连接失败 → StateError → 退避」窗口后再打一次 Start
	time.Sleep(300 * time.Millisecond)
	src.Start()
	src.Stop()
	<-closed
	if n := starting.Load(); n != 1 {
		t.Fatalf("StateStarting 次数 = %d，须恰为 1", n)
	}
}

// TestSourceStatesChClosedOnStop：Stop 先发终态 StateStopped 再关闭状态
// 通道（缓冲保序），监听方 for-range 可正常退出不泄漏。
func TestSourceStatesChClosedOnStop(t *testing.T) {
	src := newRTSPSource("rtsp://127.0.0.1:1/dead", "tcp", quietLog())
	src.Start()
	src.Stop()

	gotStopped := false
	deadline := time.After(2 * time.Second)
	for {
		select {
		case st, ok := <-src.States():
			if !ok {
				if !gotStopped {
					t.Fatal("通道关闭前未收到 StateStopped")
				}
				return
			}
			if st.State == StateStopped {
				gotStopped = true
			}
		case <-deadline:
			t.Fatal("2s 内状态通道未关闭")
		}
	}
}

// TestManagerSlotListenerNoLeak：多轮建槽→拆槽后监听协程全部退出。
// 源为 external-rtsp 死端口（立即拒连、快速进退避）：Active 需真实上游
// 才能到达，而本用例考察的是拆槽路径上「源 Stop → 状态通道关闭 →
// 监听协程退出」的回收链，与是否到过 Active 无关。
func TestManagerSlotListenerNoLeak(t *testing.T) {
	cfg := config.Defaults()
	cfg.Media.Source = "external-rtsp"
	cfg.Media.ExternalURL = deadPortURL
	cfg.Media.IdleStop = 40 * time.Millisecond
	cfg.Devices = []config.Device{{ID: "10000001"}}
	m := NewManager(cfg, quietLog(), nil)
	defer m.StopAll()

	// 预热一轮让运行时进入稳态（首轮 goroutine 池成形）
	rel, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	rel()
	waitForCondMedia(t, 2*time.Second, func() bool {
		return !m.HasSlot("10000001", ProfileMain)
	}, "预热轮槽位未拆除")
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	for i := 0; i < 5; i++ {
		rel, err := m.Acquire("10000001", ProfileMain)
		if err != nil {
			t.Fatal(err)
		}
		rel()
		waitForCondMedia(t, 2*time.Second, func() bool {
			return !m.HasSlot("10000001", ProfileMain)
		}, "空闲到期未拆槽")
	}
	waitForCondMedia(t, 3*time.Second, func() bool {
		return runtime.NumGoroutine() <= base+1
	}, "拆槽后协程未回收（监听协程泄漏）")
}

// TestManagerSessionHoldMultiViewer：session_hold 下多位观众——A 释放后
// 呼叫仍 Active（B 持引用，不 Pause 不拆槽）；B 也释放后空闲到期进入
// Paused（观众清零的暂停路径被真实触发）。
func TestManagerSessionHoldMultiViewer(t *testing.T) {
	cfg := config.Defaults()
	cfg.Media.Source = "p2p-av"
	cfg.Media.P2P.TreatedPassword = 387648820
	cfg.Media.P2P.SessionHold = true
	cfg.Media.P2P.HardIdle = 80 * time.Millisecond
	cfg.Devices = []config.Device{{ID: "10000001"}}
	m := NewManager(cfg, quietLog(), nil)

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
	defer m.StopAll()

	// 两位观众先后 Acquire（第二位走 Start 幂等分支）
	relA, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	relB, err := m.Acquire("10000001", ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	waitForCondMedia(t, 3*time.Second, func() bool {
		if cur() == nil {
			return false
		}
		cur().feed(nal(0x67, 12)) // SPS
		cur().feed(nal(0x68, 6))  // PPS
		return m.StateOf("10000001")[ProfileMain] == StateActive
	}, "源未到 Active")

	// A 离开：仍有 B 持引用——不得暂停、不得拆槽
	relA()
	time.Sleep(200 * time.Millisecond) // 覆盖 HardIdle 时长
	if st := m.StateOf("10000001")[ProfileMain]; st != StateActive {
		t.Fatalf("A 释放后状态 = %s，须仍为 active（B 仍观看）", st)
	}
	if cur().pauseN.Load() != 0 {
		t.Fatalf("仍有观众时不得 Pause（pause=%d）", cur().pauseN.Load())
	}
	if !m.HasSlot("10000001", ProfileMain) {
		t.Fatal("仍有观众时不得拆槽")
	}

	// B 也离开：观众清零 → HardIdle 到期 → 暂停保持
	relB()
	waitForCondMedia(t, 2*time.Second, func() bool {
		return m.StateOf("10000001")[ProfileMain] == StatePaused
	}, "观众清零后未进入 paused")
	if cur().pauseN.Load() != 1 {
		t.Fatalf("观众清零后 caller.Pause 调用 %d 次，须恰 1 次", cur().pauseN.Load())
	}
}
