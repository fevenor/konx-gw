// p2p 源熔断（C2-3）与 closed 竞态兜底（D14）测试：连续建链失败进入
// 长冷却、冷却可被新观众显式需求（Start）提前唤醒、成功存活清零计数；
// dial 返回的呼叫在回调注册前已终结时 runOnce 必须补查兜底不挂起。
package media

import (
	"errors"
	"sync"
	"testing"
	"time"
)

// breakerEnv 熔断测试脚手架：快节奏参数 + dial 计数与最后一枚 caller。
type breakerEnv struct {
	mu       sync.Mutex
	dials    int
	last     *fakeCaller
	failDial bool
}

func (e *breakerEnv) dial(uint32, int) (P2PCaller, error) {
	e.mu.Lock()
	e.dials++
	n := e.dials
	fail := e.failDial
	e.mu.Unlock()
	if n <= 0 {
		return nil, nil
	}
	fc := &fakeCaller{}
	e.mu.Lock()
	e.last = fc
	e.mu.Unlock()
	if fail {
		return nil, errors.New("dial rejected")
	}
	return fc, nil
}

func (e *breakerEnv) count() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.dials
}

func (e *breakerEnv) cur() *fakeCaller {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.last
}

func newBreakerSource(dial P2PDialer) *p2pSource {
	src := newP2PSource(10000001, 387648820, dial, quietLog())
	// 快节奏：3 次失败熔断、300ms 冷却、40ms 即算成功存活、5ms 起步退避
	src.failLimit = 3
	src.cooldown = 300 * time.Millisecond
	src.successMin = 40 * time.Millisecond
	src.firstRetry = 5 * time.Millisecond
	return src
}

// TestP2PBreakerCooldownAndWake：连续失败达阈值进入冷却（窗口内无自动
// dial）；显式 Start（新观众需求）提前唤醒一次尝试；再失败继续冷却。
func TestP2PBreakerCooldownAndWake(t *testing.T) {
	env := &breakerEnv{failDial: true}
	src := newBreakerSource(env.dial)
	src.Start()
	defer src.Stop()

	// 3 次快速失败 → 熔断
	waitForCondMedia(t, 2*time.Second, func() bool { return env.count() >= 3 }, "未完成 3 次失败尝试")
	src.mu.Lock()
	cooling := src.cooling
	streak := src.failStreak
	src.mu.Unlock()
	if !cooling || streak != 3 {
		t.Fatalf("连续 3 次失败后应熔断: cooling=%v streak=%d", cooling, streak)
	}

	// 冷却窗口内（300ms，已留足调度余量取中段）不得自动重试
	time.Sleep(150 * time.Millisecond)
	if n := env.count(); n != 3 {
		t.Fatalf("冷却窗口内出现自动重试（dials=%d）", n)
	}

	// 新观众显式需求（Start 幂等分支）提前唤醒 → 立即一次尝试
	src.Start()
	waitForCondMedia(t, 2*time.Second, func() bool { return env.count() >= 4 }, "显式需求未唤醒冷却")
	// 唤醒后本次尝试仍失败（failDial）→ 仍处熔断态
	src.mu.Lock()
	cooling = src.cooling
	src.mu.Unlock()
	if !cooling {
		t.Fatal("唤醒尝试失败后应继续熔断冷却")
	}
}

// TestP2PBreakerResetOnSuccess：冷却到期后的成功呼叫（存活超过
// successMin）终结时清零失败计数——后续失败从 0 重新累计。
func TestP2PBreakerResetOnSuccess(t *testing.T) {
	env := &breakerEnv{failDial: true}
	src := newBreakerSource(env.dial)
	src.Start()
	defer src.Stop()

	// 3 次失败熔断
	waitForCondMedia(t, 2*time.Second, func() bool { return env.count() >= 3 }, "未完成 3 次失败尝试")

	// 冷却到期（300ms + 余量）自动重试；此时拨改为成功，呼叫存活超过
	// successMin 后由测试显式挂断（模拟设备 CLOSE）→ 视为建链成功过
	env.mu.Lock()
	env.failDial = false
	env.mu.Unlock()
	waitForCondMedia(t, 3*time.Second, func() bool { return env.count() >= 4 }, "冷却到期未自动重试")
	fc := env.cur()
	if fc == nil {
		t.Fatal("无 caller")
	}
	time.Sleep(80 * time.Millisecond) // > successMin(40ms)：存活即成功
	fc.Stop()                         // closed → runOnce 返回

	// 成功清零：等待失败计数被记录（成功后那次终结记 1），且退出熔断态
	waitForCondMedia(t, 2*time.Second, func() bool {
		src.mu.Lock()
		defer src.mu.Unlock()
		return src.failStreak == 1 && !src.cooling
	}, "成功呼叫未清零熔断计数")
}

// TestP2PSourceClosedBeforePump（D14）：dial 返回的呼叫在 OnState 注册前
// 即已 closed（事件必漏）——runOnce 补查必须立即返回错误进入重拨，
// 不得永久挂起。
func TestP2PSourceClosedBeforePump(t *testing.T) {
	var mu sync.Mutex
	dials := 0
	src := newP2PSource(10000001, 387648820, func(uint32, int) (P2PCaller, error) {
		mu.Lock()
		dials++
		n := dials
		mu.Unlock()
		if n == 1 {
			fc := &fakeCaller{}
			fc.stopped.Store(true) // dial 返回前已终结：回调注册晚于事件
			return fc, nil
		}
		return &fakeCaller{}, nil
	}, quietLog())
	src.firstRetry = 5 * time.Millisecond // 快节奏：失败后 5ms 即重拨
	src.Start()
	defer src.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := dials
		mu.Unlock()
		if n >= 2 {
			return // 第一次已 closed 的呼叫被补查兜底，重拨已发生
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("已 closed 呼叫未被补查兜底（dials=%d，runOnce 挂起）", dials)
}

// TestP2PStopClosesStatesChannel（S11-p2p）：Stop 收尾先发终态再关状态
// 通道；消费者 range 到 close 自然退出；Stop 后 emit 不再发送（不 panic）。
func TestP2PStopClosesStatesChannel(t *testing.T) {
	src := newP2PSource(10000001, 387648820, func(uint32, int) (P2PCaller, error) {
		return &fakeCaller{}, nil
	}, quietLog())
	src.Start()

	sawStopped := false
	closed := make(chan struct{})
	go func() {
		defer close(closed)
		for st := range src.States() {
			if st.State == StateStopped {
				sawStopped = true // 终态必须在通道关闭前送达（保序）
			}
		}
	}()

	time.Sleep(50 * time.Millisecond)
	src.Stop() // 内部 emit(StateStopped) 后 close(stateCh)

	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("状态通道未随 Stop 关闭（消费者 range 未退出）")
	}
	if !sawStopped {
		t.Fatal("通道关闭前未送达终态 StateStopped")
	}

	// Stop 之后的 emit（如迟到的 Pause 广播）必须静默丢弃而非 panic
	src.emit(StatePaused, nil)
	src.emit(StateActive, nil)
}
