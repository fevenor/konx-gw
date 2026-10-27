package events_test

// 事件总线健壮性测试：非阻塞投递、慢消费者断开、panic 隔离、
// FIFO 保序与幂等退订。

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/events"
)

// waitFor 轮询等待条件成立，超时返回 false。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return cond()
}

// TestBusFIFOOrder：单订阅者应按发布顺序（入队顺序）收到全部事件。
func TestBusFIFOOrder(t *testing.T) {
	bus := events.NewBus()
	var mu sync.Mutex
	var got []int64
	done := make(chan struct{})
	unsub := bus.Subscribe(func(ev events.Event) {
		mu.Lock()
		got = append(got, ev.ID)
		if len(got) == 50 {
			close(done)
		}
		mu.Unlock()
	})
	defer unsub()

	for i := 1; i <= 50; i++ {
		bus.Publish(events.Event{ID: int64(i), Type: "t"})
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("未在时限内收齐事件")
	}
	mu.Lock()
	defer mu.Unlock()
	for i, id := range got {
		if id != int64(i+1) {
			t.Fatalf("顺序被破坏：got[%d]=%d", i, id)
		}
	}
}

// TestBusSlowSubscriberDroppedNotStallPublisher：慢订阅者队列填满后应被
// 断开（退订），发布方每次 Publish 都应有界返回，快订阅者不受影响。
func TestBusSlowSubscriberDroppedNotStallPublisher(t *testing.T) {
	bus := events.NewBus()
	release := make(chan struct{})
	defer close(release) // 测试结束释放阻塞中的慢订阅者

	var slowGot atomic.Int64
	bus.Subscribe(func(events.Event) {
		slowGot.Add(1)
		<-release // 模拟卡死的慢消费者
	})

	var fastGot atomic.Int64
	fastDone := make(chan struct{})
	fastUnsub := bus.Subscribe(func(ev events.Event) {
		if fastGot.Add(1) == int64(300) {
			close(fastDone)
		}
	})
	defer fastUnsub()

	// 超过队列容量持续发布：慢订阅者应被断开，发布方绝不能被长期拖住
	for i := 0; i < 300; i++ {
		t0 := time.Now()
		bus.Publish(events.Event{ID: int64(i), Type: "t"})
		if d := time.Since(t0); d > 2*time.Second {
			t.Fatalf("Publish 被慢订阅者拖住 %v（第 %d 次）", d, i)
		}
	}

	select {
	case <-fastDone:
	case <-time.After(5 * time.Second):
		t.Fatalf("快订阅者被慢订阅者拖累：仅收到 %d/300", fastGot.Load())
	}
	if slowGot.Load() > int64(1) {
		t.Fatalf("慢订阅者未被及时断开，已被回调 %d 次", slowGot.Load())
	}
}

// TestBusPanicRecovered：订阅者回调 panic 不得打崩发布方，其他订阅者
// 必须完整收到后续事件。
func TestBusPanicRecovered(t *testing.T) {
	bus := events.NewBus()
	bus.Subscribe(func(events.Event) {
		panic("回调故障演练")
	})

	var got atomic.Int64
	gotAll := make(chan struct{})
	unsub := bus.Subscribe(func(ev events.Event) {
		if got.Add(1) == 10 {
			close(gotAll)
		}
	})
	defer unsub()

	for i := 0; i < 10; i++ {
		bus.Publish(events.Event{ID: int64(i), Type: "t"}) // 若未 recover，此处直接打崩测试
	}
	select {
	case <-gotAll:
	case <-time.After(2 * time.Second):
		t.Fatalf("panic 订阅者影响了其他订阅者：仅收到 %d/10", got.Load())
	}
}

// TestBusUnsubscribeIdempotent：退订函数重复调用安全，退订后不再收到事件。
func TestBusUnsubscribeIdempotent(t *testing.T) {
	bus := events.NewBus()
	var n atomic.Int64
	unsub := bus.Subscribe(func(events.Event) { n.Add(1) })
	unsub()
	unsub() // 重复退订不得 panic

	bus.Publish(events.Event{Type: "t"})
	time.Sleep(50 * time.Millisecond)
	if n.Load() != 0 {
		t.Fatalf("退订后仍收到 %d 条事件", n.Load())
	}
}

// TestBusConcurrentPublishNoLoss：并发发布下任一订阅者应恰好收到全部
// 事件（不丢失、不重复；跨发布者的全局顺序不作为总线承诺）。
// 总量控制在队列容量内：超过容量的突发本就应触发慢消费者断开策略。
func TestBusConcurrentPublishNoLoss(t *testing.T) {
	bus := events.NewBus()
	const publishers, perPub = 8, 30
	total := publishers * perPub

	var mu sync.Mutex
	seen := make(map[int64]int)
	done := make(chan struct{})
	unsub := bus.Subscribe(func(ev events.Event) {
		mu.Lock()
		seen[ev.ID]++
		if len(seen) == total {
			close(done)
		}
		mu.Unlock()
	})
	defer unsub()

	var wg sync.WaitGroup
	for p := 0; p < publishers; p++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < perPub; i++ {
				bus.Publish(events.Event{ID: int64(base*perPub + i + 1), Type: "t"})
			}
		}(p)
	}
	wg.Wait()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("事件丢失：仅收到 %d/%d", len(seen), total)
	}
	mu.Lock()
	defer mu.Unlock()
	for id, c := range seen {
		if c != 1 {
			t.Fatalf("事件 %d 重复投递 %d 次", id, c)
		}
	}
}
