package events

import (
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

// 事件总线投递参数：
//   - 每订阅者独立缓冲队列 + 独立投递协程，回调慢或 panic 只影响该订阅者自身；
//   - 队列已满视为慢消费者，立即取消该订阅并清理其投递协程；
//   - Publish 对已入队事件做有界等待：快速回调会在 Publish 返回前完成
//     （保持既有同步投递语义），慢回调最多拖住发布方一个等待窗口。
const (
	busQueueCap = 256                    // 每订阅者缓冲队列容量
	busWaitCap  = 250 * time.Millisecond // 发布方单条事件的有界等待
	busWaitPoll = 100 * time.Microsecond // 有界等待期间的轮询间隔
)

// subscriber 是一个已注册的订阅者及其专属投递管道。
type subscriber struct {
	id    int
	fn    Subscriber
	ch    chan Event   // 待投递事件队列（FIFO）
	depth atomic.Int32 // 当前积压深度：入队时超过 1 说明订阅者本就消费落后，发布方无需等待
}

// Bus is a fan-out bus: each subscriber owns a buffered queue and a dispatch
// goroutine, so a slow or panicking consumer can never stall publishers or
// other subscribers.
type Bus struct {
	mu   sync.RWMutex
	subs map[int]*subscriber
	next int
}

func NewBus() *Bus { return &Bus{subs: map[int]*subscriber{}} }

func (b *Bus) Subscribe(fn Subscriber) func() {
	b.mu.Lock()
	sub := &subscriber{
		id: b.next,
		fn: fn,
		ch: make(chan Event, busQueueCap),
	}
	b.next++
	b.subs[sub.id] = sub
	b.mu.Unlock()
	go b.pump(sub)
	return func() { b.unsubscribe(sub) }
}

// pump 是订阅者的独立投递协程：按 FIFO 顺序逐条调用回调。
func (b *Bus) pump(sub *subscriber) {
	for ev := range sub.ch {
		b.dispatch(sub, ev)
		sub.depth.Add(-1)
	}
}

// dispatch 调用一个订阅者回调；回调 panic 在此兜底，不打崩投递协程与
// 发布方，也不影响其他订阅者。
func (b *Bus) dispatch(sub *subscriber, ev Event) {
	defer func() {
		if r := recover(); r != nil {
			slog.Default().Error("事件总线：订阅者处理事件发生 panic，已拦截",
				"subscriber", sub.id, "event_type", ev.Type, "panic", fmt.Sprint(r))
		}
	}()
	sub.fn(ev)
}

// Publish 将事件投递给所有订阅者，绝不长期阻塞发布方：
// 队列已满的慢订阅者会被立即取消订阅；刚入队且订阅者无积压的事件
// 最多等待一个窗口（维持快速回调下 Publish 返回即投递完成的既有语义）。
func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	pending := make([]*subscriber, 0, len(b.subs))
	var slow []*subscriber
	for _, sub := range b.subs {
		select {
		case sub.ch <- ev:
			if sub.depth.Add(1) == 1 {
				pending = append(pending, sub)
			}
		default:
			slow = append(slow, sub)
		}
	}
	b.mu.RUnlock()
	for _, sub := range slow {
		b.unsubscribe(sub)
		slog.Default().Warn("事件总线：订阅者消费过慢（队列已满），已断开该订阅者",
			"subscriber", sub.id, "event_type", ev.Type)
	}
	for _, sub := range pending {
		// 有界等待积压清零：不用信号通道，避免令牌积压把 pump 反向卡死
		deadline := time.Now().Add(busWaitCap)
		for sub.depth.Load() > 0 && time.Now().Before(deadline) {
			time.Sleep(busWaitPoll)
		}
	}
}

// unsubscribe 取消订阅并清理（幂等）：关闭队列令投递协程排空后退出。
// 队列的写入都在读锁内、关闭在写锁内，二者互斥，不会向已关闭通道发送。
func (b *Bus) unsubscribe(sub *subscriber) {
	b.mu.Lock()
	if _, ok := b.subs[sub.id]; !ok {
		b.mu.Unlock()
		return
	}
	delete(b.subs, sub.id)
	b.mu.Unlock()
	close(sub.ch)
}
