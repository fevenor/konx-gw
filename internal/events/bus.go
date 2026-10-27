package events

import "sync"

// Bus is a synchronous fan-out to subscribers; slow subscribers must not
// block (each receive is dispatched in its own goroutine is deliberately
// NOT done — ordering matters for WS sequencing; keep handlers fast).
type Bus struct {
	mu   sync.RWMutex
	subs map[int]Subscriber
	next int
}

func NewBus() *Bus { return &Bus{subs: map[int]Subscriber{}} }

func (b *Bus) Subscribe(fn Subscriber) func() {
	b.mu.Lock()
	id := b.next
	b.next++
	b.subs[id] = fn
	b.mu.Unlock()
	return func() {
		b.mu.Lock()
		delete(b.subs, id)
		b.mu.Unlock()
	}
}

func (b *Bus) Publish(ev Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, fn := range b.subs {
		fn(ev)
	}
}
