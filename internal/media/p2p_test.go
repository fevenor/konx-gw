// p2p-av 媒体源测试：mock av Caller 驱动 → 断言描述合成、状态流转与
// RTP 打包（单 NAL 直装 / 大 NAL FU-A 分片）。
package media

import (
	"bytes"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/p2p/av"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeCaller 模拟 *av.Caller（OnH264 驱动；含会话保持 Pause/Resume 计数）。
type fakeCaller struct {
	mu      sync.Mutex
	onH264  func([]byte)
	onState func(av.State, error)
	stopped atomic.Bool
	pausedF atomic.Bool
	pauseN  atomic.Int32
	resumeN atomic.Int32
	devID   uint32
	pwd     int
}

func (f *fakeCaller) OnH264(fn func([]byte)) {
	f.mu.Lock()
	f.onH264 = fn
	f.mu.Unlock()
}
func (f *fakeCaller) OnState(fn func(av.State, error)) {
	f.mu.Lock()
	f.onState = fn
	f.mu.Unlock()
}
func (f *fakeCaller) State() av.State {
	if f.stopped.Load() {
		return av.StateClosed
	}
	if f.pausedF.Load() {
		return av.StatePaused
	}
	return av.StateStreaming
}
func (f *fakeCaller) Stop() {
	f.stopped.Store(true)
	f.mu.Lock()
	cb := f.onState
	f.mu.Unlock()
	if cb != nil {
		cb(av.StateClosed, nil)
	}
}
func (f *fakeCaller) Pause() {
	f.pauseN.Add(1)
	f.pausedF.Store(true)
	f.mu.Lock()
	cb := f.onState
	f.mu.Unlock()
	if cb != nil {
		cb(av.StatePaused, nil)
	}
}
func (f *fakeCaller) Resume() {
	f.resumeN.Add(1)
	f.pausedF.Store(false)
	f.mu.Lock()
	cb := f.onState
	f.mu.Unlock()
	if cb != nil {
		cb(av.StateStreaming, nil)
	}
}
func (f *fakeCaller) feed(nal []byte) {
	f.mu.Lock()
	cb := f.onH264
	f.mu.Unlock()
	if cb != nil {
		cb(nal)
	}
}

func nal(hdr byte, size int) []byte {
	b := make([]byte, 4+size)
	copy(b, []byte{0, 0, 0, 1})
	b[4] = hdr
	for i := 5; i < len(b); i++ {
		b[i] = byte(i * 31)
	}
	return b
}

// TestP2PSourceDescAndRTP：SPS/PPS → 描述合成（sprop）+ StateActive；
// 小 NAL 单包直装、大 NAL FU-A 分片（S/E/marker/seq 连续/载荷重组还原）。
func TestP2PSourceDescAndRTP(t *testing.T) {
	fc := &fakeCaller{}
	src := newP2PSource(10000001, 387648820, func(uint32, int) (P2PCaller, error) {
		return fc, nil
	}, quietLog())

	states := make(chan State, 16)
	go func() {
		for st := range src.States() {
			select {
			case states <- st.State:
			default:
			}
		}
	}()
	var mu sync.Mutex
	var got []*rtp.Packet
	var gotMedia []*description.Media
	src.SetSink(func(md *description.Media, p *rtp.Packet) {
		mu.Lock()
		got = append(got, p)
		gotMedia = append(gotMedia, md)
		mu.Unlock()
	})
	src.Start()
	defer src.Stop()
	waitState(t, states, StateStarting, 2*time.Second)

	fc.feed(nal(0x67, 12)) // SPS
	if src.Desc() != nil {
		t.Fatal("描述须待 SPS+PPS 双全")
	}
	fc.feed(nal(0x68, 6)) // PPS
	waitState(t, states, StateActive, 2*time.Second)
	desc := src.Desc()
	if desc == nil || len(desc.Medias) != 1 {
		t.Fatalf("desc = %v", desc)
	}

	// 小 NAL：单包直装 + marker
	fc.feed(nal(0x41, 100))
	// 大 NAL：FU-A 分片
	fc.feed(nal(0x65, 3000))
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	if len(got) < 2 {
		t.Fatalf("RTP packets = %d", len(got))
	}
	// 末包 marker=1（每 NAL）
	if !got[0].Marker {
		t.Fatal("single-NAL packet marker 未置位")
	}
	// 找 FU-A 序列（大 NAL 的分片 ≥2）
	var fu []*rtp.Packet
	for _, p := range got[1:] {
		if len(p.Payload) >= 2 && p.Payload[0]&0x1f == 28 {
			fu = append(fu, p)
		}
	}
	if len(fu) < 2 {
		t.Fatalf("FU-A fragments = %d", len(fu))
	}
	if fu[0].Payload[1]&0x80 == 0 {
		t.Fatal("FU-A 首片缺 S 位")
	}
	if fu[len(fu)-1].Payload[1]&0x40 == 0 {
		t.Fatal("FU-A 末片缺 E 位")
	}
	if !fu[len(fu)-1].Marker {
		t.Fatal("FU-A 末片 marker 未置位")
	}
	for i := 1; i < len(fu); i++ {
		if fu[i].SequenceNumber != fu[i-1].SequenceNumber+1 {
			t.Fatalf("seq 不连续 @%d", i)
		}
	}
	// 重组还原 NAL 载荷
	var reas []byte
	reas = append(reas, (fu[0].Payload[0]&0xe0)|(fu[0].Payload[1]&0x1f))
	for _, p := range fu {
		reas = append(reas, p.Payload[2:]...)
	}
	want := nal(0x65, 3000)[4:]
	if !bytes.Equal(reas, want) {
		t.Fatalf("FU-A 重组不匹配: %d vs %d 字节", len(reas), len(want))
	}
}

func waitState(t *testing.T, ch <-chan State, want State, timeout time.Duration) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case s := <-ch:
			if s == want {
				return
			}
		case <-deadline:
			t.Fatalf("state %s not reached", want)
		}
	}
}

// TestManagerP2PAcquire：Acquire 分支走 p2p 源；sub 回退 main；treated
// 密码直配生效；密码缺失报配置错误。
func TestManagerP2PAcquire(t *testing.T) {
	cfg := config.Defaults()
	cfg.Media.Source = "p2p-av"
	cfg.Media.P2P.TreatedPassword = 387648820
	cfg.Devices = []config.Device{{ID: "10000001"}}
	m := NewManager(cfg, quietLog(), nil)

	var dialed atomic.Int32
	var devIDArg atomic.Uint32
	var pwdArg atomic.Int32
	m.SetP2PDialer(func(d uint32, p int) (P2PCaller, error) {
		dialed.Add(1)
		devIDArg.Store(d)
		pwdArg.Store(int32(p))
		return &fakeCaller{}, nil
	})
	// sub 请求 → 回退 main 槽（dial 在 Start goroutine 内异步发生）
	rel, err := m.Acquire("10000001", ProfileSub)
	if err != nil {
		t.Fatal(err)
	}
	defer rel()
	deadline := time.Now().Add(2 * time.Second)
	for dialed.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if dialed.Load() != 1 {
		t.Fatalf("dial called %d times", dialed.Load())
	}
	if devIDArg.Load() != 10000001 || pwdArg.Load() != 387648820 {
		t.Fatalf("dial args = %d/%d", devIDArg.Load(), pwdArg.Load())
	}
	if _, ok := m.StateOf("10000001")[ProfileMain]; !ok {
		t.Fatal("main slot 未建立")
	}

	// 密码缺失：无直配无解析器 → 配置错误（不 panic）
	cfg2 := config.Defaults()
	cfg2.Media.Source = "p2p-av"
	cfg2.Devices = []config.Device{{ID: "10000001"}}
	m2 := NewManager(cfg2, quietLog(), nil)
	if _, err := m2.Acquire("10000001", ProfileMain); err == nil {
		t.Fatal("缺 treated 密码必须报错")
	} else {
		t.Log("密码缺失错误:", err)
	}
	// 非数字设备 id
	cfg3 := config.Defaults()
	cfg3.Media.Source = "p2p-av"
	cfg3.Media.P2P.TreatedPassword = 1
	cfg3.Devices = []config.Device{{ID: "front-bell"}}
	m3 := NewManager(cfg3, quietLog(), nil)
	m3.SetP2PDialer(func(uint32, int) (P2PCaller, error) { return &fakeCaller{}, nil })
	if _, err := m3.Acquire("front-bell", ProfileMain); err == nil {
		t.Fatal("非数字设备 id 必须报错")
	}
}
