// 出口 per-session 记账测试：fake sourceManager 计数 + 127.0.0.1:0
// 本地服务（ServerStream.Initialize 需要已启动的 Server）。覆盖多 track
// 单引用、SETUP 未 PLAY 释放、多观众共享流条目、sub 归一单条目、并发
// 单流、源停拆联动与建流失败回滚。
package rtspout

import (
	"io"
	"log/slog"
	"sync"
	"testing"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/media"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func key(dev string, p media.Profile) string { return dev + "|" + string(p) }

// fakeSourceManager 计数各键的 Acquire/Release/RegisterWriter/
// UnregisterWriter/Prewarm 调用；Desc/HasSlot 可控（模拟描述就绪与否）。
// 收到的 profile 一律视为已归一（归一由被测 Server 在入口完成）。
type fakeSourceManager struct {
	mu          sync.Mutex
	acquires    map[string]int
	releases    map[string]int
	registers   map[string]int
	unregisters map[string]int
	prewarms    map[string]int
	descs       map[string]*description.Session
	slots       map[string]bool
	normSub     bool
}

func newFakeSourceManager() *fakeSourceManager {
	return &fakeSourceManager{
		acquires:    map[string]int{},
		releases:    map[string]int{},
		registers:   map[string]int{},
		unregisters: map[string]int{},
		prewarms:    map[string]int{},
		descs:       map[string]*description.Session{},
		slots:       map[string]bool{},
	}
}

func (f *fakeSourceManager) Acquire(d string, p media.Profile) (func(), error) {
	f.mu.Lock()
	f.acquires[key(d, p)]++
	f.mu.Unlock()
	return func() {}, nil
}

func (f *fakeSourceManager) Release(d string, p media.Profile) {
	f.mu.Lock()
	f.releases[key(d, p)]++
	f.mu.Unlock()
}

func (f *fakeSourceManager) Desc(d string, p media.Profile) *description.Session {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.descs[key(d, p)]
}

func (f *fakeSourceManager) HasSlot(d string, p media.Profile) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.slots[key(d, p)]
}

func (f *fakeSourceManager) Prewarm(d string) {
	f.mu.Lock()
	f.prewarms[d]++
	f.mu.Unlock()
}

func (f *fakeSourceManager) NormProfile(p media.Profile) media.Profile {
	if f.normSub && p == media.ProfileSub {
		return media.ProfileMain
	}
	return p
}

func (f *fakeSourceManager) RegisterWriter(d string, p media.Profile, fn func(*description.Media, *rtp.Packet)) {
	f.mu.Lock()
	f.registers[key(d, p)]++
	f.mu.Unlock()
}

func (f *fakeSourceManager) UnregisterWriter(d string, p media.Profile) {
	f.mu.Lock()
	f.unregisters[key(d, p)]++
	f.mu.Unlock()
}

func (f *fakeSourceManager) counters() (acq, rel, reg, unreg map[string]int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := func(src map[string]int) map[string]int {
		dst := make(map[string]int, len(src))
		for k, v := range src {
			dst[k] = v
		}
		return dst
	}
	return cp(f.acquires), cp(f.releases), cp(f.registers), cp(f.unregisters)
}

// newTestServer 起一个绑定 127.0.0.1:0 的出口服务（Initialize 需要已
// 启动的 Server），测试结束自动关闭。
func newTestServer(t *testing.T, mgr *fakeSourceManager) *Server {
	t.Helper()
	s := New("127.0.0.1:0", mgr, nil, nil, quietLog())
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s
}

// readyDesc 标记某键描述就绪。
func (f *fakeSourceManager) readyDesc(k string) {
	f.mu.Lock()
	f.descs[k] = &description.Session{Title: "fake " + k}
	f.slots[k] = true
	f.mu.Unlock()
}

func setupOK(t *testing.T, s *Server, sess *gortsplib.ServerSession, path string) *gortsplib.ServerStream {
	t.Helper()
	resp, ss, err := s.OnSetup(&gortsplib.ServerHandlerOnSetupCtx{Session: sess, Path: path})
	if err != nil {
		t.Fatalf("OnSetup(%s) err: %v", path, err)
	}
	if resp.StatusCode != base.StatusOK {
		t.Fatalf("OnSetup(%s) status = %v", path, resp.StatusCode)
	}
	if ss == nil {
		t.Fatalf("OnSetup(%s) 返回空流", path)
	}
	return ss
}

func closeSession(s *Server, sess *gortsplib.ServerSession) {
	s.OnSessionClose(&gortsplib.ServerHandlerOnSessionCloseCtx{Session: sess})
}

// TestSetupMultiTrackSingleRef：同会话对同一路径多次 SETUP（多 track
// 场景）只 Acquire 一次；会话关闭恰一次 Release、一份 writer 注册/
// 注销。
func TestSetupMultiTrackSingleRef(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	sess := &gortsplib.ServerSession{}
	for i := 0; i < 3; i++ {
		setupOK(t, s, sess, "/konx/d1/main")
	}
	acq, rel, reg, _ := mgr.counters()
	if acq["d1|main"] != 1 {
		t.Fatalf("Acquire = %d，须 1（多 track 去重）", acq["d1|main"])
	}
	if reg["d1|main"] != 1 {
		t.Fatalf("RegisterWriter = %d，须 1", reg["d1|main"])
	}

	closeSession(s, sess)
	_, rel, _, unreg := mgr.counters()
	if rel["d1|main"] != 1 {
		t.Fatalf("Release = %d，须恰 1（修引用泄漏）", rel["d1|main"])
	}
	if unreg["d1|main"] != 1 {
		t.Fatalf("UnregisterWriter = %d，须 1", unreg["d1|main"])
	}
}

// TestSetupWithoutPlayStillReleased：SETUP 后未 PLAY 即断开——源引用仍
// 被释放（SETUP 即记账，不依赖 PLAY 注册）。
func TestSetupWithoutPlayStillReleased(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	sess := &gortsplib.ServerSession{}
	setupOK(t, s, sess, "/konx/d1/main")
	closeSession(s, sess)

	_, rel, _, unreg := mgr.counters()
	if rel["d1|main"] != 1 {
		t.Fatalf("未 PLAY 会话 Release = %d，须 1", rel["d1|main"])
	}
	if unreg["d1|main"] != 1 {
		t.Fatalf("未 PLAY 会话 UnregisterWriter = %d，须 1", unreg["d1|main"])
	}
}

// TestTeardownKeepsStreamForOtherViewer：A 断开不拆共享流（writer 不注
// 销、B 的 ServerStream 仍是同一份）；B 也断开后才拆（恰一次注销）。
func TestTeardownKeepsStreamForOtherViewer(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	sessA, sessB := &gortsplib.ServerSession{}, &gortsplib.ServerSession{}
	ssA := setupOK(t, s, sessA, "/konx/d1/main")
	ssB := setupOK(t, s, sessB, "/konx/d1/main")
	if ssA != ssB {
		t.Fatal("两位观众必须共享同一 ServerStream")
	}

	closeSession(s, sessA)
	_, rel, reg, unreg := mgr.counters()
	if unreg["d1|main"] != 0 {
		t.Fatalf("A 断开后不得注销 writer（unregister=%d）", unreg["d1|main"])
	}
	if rel["d1|main"] != 1 {
		t.Fatalf("A 断开后 Release = %d，须 1", rel["d1|main"])
	}
	s.mu.Lock()
	_, alive := s.streams["d1|main"]
	s.mu.Unlock()
	if !alive {
		t.Fatal("A 断开后共享流条目被拆除（B 仍在观看）")
	}

	closeSession(s, sessB)
	_, rel, reg, unreg = mgr.counters()
	if unreg["d1|main"] != 1 {
		t.Fatalf("B 断开后 UnregisterWriter = %d，须恰 1", unreg["d1|main"])
	}
	if rel["d1|main"] != 2 {
		t.Fatalf("Release 总数 = %d，须 2", rel["d1|main"])
	}
	if reg["d1|main"] != 1 {
		t.Fatalf("RegisterWriter = %d，须 1", reg["d1|main"])
	}
	s.mu.Lock()
	_, alive = s.streams["d1|main"]
	s.mu.Unlock()
	if alive {
		t.Fatal("观众清零后条目未拆除")
	}
}

// TestSubNormalizedToMainSingleEntry：p2p-av 单码流（sub 归一到 main）下
// main/sub 两个会话只产生一份 writer 键 d1|main；sub 会话关闭不注销，
// main 会话关闭恰注销一次。
func TestSubNormalizedToMainSingleEntry(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.normSub = true
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	sessMain, sessSub := &gortsplib.ServerSession{}, &gortsplib.ServerSession{}
	ssMain := setupOK(t, s, sessMain, "/konx/d1/main")
	ssSub := setupOK(t, s, sessSub, "/konx/d1/sub")
	if ssMain != ssSub {
		t.Fatal("main/sub 归一后必须共享同一 ServerStream")
	}
	_, _, reg, _ := mgr.counters()
	if reg["d1|main"] != 1 || reg["d1|sub"] != 0 {
		t.Fatalf("RegisterWriter 计数 = %v（须只落 d1|main 一份）", reg)
	}

	closeSession(s, sessSub)
	_, _, _, unreg := mgr.counters()
	if unreg["d1|main"] != 0 {
		t.Fatalf("sub 会话关闭不得注销共享 writer（unregister=%d）", unreg["d1|main"])
	}
	closeSession(s, sessMain)
	_, rel, _, unreg := mgr.counters()
	if unreg["d1|main"] != 1 {
		t.Fatalf("main 会话关闭后 UnregisterWriter = %d，须恰 1", unreg["d1|main"])
	}
	if rel["d1|main"] != 2 {
		t.Fatalf("Release 总数 = %d，须 2（每会话恰一次）", rel["d1|main"])
	}
}

// TestConcurrentSetupSingleStream：20 个会话并发 SETUP 同一路径——
// 查-建同临界区，RegisterWriter 恰一次；全部关闭后注销恰一次。
func TestConcurrentSetupSingleStream(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	const n = 20
	sessions := make([]*gortsplib.ServerSession, n)
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := range sessions {
		sessions[i] = &gortsplib.ServerSession{}
		wg.Add(1)
		go func(sess *gortsplib.ServerSession) {
			defer wg.Done()
			resp, _, err := s.OnSetup(&gortsplib.ServerHandlerOnSetupCtx{Session: sess, Path: "/konx/d1/main"})
			if err != nil {
				errCh <- err
				return
			}
			if resp.StatusCode != base.StatusOK {
				errCh <- errNotReady
			}
		}(sessions[i])
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("并发 SETUP 出错: %v", err)
	}

	_, _, reg, _ := mgr.counters()
	if reg["d1|main"] != 1 {
		t.Fatalf("并发下 RegisterWriter = %d，须恰 1（无双建）", reg["d1|main"])
	}
	for _, sess := range sessions {
		closeSession(s, sess)
	}
	_, rel, _, unreg := mgr.counters()
	if rel["d1|main"] != n {
		t.Fatalf("Release = %d，须 %d", rel["d1|main"], n)
	}
	if unreg["d1|main"] != 1 {
		t.Fatalf("UnregisterWriter = %d，须恰 1", unreg["d1|main"])
	}
}

// TestOnSourceStateDropsEntry：源 StateStopped 联动失效出口条目（含
// DESCRIBE-only 孤儿条目）；重复触发幂等（不重复注销）。
func TestOnSourceStateDropsEntry(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	s := newTestServer(t, mgr)

	// DESCRIBE-only：建条目但不占观众
	resp, ss, err := s.OnDescribe(&gortsplib.ServerHandlerOnDescribeCtx{Path: "/konx/d1/main"})
	if err != nil || resp.StatusCode != base.StatusOK || ss == nil {
		t.Fatalf("OnDescribe 失败: %v %v %v", resp.StatusCode, ss, err)
	}
	_, _, reg, _ := mgr.counters()
	if reg["d1|main"] != 1 {
		t.Fatalf("RegisterWriter = %d，须 1", reg["d1|main"])
	}

	s.OnSourceState(media.SourceState{Device: "d1", Profile: media.ProfileMain, State: media.StateStopped})
	s.mu.Lock()
	_, alive := s.streams["d1|main"]
	s.mu.Unlock()
	if alive {
		t.Fatal("StateStopped 后孤儿条目未失效")
	}
	_, _, _, unreg := mgr.counters()
	if unreg["d1|main"] != 1 {
		t.Fatalf("UnregisterWriter = %d，须 1", unreg["d1|main"])
	}

	// 幂等：再次触发不重复注销
	s.OnSourceState(media.SourceState{Device: "d1", Profile: media.ProfileMain, State: media.StateStopped})
	_, _, _, unreg = mgr.counters()
	if unreg["d1|main"] != 1 {
		t.Fatalf("重复触发后 UnregisterWriter = %d，须仍为 1（幂等）", unreg["d1|main"])
	}

	// 非 Stopped 状态不动作
	mgr.readyDesc("d1|main")
	if resp, _, err := s.OnDescribe(&gortsplib.ServerHandlerOnDescribeCtx{Path: "/konx/d1/main"}); err != nil || resp.StatusCode != base.StatusOK {
		t.Fatalf("重建条目失败: %v %v", resp.StatusCode, err)
	}
	s.OnSourceState(media.SourceState{Device: "d1", Profile: media.ProfileMain, State: media.StateActive})
	s.mu.Lock()
	_, alive = s.streams["d1|main"]
	s.mu.Unlock()
	if !alive {
		t.Fatal("非 Stopped 状态不得失效条目")
	}
}

// TestSetupRollbackOnStreamFail：描述始终不就绪（无槽位即不等待）——
// SETUP 回 503 且 Acquire/Release 严格配对、不注册 writer。
func TestSetupRollbackOnStreamFail(t *testing.T) {
	mgr := newFakeSourceManager() // 无 readyDesc：Desc 恒 nil、HasSlot 恒 false
	s := newTestServer(t, mgr)

	sess := &gortsplib.ServerSession{}
	resp, _, err := s.OnSetup(&gortsplib.ServerHandlerOnSetupCtx{Session: sess, Path: "/konx/d1/main"})
	if err != nil {
		t.Fatalf("OnSetup err: %v", err)
	}
	if resp.StatusCode != base.StatusServiceUnavailable {
		t.Fatalf("status = %v，须 503", resp.StatusCode)
	}
	acq, rel, reg, _ := mgr.counters()
	if acq["d1|main"] != 1 || rel["d1|main"] != 1 {
		t.Fatalf("Acquire/Release = %d/%d，须 1/1（回滚配对）", acq["d1|main"], rel["d1|main"])
	}
	if reg["d1|main"] != 0 {
		t.Fatalf("建流失败不得注册 writer（register=%d）", reg["d1|main"])
	}
	// 会话关闭时无记账可释（已回滚），不得二次 Release
	closeSession(s, sess)
	_, rel, _, _ = mgr.counters()
	if rel["d1|main"] != 1 {
		t.Fatalf("关闭后 Release = %d，不得重复释放", rel["d1|main"])
	}
}

// TestOnPlayHookPlayedOnly：hook 上行只在会话首次 PLAY 且有 SETUP 记账
// 时触发；未 PLAY 的会话关闭不触发下行。
func TestOnPlayHookPlayedOnly(t *testing.T) {
	mgr := newFakeSourceManager()
	mgr.readyDesc("d1|main")
	var mu sync.Mutex
	up, down := 0, 0
	s := New("127.0.0.1:0", mgr, nil, func(kind string, u bool) {
		mu.Lock()
		if u {
			up++
		} else {
			down++
		}
		mu.Unlock()
	}, quietLog())
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)

	sess := &gortsplib.ServerSession{}
	setupOK(t, s, sess, "/konx/d1/main")
	// 未 PLAY 的裸 PLAY 请求（无记账会话）不得计上行
	if resp, _ := s.OnPlay(&gortsplib.ServerHandlerOnPlayCtx{Session: &gortsplib.ServerSession{}, Path: "/konx/d1/main"}); resp.StatusCode != base.StatusOK {
		t.Fatal("PLAY 裸会话应放行（鉴权未启用）")
	}
	if resp, _ := s.OnPlay(&gortsplib.ServerHandlerOnPlayCtx{Session: sess, Path: "/konx/d1/main"}); resp.StatusCode != base.StatusOK {
		t.Fatal("PLAY 应成功")
	}
	// 重复 PLAY 只计一次上行
	if resp, _ := s.OnPlay(&gortsplib.ServerHandlerOnPlayCtx{Session: sess, Path: "/konx/d1/main"}); resp.StatusCode != base.StatusOK {
		t.Fatal("重复 PLAY 应成功")
	}
	mu.Lock()
	if up != 1 {
		t.Fatalf("上行计数 = %d，须 1（首次 PLAY）", up)
	}
	mu.Unlock()

	closeSession(s, sess)
	mu.Lock()
	if down != 1 {
		t.Fatalf("下行计数 = %d，须 1（played 会话）", down)
	}
	mu.Unlock()

	// 未 PLAY 会话：关闭不触下行
	sess2 := &gortsplib.ServerSession{}
	setupOK(t, s, sess2, "/konx/d1/main")
	closeSession(s, sess2)
	mu.Lock()
	if down != 1 {
		t.Fatalf("未 PLAY 会话关闭后下行 = %d，须仍为 1", down)
	}
	mu.Unlock()
}

// 防呆：保证 fake 实现满足接口缝（编译期校验）。
var _ sourceManager = (*fakeSourceManager)(nil)
