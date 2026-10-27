// session 侧 M4/C1-5/D7 回归：session_hold 暂停期心跳判死豁免（恢复后
// 判定立即生效）、Run 未启动时 Stop 不阻塞、decodeNotify 边界行为。
package session_test

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/session"
)

// TestSessionHeartbeatImmuneDuringHold（M4 反向）：呼叫 Pause（session_hold
// 保持）后服务器全静默——暂停期无流量不得触发 heartbeat lost（重连会
// 拆掉保持中的链路）；Resume 后静默持续则恢复判死（豁免不掩蔽真断链）。
func TestSessionHeartbeatImmuneDuringHold(t *testing.T) {
	srv := newMockServer(testTerm)
	s, _ := newAVSession(srv, 100*time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	go s.Run(ctx)
	defer func() { cancel(); s.Stop() }()

	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReady })
	c, err := s.AttachAV(10000001, 387648820)
	if err != nil {
		t.Fatalf("AttachAV: %v", err)
	}

	// 暂停保持 + 服务器全静默（无心跳 ACK、无呼叫流量）
	c.Pause()
	srv.tr.setOnSend(func(b []byte) {})

	// 8+ 个心跳周期（3 周期即判死）：暂停期必须豁免
	time.Sleep(800 * time.Millisecond)
	if s.State() != session.StateReady {
		t.Fatalf("session_hold 暂停期被心跳误杀: state=%s", s.State())
	}

	// 对照：Resume 后无流量 → 恢复判死（计数在暂停期持续增长，恢复
	// 判定无延迟）→ 正常重连，且重连自动挂断呼叫
	c.Resume()
	waitFor(t, 3*time.Second, func() bool { return s.State() == session.StateReconnecting })
}

// TestSessionStopBeforeRunReturns（C1-5）：Run 从未启动时 Stop 必须立即
// 返回（原先 <-done 永久阻塞）；随后的 Run 立即落终态退出；重复 Stop
// 幂等安全。
func TestSessionStopBeforeRunReturns(t *testing.T) {
	s, _ := newAVSession(newMockServer(testTerm), 100*time.Millisecond)

	done := make(chan struct{})
	go func() {
		s.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run 未启动时 Stop 不得阻塞")
	}

	// Stop 先行后 Run：立即落终态返回，不得进入连接循环
	s.Run(context.Background())
	if s.State() != session.StateStopped {
		t.Fatalf("Stop 先行后 Run 应立即落终态: state=%s", s.State())
	}
	// 重复 Stop 幂等
	s.Stop()
}

// TestDecodeNotifyEdges（D7）：纯 JSON 体不得整串回退为 Name；短名保留；
// 无 JSON 回退截 64B 上限；前导二进制无名。
func TestDecodeNotifyEdges(t *testing.T) {
	// 纯 JSON 体：Name 空、JSON 取整串
	d := session.DecodeNotifyForTest(&frame.Notify{Body: []byte(`{"a":1}`)})
	if d.Name != "" || d.JSON != `{"a":1}` {
		t.Fatalf("纯 JSON 体: name=%q json=%q", d.Name, d.JSON)
	}
	// 短名（<3 字符）+ 合法 JSON：保留短名
	d = session.DecodeNotifyForTest(&frame.Notify{Body: []byte("AB\x00{\"x\":2}")})
	if d.Name != "AB" || d.JSON != `{"x":2}` {
		t.Fatalf("短名被丢: name=%q json=%q", d.Name, d.JSON)
	}
	// 紧贴 JSON 的短名（形态 a）
	d = session.DecodeNotifyForTest(&frame.Notify{Body: []byte("XY{\"y\":3}")})
	if d.Name != "XY" || d.JSON != `{"y":3}` {
		t.Fatalf("紧贴短名被丢: name=%q json=%q", d.Name, d.JSON)
	}
	// 无 JSON 回退：可打印前缀截 64B 上限
	d = session.DecodeNotifyForTest(&frame.Notify{Body: bytes.Repeat([]byte("Z"), 100)})
	if len(d.Name) != 64 {
		t.Fatalf("回退名应截 64B: got %d", len(d.Name))
	}
	// 前导二进制无 JSON：Name 空（不得把二进制垃圾当名）
	d = session.DecodeNotifyForTest(&frame.Notify{Body: []byte{0x01, 0x02, 'A', 'B'}})
	if d.Name != "" {
		t.Fatalf("前导二进制不得成名: %q", d.Name)
	}
}
