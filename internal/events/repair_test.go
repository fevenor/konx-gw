package events_test

// 本批修复（C2-1/D1/D3/M3）对应的行为测试：入库失败计数、keep_raw
// 配置、去重载荷指纹判重、客户端注册表 TTL 清理与升序补拉查询。

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/events"
)

func newStore(t *testing.T) *events.Store {
	t.Helper()
	st, err := events.Open(filepath.Join(t.TempDir(), "repair.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// TestDedupPayloadFingerprint（D3）：同设备+同类型+同 3s 窗口内，
// 载荷（含 msg_id）不同的事件是两次真实事件，均应入库；
// 完全重复（同 msg_id 同载荷）仅入一条。
func TestDedupPayloadFingerprint(t *testing.T) {
	st := newStore(t)
	eng := events.NewEngine(events.NewRegistry(), st)

	// 快速双击门铃：两次真实按键，推送 msg_id 与载荷不同
	eng.Submit(events.Event{MsgID: "m1", DeviceID: "10000001", Type: "doorbell.press", Payload: `{"count":1}`})
	eng.Submit(events.Event{MsgID: "m2", DeviceID: "10000001", Type: "doorbell.press", Payload: `{"count":2}`})

	got, err := st.Query("10000001", events.TypeDoorbellPress, 0, 10)
	if err != nil || len(got) != 2 {
		t.Fatalf("不同载荷的双击应均入库: err=%v len=%d", err, len(got))
	}

	// 完全重复（同 msg_id 同载荷）：仅一条
	eng.Submit(events.Event{MsgID: "m2", DeviceID: "10000001", Type: "doorbell.press", Payload: `{"count":2}`})
	got, _ = st.Query("10000001", events.TypeDoorbellPress, 0, 10)
	if len(got) != 2 {
		t.Fatalf("完全重复不应重复入库: len=%d", len(got))
	}

	// 无 msg_id 的空载荷事件：同键同窗口仍判重（保持旧语义）
	eng.Submit(events.Event{DeviceID: "cloud", Type: "gw.system"})
	eng.Submit(events.Event{DeviceID: "cloud", Type: "gw.system"})
	got, _ = st.Query("cloud", "gw.system", 0, 10)
	if len(got) != 1 {
		t.Fatalf("空载荷完全重复应判重: len=%d", len(got))
	}
}

// TestInsertFailureCounted（C2-1）：入库失败必须计数（供监控接入）且
// 不得打崩发布方。
func TestInsertFailureCounted(t *testing.T) {
	st := newStore(t)
	eng := events.NewEngine(events.NewRegistry(), st)
	// 关闭底层库后再提交：Insert 失败走显式失败分支
	st.Close()
	eng.Submit(events.Event{MsgID: "boom", DeviceID: "x", Type: "gw.system"})
	if n := eng.InsertFailures(); n < 1 {
		t.Fatalf("入库失败未计数: %d", n)
	}
}

// TestKeepRawConfig（D1）：keep_raw=false 跳过 raw 存储（读取侧为空）；
// 默认/显式 true 保持存 raw 的现状语义。
func TestKeepRawConfig(t *testing.T) {
	st := newStore(t)

	// 默认（未配置）：存 raw（现状语义）
	ev1 := events.Event{MsgID: "r1", DeviceID: "d", Type: "t", Raw: []byte{0xDE, 0xAD}}
	if err := st.Insert(&ev1); err != nil {
		t.Fatal(err)
	}
	if hex, err := st.RawHex(ev1.ID); err != nil || hex != "dead" {
		t.Fatalf("默认应存 raw: hex=%q err=%v", hex, err)
	}

	// keep_raw=false：跳过 raw
	st.SetKeepRaw(false)
	ev2 := events.Event{MsgID: "r2", DeviceID: "d", Type: "t", Raw: []byte{0xBE, 0xEF}}
	if err := st.Insert(&ev2); err != nil {
		t.Fatal(err)
	}
	if hex, err := st.RawHex(ev2.ID); err != nil || hex != "" {
		t.Fatalf("keep_raw=false 不应存 raw: hex=%q err=%v", hex, err)
	}

	// 恢复 true
	st.SetKeepRaw(true)
	ev3 := events.Event{MsgID: "r3", DeviceID: "d", Type: "t", Raw: []byte{0x01}}
	if err := st.Insert(&ev3); err != nil {
		t.Fatal(err)
	}
	if hex, err := st.RawHex(ev3.ID); err != nil || hex != "01" {
		t.Fatalf("恢复 keep_raw=true 后应存 raw: hex=%q err=%v", hex, err)
	}
}

// TestSweepStaleClients（M3）：TTL 清理只清 web 类僵尸行；
// SweepAllClientsInactive 清全部残留。
func TestSweepStaleClients(t *testing.T) {
	st := newStore(t)
	if err := st.UpsertClient("web-1", "web", "", ""); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertClient("rtsp-egress", "rtsp", "", ""); err != nil {
		t.Fatal(err)
	}

	// 负 TTL 强制把所有 web 类行视作过期（测试用）
	if err := st.SweepStaleClients(-time.Hour); err != nil {
		t.Fatal(err)
	}
	clients, err := st.ActiveClients()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 1 || clients[0]["id"] != "rtsp-egress" {
		t.Fatalf("TTL 清理后应仅剩 rtsp 行: %v", clients)
	}

	// 启动清理：全部置断开
	if err := st.SweepAllClientsInactive(); err != nil {
		t.Fatal(err)
	}
	clients, err = st.ActiveClients()
	if err != nil {
		t.Fatal(err)
	}
	if len(clients) != 0 {
		t.Fatalf("启动清理后应无活跃客户端: %v", clients)
	}
}

// TestQuerySinceOrder（M2① 支撑）：补拉查询按 id 升序返回、since 语义正确。
func TestQuerySinceOrder(t *testing.T) {
	st := newStore(t)
	ids := make([]int64, 0, 3)
	for i := 0; i < 3; i++ {
		ev := events.Event{MsgID: string(rune('a' + i)), DeviceID: "d", Type: "t"}
		if err := st.Insert(&ev); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, ev.ID)
	}
	got, err := st.QuerySince(ids[0], 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("since 之后应恰有 2 条: %d", len(got))
	}
	if got[0].ID != ids[1] || got[1].ID != ids[2] {
		t.Fatalf("应按 id 升序: got=%d,%d want=%d,%d", got[0].ID, got[1].ID, ids[1], ids[2])
	}
}
