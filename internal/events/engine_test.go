package events_test

import (
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/events"
	"github.com/fevenor/konx-gw/internal/p2p/session"
)

func newTestEngine(t *testing.T) (*events.Engine, *events.Store) {
	t.Helper()
	st, err := events.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := events.NewRegistry()
	reg.Register("CheckLogin", &events.CheckLoginDecoder{})
	reg.Register("ALARM_MSG_FROM_SRV", events.AlarmDecoder{DoorbellTypes: map[uint32]bool{13: true}})
	reg.Register("CALLING", events.AlarmDecoder{})
	reg.SetFallback(events.UnknownDecoder{})
	return events.NewEngine(reg, st), st
}

func TestCheckLoginDecoding(t *testing.T) {
	eng, st := newTestEngine(t)
	eng.IngestPush(session.Notify{
		MsgID: 7, Name: "CheckLogin",
		JSON: `{"cmd":1,"sessionID":497459251}`,
	})
	got, err := st.Query("", events.TypeCloudSession, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("query: %v len=%d", err, len(got))
	}
}

func TestDoorbellBinaryAlarm(t *testing.T) {
	eng, st := newTestEngine(t)
	// binary ALARM layout: +0x04 u64 dev_id, +0x18 u32 type, +0x24 u16 len, +0x26 data
	body := make([]byte, 0x30)
	for i := 0; i < 8; i++ {
		body[0x04+i] = byte(uint64(10000001) >> (8 * i))
	}
	for i := 0; i < 4; i++ {
		body[0x18+i] = byte(uint32(13) >> (8 * i)) // doorbell type per official SDK mapping (13=doorbell push)
	}
	eng.IngestPush(session.Notify{MsgID: 42, Name: "ALARM_MSG_FROM_SRV", Body: body})
	got, err := st.Query("10000001", events.TypeDoorbellPress, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("doorbell query: %v len=%d", err, len(got))
	}
	if got[0].MsgID != "42" {
		t.Fatalf("msgid %q", got[0].MsgID)
	}
}

func TestUnknownPreservedWithRaw(t *testing.T) {
	eng, st := newTestEngine(t)
	body := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	eng.IngestPush(session.Notify{MsgID: 9, Name: "SomeNewMsg", Body: body})
	got, err := st.Query("unknown", events.TypeUnknown, 0, 10)
	if err != nil || len(got) != 1 {
		t.Fatalf("unknown query: %v len=%d", err, len(got))
	}
	rawHex, err := st.RawHex(got[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if rawHex != "deadbeef" {
		t.Fatalf("raw %q", rawHex)
	}
}

func TestDedupByMsgIDAndWindow(t *testing.T) {
	eng, st := newTestEngine(t)
	var mu sync.Mutex
	var n int
	eng.Subscribe(func(events.Event) { mu.Lock(); n++; mu.Unlock() })

	body := make([]byte, 0x30)
	// same MsgID twice → one stored
	eng.IngestPush(session.Notify{MsgID: 100, Name: "ALARM_MSG_FROM_SRV", Body: body})
	eng.IngestPush(session.Notify{MsgID: 100, Name: "ALARM_MSG_FROM_SRV", Body: body})
	// different MsgID but same device+type within window → deduped too
	eng.IngestPush(session.Notify{MsgID: 101, Name: "ALARM_MSG_FROM_SRV", Body: body})

	mu.Lock()
	if n != 1 {
		t.Fatalf("fan-out count = %d, want 1", n)
	}
	mu.Unlock()
	got, _ := st.Query("", events.TypeDeviceAlarm, 0, 100)
	if len(got) != 1 {
		t.Fatalf("stored %d events, want 1", len(got))
	}
}

func TestCleanup(t *testing.T) {
	eng, st := newTestEngine(t)
	eng.Submit(events.Event{DeviceID: "x", Type: "gw.system"})
	if err := st.Cleanup(90, 1); err != nil {
		t.Fatal(err)
	}
	got, _ := st.Query("", "", 0, 100)
	if len(got) > 1 {
		t.Fatalf("cleanup left %d", len(got))
	}
	_ = time.Second
}
