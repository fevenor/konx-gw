// Package events implements the event engine (DESIGN §8): pluggable push
// decoders, MsgID+window dedup, SQLite persistence and a fan-out bus.
package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/fevenor/konx-gw/internal/cloud"
	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/session"
)

// Event types (DESIGN §8.1).
const (
	TypeDoorbellPress = "doorbell.press"
	TypeDeviceAlarm   = "device.alarm"
	TypeDeviceCalling = "device.calling"
	TypeDeviceOnline  = "device.online"
	TypeDeviceOffline = "device.offline"
	TypeCloudSession  = "cloud.session"
	TypeP2PSession    = "p2p.session"
	TypeSource        = "source"
	TypeSystem        = "gw.system"
	TypeUnknown       = "unknown"
)

// Event is the canonical normalized event.
type Event struct {
	ID       int64  `json:"id"`
	MsgID    string `json:"msg_id,omitempty"`
	DeviceID string `json:"device_id"`
	Type     string `json:"type"`
	TSEvent  int64  `json:"ts_event,omitempty"` // seconds, event-supplied
	TSRecv   int64  `json:"ts_recv"`            // seconds, gateway monotonic
	Payload  string `json:"payload"`            // normalized JSON
	Raw      []byte `json:"-"`                  // stored separately, not serialized
}

// PushDecoder turns one decoded notify into events (DESIGN §4.3).
type PushDecoder interface {
	Name() string
	Decode(n session.Notify) ([]Event, error)
}

// Registry of decoders by message name.
type Registry struct {
	mu       sync.RWMutex
	decoders map[string]PushDecoder
	fallback PushDecoder
}

func NewRegistry() *Registry {
	return &Registry{decoders: map[string]PushDecoder{}}
}

func (r *Registry) Register(name string, d PushDecoder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.decoders[name] = d
}

func (r *Registry) SetFallback(d PushDecoder) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.fallback = d
}

func (r *Registry) lookup(name string) PushDecoder {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if d, ok := r.decoders[name]; ok {
		return d
	}
	base := name
	if i := strings.IndexAny(base, "{/"); i > 0 {
		base = base[:i]
	}
	return r.fallback
}

// ---------------------------------------------------------------------------
// Built-in decoders
// ---------------------------------------------------------------------------

// checkLoginThrottle: the server re-pushes CheckLogin every 2-8s until the
// 0x36 app-level answer arrives; identical probe names are collapsed to one
// event per window (suppressed repeats are counted into the next emit).
const checkLoginThrottle = 300 * time.Second

// CheckLoginDecoder observes the server session probe.
type CheckLoginDecoder struct {
	mu       sync.Mutex
	lastEmit map[string]time.Time
	counts   map[string]int
}

func (*CheckLoginDecoder) Name() string { return "checklogin" }

func (d *CheckLoginDecoder) Decode(n session.Notify) ([]Event, error) {
	now := time.Now()
	d.mu.Lock()
	if d.lastEmit == nil {
		d.lastEmit = map[string]time.Time{}
		d.counts = map[string]int{}
	}
	d.counts[n.Name]++
	if t, ok := d.lastEmit[n.Name]; ok && now.Sub(t) < checkLoginThrottle {
		d.mu.Unlock()
		return nil, nil // suppressed repeat; count carried on next emit
	}
	d.lastEmit[n.Name] = now
	count := d.counts[n.Name]
	d.mu.Unlock()
	return []Event{{
		DeviceID: "cloud",
		Type:     TypeCloudSession,
		TSRecv:   now.Unix(),
		Payload:  mustJSON(map[string]any{"probe": n.Name, "msg_id": n.MsgID, "count": count}),
	}}, nil
}

// AlarmDecoder handles ALARM_MSG_FROM_SRV / CALLING / NOTICE_MSG family.
// alarm_type semantics: the exact "button pressed" value is pending T1
// live capture; the mapping table below is configurable at runtime.
type AlarmDecoder struct {
	// DoorbellTypes: alarm_type values meaning "doorbell button".
	DoorbellTypes map[uint32]bool
	// KnownDevices: device id → name resolution.
	KnownDevices map[string]string
}

func (AlarmDecoder) Name() string { return "alarm" }

func (d AlarmDecoder) Decode(n session.Notify) ([]Event, error) {
	// Try JSON body first (v2 pushes carry JSON; binary fallback below).
	if strings.TrimSpace(n.JSON) != "" {
		var m map[string]any
		if err := json.Unmarshal([]byte(n.JSON), &m); err == nil {
			dev := str(m, "dev_id", "did", "device_id", "DevID", "device ID")
			et := TypeDeviceAlarm
			// Official iOS dictionaries use spaced keys ("alarm type").
			alarmType := str(m, "alarm_type", "alarmType", "type", "alarm type")
			if isCalling(n.Name) {
				et = TypeDeviceCalling
			} else if d.isDoorbellName(n.Name) || d.isDoorbellTypeStr(alarmType) {
				et = TypeDoorbellPress
			}
			return []Event{{
				MsgID:    fmt.Sprintf("%d", n.MsgID),
				DeviceID: orDefault(dev, "unknown"),
				Type:     et,
				TSRecv:   time.Now().Unix(),
				Payload:  n.JSON,
				Raw:      n.Body,
			}}, nil
		}
	}
	// Binary ALARM layout.
	if a, err := frame.ParseAlarm(n.Body); err == nil {
		et := TypeDeviceAlarm
		if d.isDoorbellName(n.Name) || (d.DoorbellTypes != nil && d.DoorbellTypes[a.AlarmType]) {
			et = TypeDoorbellPress
		}
		return []Event{{
			MsgID:    fmt.Sprintf("%d", n.MsgID),
			DeviceID: fmt.Sprintf("%d", a.DevID),
			Type:     et,
			TSRecv:   time.Now().Unix(),
			Payload: mustJSON(map[string]any{
				"alarm_type": a.AlarmType, "data": string(a.Data),
			}),
			Raw: n.Body,
		}}, nil
	}
	// Unknown shape — preserve raw (U1 fallback path).
	return []Event{{
		MsgID:    fmt.Sprintf("%d", n.MsgID),
		DeviceID: "unknown",
		Type:     TypeUnknown,
		TSRecv:   time.Now().Unix(),
		Payload:  mustJSON(map[string]any{"msg_name": n.Name, "len": len(n.Body)}),
		Raw:      n.Body,
	}}, nil
}

func (d AlarmDecoder) isDoorbellName(name string) bool {
	return strings.Contains(strings.ToUpper(name), "CALL") == false &&
		(strings.Contains(strings.ToUpper(name), "BELL") || strings.Contains(strings.ToUpper(name), "DOOR"))
}

func (d AlarmDecoder) isDoorbellTypeStr(t string) bool {
	if t == "" || d.DoorbellTypes == nil {
		return false
	}
	var v uint32
	if _, err := fmt.Sscanf(t, "%d", &v); err != nil {
		return false
	}
	return d.DoorbellTypes[v]
}

func isCalling(name string) bool { return strings.Contains(strings.ToUpper(name), "CALL") }

// UnknownDecoder is the fallback: never drop, always preserve.
type UnknownDecoder struct{}

func (UnknownDecoder) Name() string { return "unknown" }
func (UnknownDecoder) Decode(n session.Notify) ([]Event, error) {
	return []Event{{
		MsgID:    fmt.Sprintf("%d", n.MsgID),
		DeviceID: "unknown",
		Type:     TypeUnknown,
		TSRecv:   time.Now().Unix(),
		Payload:  mustJSON(map[string]any{"msg_name": n.Name, "len": len(n.Body)}),
		Raw:      n.Body,
	}}, nil
}

// ---------------------------------------------------------------------------
// Engine: decode → dedup → store → bus
// ---------------------------------------------------------------------------

type Subscriber func(e Event)

type Engine struct {
	reg   *Registry
	store *Store
	bus   *Bus
	dedup *dedup
}

func NewEngine(reg *Registry, store *Store) *Engine {
	return &Engine{
		reg:   reg,
		store: store,
		bus:   NewBus(),
		dedup: newDedup(3*time.Second, 512),
	}
}

// IngestPush decodes and processes one inbound notify.
func (e *Engine) IngestPush(n session.Notify) {
	d := e.reg.lookup(n.Name)
	if d == nil {
		d = UnknownDecoder{}
	}
	evs, err := d.Decode(n)
	if err != nil {
		evs = []Event{{
			DeviceID: "unknown", Type: TypeUnknown, TSRecv: time.Now().Unix(),
			Payload: mustJSON(map[string]any{"decode_error": err.Error(), "msg_name": n.Name}),
			Raw:     n.Body,
		}}
	}
	for _, ev := range evs {
		e.Submit(ev)
	}
}

// Submit applies dedup, persistence and fan-out to a local event.
func (e *Engine) Submit(ev Event) {
	now := time.Now()
	if ev.TSRecv == 0 {
		ev.TSRecv = now.Unix()
	}
	if e.dedup.seen(ev) {
		return
	}
	if err := e.store.Insert(&ev); err == nil {
		e.bus.Publish(ev)
	}
}

// Subscribe registers a fan-out receiver; returns a cancel func.
func (e *Engine) Subscribe(fn Subscriber) func() {
	return e.bus.Subscribe(fn)
}

// dedup: MsgID primary + (device,type,3s window) secondary.
type dedup struct {
	mu     sync.Mutex
	window time.Duration
	msgIDs map[string]time.Time
	recent map[string]time.Time
}

func newDedup(w time.Duration, max int) *dedup {
	d := &dedup{window: w, msgIDs: map[string]time.Time{}, recent: map[string]time.Time{}}
	go func() {
		for range time.Tick(w) {
			d.mu.Lock()
			cut := time.Now().Add(-w)
			for k, t := range d.msgIDs {
				if t.Before(cut) {
					delete(d.msgIDs, k)
				}
			}
			for k, t := range d.recent {
				if t.Before(cut) {
					delete(d.recent, k)
				}
			}
			d.mu.Unlock()
		}
	}()
	return d
}

func (d *dedup) seen(ev Event) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ev.MsgID != "" {
		if _, ok := d.msgIDs[ev.MsgID]; ok {
			return true
		}
		d.msgIDs[ev.MsgID] = time.Now()
	}
	key := ev.DeviceID + "|" + ev.Type
	if _, ok := d.recent[key]; ok {
		return true
	}
	d.recent[key] = time.Now()
	return false
}

// Local event helpers used by other modules.

// CloudEvent reports login/logout transitions.
func CloudEvent(what string, s *cloud.Session) Event {
	pl := map[string]any{"what": what}
	if s != nil {
		pl["uid"] = s.UID
	}
	return Event{DeviceID: "cloud", Type: TypeCloudSession, TSRecv: time.Now().Unix(), Payload: mustJSON(pl)}
}

// SessionEvent reports p2p session state changes.
func SessionEvent(state string, server string) Event {
	return Event{
		DeviceID: "cloud", Type: TypeP2PSession, TSRecv: time.Now().Unix(),
		Payload: mustJSON(map[string]any{"state": state, "server": server}),
	}
}

// SystemEvent reports gateway lifecycle facts.
func SystemEvent(what string, detail any) Event {
	return Event{
		DeviceID: "gw", Type: TypeSystem, TSRecv: time.Now().Unix(),
		Payload: mustJSON(map[string]any{"what": what, "detail": detail}),
	}
}

func mustJSON(v any) string {
	b, _ := json.Marshal(v)
	return string(b)
}

func str(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch tv := v.(type) {
			case string:
				return tv
			case float64:
				return fmt.Sprintf("%.0f", tv)
			}
		}
	}
	return ""
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
