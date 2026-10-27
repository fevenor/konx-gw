// Package talk implements the intercom pipeline (DESIGN §6). The session
// model, three entrypoints and the G.711 codec are complete; the device
// sink ships as UnavailableSink until the p2pv3 device-session reverse
// engineering (RE_TASKS T3–T6) lands — requests then fail with a clear
// 501 instead of pretending.
package talk

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// ErrUnavailable is returned while the p2p-av sink is not implemented.
var ErrUnavailable = errors.New("talk is awaiting the p2pv3 device-session module (RE_TASKS T3–T6 / milestone M5)")

// Sink sends encoded audio frames to the device.
type Sink interface {
	Start(ctx context.Context, deviceID string, codec string) error
	Write(b []byte) error // one codec frame (e.g. 160B G.711 @20ms)
	Stop() error
}

// UnavailableSink is the pre-M5 sink.
type UnavailableSink struct{}

func (UnavailableSink) Start(context.Context, string, string) error { return ErrUnavailable }
func (UnavailableSink) Write([]byte) error                          { return ErrUnavailable }
func (UnavailableSink) Stop() error                                 { return nil }

// Session is one active intercom (per-device exclusive).
type Session struct {
	DeviceID string
	Started  time.Time
	By       string // entrypoint id (web / api / onvif)
	codec    string
	sink     Sink

	mu     sync.Mutex
	last   time.Time
	closed bool
	cancel context.CancelFunc
}

// Manager owns per-device sessions with heartbeat timeouts.
type Manager struct {
	sinkFn func(deviceID string) Sink // sink factory (replaced at M5)

	mu       sync.Mutex
	sessions map[string]*Session
	OnChange func(deviceID string, active bool, by string)
}

func NewManager() *Manager {
	return &Manager{sessions: map[string]*Session{}, sinkFn: func(string) Sink { return UnavailableSink{} }}
}

// SetSinkFactory installs a real sink factory (M5).
func (m *Manager) SetSinkFactory(fn func(deviceID string) Sink) {
	m.mu.Lock()
	m.sinkFn = fn
	m.mu.Unlock()
}

// Start begins an intercom session; exclusive per device.
func (m *Manager) Start(ctx context.Context, deviceID, by string) (*Session, error) {
	m.mu.Lock()
	sinkFn := m.sinkFn
	if _, busy := m.sessions[deviceID]; busy {
		m.mu.Unlock()
		return nil, errors.New("talk session busy on this device")
	}
	sink := sinkFn(deviceID)
	sctx, cancel := context.WithCancel(ctx)
	s := &Session{DeviceID: deviceID, Started: time.Now(), By: by, codec: "PCMA", sink: sink, cancel: cancel, last: time.Now()}
	m.sessions[deviceID] = s
	m.mu.Unlock()

	if err := sink.Start(sctx, deviceID, s.codec); err != nil {
		m.mu.Lock()
		delete(m.sessions, deviceID)
		m.mu.Unlock()
		return nil, err
	}
	if m.OnChange != nil {
		m.OnChange(deviceID, true, by)
	}
	// heartbeat watchdog: 5s without Write/Ping closes the session
	go func() {
		t := time.NewTicker(time.Second)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-t.C:
				s.mu.Lock()
				idle := time.Since(s.last)
				s.mu.Unlock()
				if idle > 5*time.Second {
					m.Stop(deviceID)
					return
				}
			}
		}
	}()
	return s, nil
}

// WritePCM16 accepts 8kHz mono s16le chunks, encodes to G.711 A-law and
// forwards 160-byte frames.
func (m *Manager) WritePCM16(deviceID string, pcm []byte) error {
	m.mu.Lock()
	s := m.sessions[deviceID]
	m.mu.Unlock()
	if s == nil {
		return errors.New("no talk session")
	}
	// 320B PCM = 160 samples = 20ms PCMA frame
	for off := 0; off+320 <= len(pcm); off += 320 {
		frame := PCM16ToPCMA(pcm[off : off+320])
		if err := s.sink.Write(frame); err != nil {
			return err
		}
	}
	s.mu.Lock()
	s.last = time.Now()
	s.mu.Unlock()
	return nil
}

// Ping refreshes the heartbeat (entry without audio).
func (m *Manager) Ping(deviceID string) {
	m.mu.Lock()
	s := m.sessions[deviceID]
	m.mu.Unlock()
	if s != nil {
		s.mu.Lock()
		s.last = time.Now()
		s.mu.Unlock()
	}
}

// Stop ends the session.
func (m *Manager) Stop(deviceID string) {
	m.mu.Lock()
	s := m.sessions[deviceID]
	delete(m.sessions, deviceID)
	m.mu.Unlock()
	if s != nil {
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			s.cancel()
		}
		s.mu.Unlock()
		_ = s.sink.Stop()
		if m.OnChange != nil {
			m.OnChange(deviceID, false, s.By)
		}
	}
}

// Active reports the active session for one device (nil if none).
func (m *Manager) Active(deviceID string) *Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[deviceID]
}

// ActiveAll lists active sessions.
func (m *Manager) ActiveAll() map[string]*Session {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]*Session, len(m.sessions))
	for k, v := range m.sessions {
		out[k] = v
	}
	return out
}

var _ = fmt.Sprintf
