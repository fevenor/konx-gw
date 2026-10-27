// AttachAV/DetachAV 门控测试：未 Ready 拒绝、未挂载 Detach 不 panic。
package session_test

import (
	"io"
	"log/slog"
	"testing"

	"github.com/fevenor/konx-gw/internal/p2p/session"
)

func TestAttachAVRequiresReady(t *testing.T) {
	s := session.New(session.Options{
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}, session.Events{})
	if _, err := s.AttachAV(10000001, 387648820); err == nil {
		t.Fatal("AttachAV before Ready must fail")
	}
}

func TestDetachAVNoop(t *testing.T) {
	s := session.New(session.Options{}, session.Events{})
	s.DetachAV() // 未挂载时不得 panic
}
