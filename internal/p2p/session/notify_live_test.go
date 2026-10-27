package session_test

import (
	"encoding/hex"
	"testing"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/session"
)

// CheckLogin push body, synthetic vector (layout per DESIGN §B):
// 15-byte binary header, NUL-terminated name, JSON to end.
const liveCheckLoginBody = "0000000004000000000000000b034100436865636b4c6f67696e007b22636d64223a312c2273657373696f6e4944223a3831393237393934382c2273657373696f6e494432223a313136333630363739322c224170704f53223a337d"

func TestDecodeNotifyLiveShape(t *testing.T) {
	body, _ := hex.DecodeString(liveCheckLoginBody)
	p := &frame.Parsed{Type: frame.TypeNotifyPush, Payload: append(make([]byte, 0x0C), body...)}
	n, err := frame.ParseNotify(p)
	if err != nil {
		t.Fatal(err)
	}
	d := session.DecodeNotifyForTest(n)
	if d.Name != "CheckLogin" {
		t.Fatalf("name = %q, want CheckLogin", d.Name)
	}
	if d.JSON == "" || d.JSON[0] != '{' {
		t.Fatalf("json = %q", d.JSON)
	}
}

func TestDecodeNotifyPoCShape(t *testing.T) {
	body := []byte("CheckLogin{\"cmd\":1}")
	p := &frame.Parsed{Type: frame.TypeNotifyPush, Payload: append(make([]byte, 0x0C), body...)}
	n, _ := frame.ParseNotify(p)
	d := session.DecodeNotifyForTest(n)
	if d.Name != "CheckLogin" || d.JSON != `{"cmd":1}` {
		t.Fatalf("poc shape: %+v", d)
	}
}
