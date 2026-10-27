package frame_test

import (
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

type golden struct {
	RC532Enc []struct {
		In  string `json:"in"`
		Out string `json:"out"`
	} `json:"rc532_enc"`
	RC532Roundtrip bool `json:"rc532_roundtrip"`
	RC516          []struct {
		Code1   uint32 `json:"code1"`
		Code2   uint32 `json:"code2"`
		Derived uint32 `json:"derived"`
	} `json:"rc516"`
	ListRequest struct {
		Term uint64 `json:"term"`
		Hex  string `json:"hex"`
	} `json:"list_request"`
	ListDecryptedHead string `json:"list_decrypted_head"`
	FB                struct {
		Params struct {
			UID uint64 `json:"uid"`
			C1  uint32 `json:"c1"`
			C2  uint32 `json:"c2"`
			S1  uint32 `json:"s1"`
			S2  uint32 `json:"s2"`
			S2d uint32 `json:"s2d"`
		} `json:"params"`
		DetectTickFixed string `json:"detect_tick_fixed"`
		Certify         string `json:"certify"`
		InitInfo        string `json:"init_info"`
		Heart           string `json:"heart"`
		Ack04           string `json:"ack04"`
		Ack35           string `json:"ack35"`
	} `json:"fb"`
	AlarmPayload struct {
		DevID     uint64 `json:"dev_id"`
		AlarmType uint32 `json:"alarm_type"`
		Data      string `json:"data"`
	} `json:"alarm_payload"`
	GWChksumRange64 uint32 `json:"gw_chksum_range64"`
	HashKey         struct {
		Empty128 uint32 `json:"empty128"`
		Hello    uint32 `json:"hello"`
	} `json:"hash_key"`
}

func loadGolden(t *testing.T) *golden {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "..", "testdata", "golden.json"))
	if err != nil {
		t.Fatalf("read golden: %v", err)
	}
	var g golden
	if err := json.Unmarshal(b, &g); err != nil {
		t.Fatalf("parse golden: %v", err)
	}
	return &g
}

func TestRC532Golden(t *testing.T) {
	g := loadGolden(t)
	for _, v := range g.RC532Enc {
		in, _ := hex.DecodeString(v.In)
		want, _ := hex.DecodeString(v.Out)
		c := rc5.NewRC5_32([]byte(rc5.DefaultKey))
		blk := append([]byte(nil), in...)
		c.EncryptBlock(blk)
		if hex.EncodeToString(blk) != hex.EncodeToString(want) {
			t.Fatalf("enc(%s) = %x, want %s", v.In, blk, v.Out)
		}
		c.DecryptBlock(blk)
		if hex.EncodeToString(blk) != v.In {
			t.Fatalf("roundtrip failed for %s", v.In)
		}
	}
}

func TestDeriveCode2Golden(t *testing.T) {
	g := loadGolden(t)
	for _, v := range g.RC516 {
		if got := rc5.DeriveCode2(v.Code1, v.Code2); got != v.Derived {
			t.Fatalf("DeriveCode2(%d,%d) = %d, want %d", v.Code1, v.Code2, got, v.Derived)
		}
	}
}

func TestListRequestGolden(t *testing.T) {
	g := loadGolden(t)
	got := hex.EncodeToString(frame.BuildListRequest(g.ListRequest.Term))
	if got != g.ListRequest.Hex {
		t.Fatalf("list request mismatch:\n got %s\nwant %s", got, g.ListRequest.Hex)
	}
	dec, err := frame.Decrypt(frame.BuildListRequest(g.ListRequest.Term))
	if err != nil {
		t.Fatal(err)
	}
	if hex.EncodeToString(dec[:0x18]) != g.ListDecryptedHead {
		t.Fatalf("list decrypt head mismatch: %x", dec[:0x18])
	}
}

func TestBuilderGolden(t *testing.T) {
	g := loadGolden(t)
	p := g.FB.Params
	b := frame.NewBuilder(p.UID, p.C1, p.C2, p.S1, p.S2)
	if b.S2d != p.S2d {
		t.Fatalf("S2d = %d, want %d", b.S2d, p.S2d)
	}
	cases := []struct {
		name string
		got  string
		want string
	}{
		{"detect", hex.EncodeToString(b.Detect(1761180000000)), g.FB.DetectTickFixed},
		{"certify", hex.EncodeToString(b.Certify()), g.FB.Certify},
		{"initInfo", hex.EncodeToString(b.InitInfo()), g.FB.InitInfo},
		{"heart", hex.EncodeToString(b.Heart()), g.FB.Heart},
		{"ack04", hex.EncodeToString(b.Ack(0x04)), g.FB.Ack04},
		{"ack35", hex.EncodeToString(b.Ack(0x35)), g.FB.Ack35},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Fatalf("%s mismatch:\n got %s\nwant %s", c.name, c.got, c.want)
		}
	}
}

// TestCheckLoginReplyMatchesProbe pins the 40B 0x36 app-level CheckLogin
// answer to the live-device verified layout: flags 21 00 01 00, body
// {0x12, 0×7, c1 LE, c2 LE}, chkval over the session seq space.
func TestCheckLoginReplyMatchesProbe(t *testing.T) {
	b := frame.NewBuilder(2204264224, 100000001, 200000002, 497459251, 2395831484)
	wire := b.CheckLoginReply()
	if len(wire) != 40 {
		t.Fatalf("len = %d, want 40", len(wire))
	}
	dec, err := frame.Decrypt(wire)
	if err != nil {
		t.Fatal(err)
	}
	if dec[0x14] != 0x21 {
		t.Fatalf("flags = %#x, want 0x21", dec[0x14])
	}
	if dec[0x15] != 0x00 || dec[0x16] != 0x01 || dec[0x17] != 0x00 {
		t.Fatalf("flags2/3/4 = %#x %#x %#x, want 00 01 00", dec[0x15], dec[0x16], dec[0x17])
	}
	if dec[0x18] != 0x12 {
		t.Fatalf("body[0] = %#x, want 0x12", dec[0x18])
	}
	if got := binary.LittleEndian.Uint32(dec[0x20:]); got != 100000001 {
		t.Fatalf("c1 = %d, want 100000001", got)
	}
	if got := binary.LittleEndian.Uint32(dec[0x24:]); got != 200000002 {
		t.Fatalf("c2 = %d, want 200000002", got)
	}
	p, err := frame.Parse(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !p.ChkOK {
		t.Fatal("chkval mismatch")
	}
	if p.Type != frame.TypeCheckLoginReply || p.Len != 40 || p.TermID != 2204264224 {
		t.Fatalf("parsed type=%#x len=%d term=%d", p.Type, p.Len, p.TermID)
	}
}

func TestParseRoundtrip(t *testing.T) {
	b := frame.NewBuilder(2204264224, 983944126, 2127405622, 497459251, 2395831484)
	wire := b.Certify()
	p, err := frame.Parse(wire)
	if err != nil {
		t.Fatal(err)
	}
	if !p.ChkOK {
		t.Fatal("chkval mismatch after roundtrip")
	}
	if p.Type != frame.TypeCertify || p.TermID != 2204264224 {
		t.Fatalf("parsed %+v", p)
	}
}

func TestParseAlarmGolden(t *testing.T) {
	g := loadGolden(t)
	body := make([]byte, 0x30)
	le64 := func(o int, v uint64) {
		for i := 0; i < 8; i++ {
			body[o+i] = byte(v >> (8 * i))
		}
	}
	le32 := func(o int, v uint32) {
		for i := 0; i < 4; i++ {
			body[o+i] = byte(v >> (8 * i))
		}
	}
	le16 := func(o int, v uint16) {
		for i := 0; i < 2; i++ {
			body[o+i] = byte(v >> (8 * i))
		}
	}
	le64(0x04, g.AlarmPayload.DevID)
	le32(0x18, g.AlarmPayload.AlarmType)
	le16(0x24, 3)
	copy(body[0x26:], "abc")
	a, err := frame.ParseAlarm(body)
	if err != nil {
		t.Fatal(err)
	}
	wantData, _ := hex.DecodeString(g.AlarmPayload.Data)
	if a.DevID != g.AlarmPayload.DevID || a.AlarmType != g.AlarmPayload.AlarmType || string(a.Data) != string(wantData) {
		t.Fatalf("alarm mismatch %+v", a)
	}
}

func TestGWChksumAndHashKey(t *testing.T) {
	g := loadGolden(t)
	buf := make([]byte, 64)
	for i := range buf {
		buf[i] = byte(i)
	}
	if got := rc5.GWChksum(buf); got != g.GWChksumRange64 {
		t.Fatalf("GWChksum = %#x, want %#x", got, g.GWChksumRange64)
	}
	if got := rc5.HashKey(make([]byte, 128)); got != g.HashKey.Empty128 || got != 0xD4D79C1A {
		t.Fatalf("HashKey(empty128) = %#x", got)
	}
	if got := rc5.HashKey([]byte("hello")); got != g.HashKey.Hello {
		t.Fatalf("HashKey(hello) = %#x, want %#x", got, g.HashKey.Hello)
	}
}
