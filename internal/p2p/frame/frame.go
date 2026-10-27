// Package frame implements the gute 0x7F-family wire format: encryption
// layout (protocol §5.3/§5.4), frame builders (§3.2/§4.2–§4.6) and parsing.
package frame

import (
	"encoding/binary"
	"fmt"

	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

// Frame type bytes (second header byte).
const (
	TypeDetect     = 0x01
	TypeDetectAck  = 0x02
	TypeCertify    = 0x03
	TypeCertifyAck = 0x03 // same type, 24B length distinguishes
	TypeCertifyRes = 0x04
	TypeEncKey     = 0x05
	TypeListReq    = 0x2B
	TypeListRes    = 0x2C
	TypeHeart      = 0x21
	TypeHeartAck   = 0x22
	TypeRemoteMsg  = 0x27
	TypeNotifyPush = 0x35
	// TypeCheckLoginReply is the app-level answer to the server's
	// CheckLogin push (40B shape, live-device verified).
	TypeCheckLoginReply = 0x36
	TypeInitInfo        = 0x81
)

// Header size of the encrypted region start and fixed layout offsets.
const (
	hdrClass  = 0x00
	hdrType   = 0x01
	hdrLen    = 0x02
	hdrTerm   = 0x04
	hdrSeq    = 0x0C
	hdrChk    = 0x10
	hdrFlags  = 0x14
	hdrFlags2 = 0x15
	hdrFlags3 = 0x16
	body      = 0x18
)

// HashKeyZeroPubkey is hash_key over the empty pubkey (protocol §4.3).
const HashKeyZeroPubkey = 0xD4D79C1A

// Encrypt applies the gute RC5 frame layout (flags&3==1 path):
// [0x04,0x0C) with the default key; frame-key {plain[0:4]+plain[0x14:0x18]}
// over [0x0C,0x14) and whole 8-byte blocks of [0x18,...); trailing partial
// block stays plaintext.
func Encrypt(plain []byte) []byte {
	out := make([]byte, len(plain))
	copy(out, plain)
	def := rc5.NewRC5_32([]byte(rc5.DefaultKey))
	blk := out[0x04:0x0C]
	def.EncryptBlock(blk)

	fk := make([]byte, 8)
	copy(fk, plain[0x00:0x04])
	copy(fk[4:], plain[0x14:0x18])
	fkRC := rc5.NewRC5_32(fk)

	blk = out[0x0C:0x14]
	fkRC.EncryptBlock(blk)
	n := (len(out) - body) / 8
	for i := 0; i < n; i++ {
		blk = out[body+i*8 : body+i*8+8]
		fkRC.EncryptBlock(blk)
	}
	return out
}

// Decrypt reverses Encrypt. Header [0:4) and [0x14:0x18) are always
// plaintext, so the frame key is read straight off the wire bytes.
func Decrypt(wire []byte) ([]byte, error) {
	if len(wire) < 0x18 {
		return nil, fmt.Errorf("frame too short: %d", len(wire))
	}
	out := make([]byte, len(wire))
	copy(out, wire)

	fk := make([]byte, 8)
	copy(fk, wire[0x00:0x04])
	copy(fk[4:], wire[0x14:0x18])
	fkRC := rc5.NewRC5_32(fk)

	blk := out[0x0C:0x14]
	fkRC.DecryptBlock(blk)
	n := (len(out) - body) / 8
	for i := 0; i < n; i++ {
		blk = out[body+i*8 : body+i*8+8]
		fkRC.DecryptBlock(blk)
	}
	def := rc5.NewRC5_32([]byte(rc5.DefaultKey))
	blk = out[0x04:0x0C]
	def.DecryptBlock(blk)
	return out, nil
}

// Builder produces encrypted 0x7F frames for one session, mirroring the
// Python FrameBuilder byte-for-byte (seq starts at 0 and increments).
type Builder struct {
	UID    uint64
	C1, C2 uint32
	S1, S2 uint32
	S2d    uint32
	seq    uint32
}

// NewBuilder prepares a builder and pre-derives sid2.
func NewBuilder(uid uint64, c1, c2, s1, s2 uint32) *Builder {
	return &Builder{UID: uid, C1: c1, C2: c2, S1: s1, S2: s2, S2d: rc5.DeriveCode2(s1, s2)}
}

func (b *Builder) nextSeq() uint32 { b.seq++; return b.seq }

// build lays out a frame, fills via fn, stamps chkval, encrypts.
func (b *Builder) build(ftype byte, total int, fn func([]byte)) []byte {
	f := make([]byte, total)
	f[hdrClass] = 0x7F
	f[hdrType] = ftype
	binary.LittleEndian.PutUint16(f[hdrLen:], uint16(total))
	binary.LittleEndian.PutUint64(f[hdrTerm:], b.UID)
	binary.LittleEndian.PutUint32(f[hdrSeq:], b.nextSeq())
	f[hdrFlags] = 1 // RC5 mode1
	if fn != nil {
		fn(f)
	}
	binary.LittleEndian.PutUint32(f[hdrChk:], rc5.Chkval(f))
	return Encrypt(f)
}

// Detect builds the 40B detect frame; tick is a ms timestamp.
func (b *Builder) Detect(tick uint64) []byte {
	return b.build(TypeDetect, 0x28, func(f []byte) {
		binary.LittleEndian.PutUint64(f[0x18:], tick)
		binary.LittleEndian.PutUint32(f[0x20:], b.C1)
		binary.LittleEndian.PutUint32(f[0x24:], b.C2)
	})
}

// Certify builds the 44B certify frame.
func (b *Builder) Certify() []byte {
	return b.build(TypeCertify, 0x2C, func(f []byte) {
		f[hdrFlags3] |= 0x01 // want-ack
		binary.LittleEndian.PutUint32(f[0x18:], b.C1)
		binary.LittleEndian.PutUint32(f[0x1C:], b.C2)
		binary.LittleEndian.PutUint32(f[0x20:], b.S1)
		binary.LittleEndian.PutUint32(f[0x24:], b.S2d)
		binary.LittleEndian.PutUint32(f[0x28:], HashKeyZeroPubkey)
	})
}

// InitInfo builds the 80B initInfo frame (dev_type=3, ver=6, subver=0).
func (b *Builder) InitInfo() []byte {
	return b.build(TypeInitInfo, 0x50, func(f []byte) {
		f[0x18] = 3
		f[0x19] = 6
		f[0x1A] = 0
	})
}

// Heart builds the 52B heartbeat frame.
func (b *Builder) Heart() []byte {
	return b.build(TypeHeart, 0x34, func(f []byte) {
		f[0x18] = 3
		binary.LittleEndian.PutUint32(f[0x2C:], b.C1)
		binary.LittleEndian.PutUint32(f[0x30:], b.C2)
	})
}

// Ack builds the generic 24B ACK for the given frame type.
func (b *Builder) Ack(forType byte) []byte {
	return b.build(forType, 0x18, nil)
}

// CheckLoginReply builds the 40B 0x36 app-level answer to the server's
// CheckLogin push (live-device verified): flags 21 00 01 00 and a 16B body
// {0x12, 7×0x00, c1 LE, c2 LE} — the same {c1,c2} account-credential
// trailer the CALLING frame carries. The server keeps re-pushing
// CheckLogin every 2-8s until this frame is sent.
func (b *Builder) CheckLoginReply() []byte {
	return b.build(TypeCheckLoginReply, 40, func(f []byte) {
		f[hdrFlags] = 0x21 // RC5 mode1 | captured category bits
		f[hdrFlags2] = 0x00
		f[hdrFlags3] = 0x01
		f[0x17] = 0x00
		f[0x18] = 0x12
		// +0x19..0x1F stay zero
		binary.LittleEndian.PutUint32(f[0x20:], b.C1)
		binary.LittleEndian.PutUint32(f[0x24:], b.C2)
	})
}

// BuildListRequest constructs the encrypted 0x7F2B/36B list-service request.
func BuildListRequest(termID uint64) []byte {
	f := make([]byte, 0x24)
	f[hdrClass] = 0x7F
	f[hdrType] = TypeListReq
	binary.LittleEndian.PutUint16(f[hdrLen:], 0x24)
	binary.LittleEndian.PutUint64(f[hdrTerm:], termID)
	f[hdrFlags] = 1
	binary.LittleEndian.PutUint32(f[hdrChk:], rc5.Chkval(f))
	return Encrypt(f)
}

// Parsed is a decrypted inbound frame view.
type Parsed struct {
	Type    byte
	Len     int
	TermID  uint64
	Seq     uint32
	ChkOK   bool
	Payload []byte // from 0x18
}

// Parse decrypts and validates an inbound gute frame.
func Parse(wire []byte) (*Parsed, error) {
	dec, err := Decrypt(wire)
	if err != nil {
		return nil, err
	}
	if dec[0] != 0x7F {
		return nil, fmt.Errorf("not a 0x7F frame: class=0x%02x", dec[0])
	}
	total := int(binary.LittleEndian.Uint16(dec[hdrLen:]))
	if total != len(dec) {
		return nil, fmt.Errorf("length mismatch: field=%d actual=%d", total, len(dec))
	}
	p := &Parsed{
		Type:    dec[hdrType],
		Len:     total,
		TermID:  binary.LittleEndian.Uint64(dec[hdrTerm:]),
		Seq:     binary.LittleEndian.Uint32(dec[hdrSeq:]),
		ChkOK:   binary.LittleEndian.Uint32(dec[hdrChk:]) == rc5.Chkval(dec),
		Payload: dec[body:],
	}
	return p, nil
}

// Notify is the decoded NOTIFY_MSG_SRV_PUSH_V2 application payload (§6.1).
type Notify struct {
	MsgID uint16
	Flags uint16
	SrcID uint32
	DstID uint32
	Body  []byte // message name + JSON/binary body
}

// ParseNotify decodes the push payload layout after the gute header.
func ParseNotify(p *Parsed) (*Notify, error) {
	if p.Type != TypeNotifyPush || len(p.Payload) < 0x0C {
		return nil, fmt.Errorf("not a notify payload")
	}
	n := &Notify{
		MsgID: binary.LittleEndian.Uint16(p.Payload[0x00:]),
		Flags: binary.LittleEndian.Uint16(p.Payload[0x02:]),
		SrcID: binary.LittleEndian.Uint32(p.Payload[0x04:]),
		DstID: binary.LittleEndian.Uint32(p.Payload[0x08:]),
		Body:  p.Payload[0x0C:],
	}
	return n, nil
}

// Alarm is the ALARM_MSG_FROM_SRV body layout:
// +0x04 u64 dev_id / +0x18 u32 alarm_type / +0x24 u16 data_len / +0x26 data.
type Alarm struct {
	DevID     uint64
	AlarmType uint32
	Data      []byte
}

// ParseAlarm decodes an alarm message body (received frame +0x80 per PoC).
func ParseAlarm(b []byte) (*Alarm, error) {
	if len(b) < 0x26 {
		return nil, fmt.Errorf("alarm body too short: %d", len(b))
	}
	n := int(binary.LittleEndian.Uint16(b[0x24:]))
	if n > len(b)-0x26 {
		n = len(b) - 0x26
	}
	return &Alarm{
		DevID:     binary.LittleEndian.Uint64(b[0x04:]),
		AlarmType: binary.LittleEndian.Uint32(b[0x18:]),
		Data:      b[0x26 : 0x26+n],
	}, nil
}

// SeqNext exposes the per-session sequence counter (for callers
// building custom frames with the same numbering space).
func (b *Builder) SeqNext() uint32 { b.seq++; return b.seq }
