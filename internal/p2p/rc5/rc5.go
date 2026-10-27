// Package rc5 implements the cryptographic primitives of the gute session
// layer (protocol §5).
//
// RC5-32/6/12, RC5-16/12, chkval and hash_key match the official SDK
// reference implementation and were validated against live servers;
// byte-exact golden vectors live in konx-gw/testdata/golden.json.
package rc5

import (
	"encoding/binary"
	"math/bits"
)

const (
	p32 = 0xB7E15163
	q32 = 0x9E3779B9

	p16 = 0xB7E1
	q16 = 0x9E37
)

// DefaultKey is the gute session key.
var DefaultKey = string([]byte{
	'w', 'w', 'w', '.', 'g', 'w', 'e', 'l', 'l', '.', 'c', 'c',
})

// RC5_32 is RC5 with 32-bit words, 8-byte blocks and 6 rounds (ECB, LE).
// "RC5-32/6/12" — 6 rounds, 12-byte key. S holds 2*(6+1)=14 words.
type RC5_32 struct {
	s [14]uint32
}

// NewRC5_32 expands a key of up to 12 bytes; the word count follows the
// firmware formula c = ceil(len*8/32) (12-byte key → 3 words, 8-byte → 2).
func NewRC5_32(key []byte) *RC5_32 {
	c := (len(key)*8 + 31) / 32
	if c < 1 {
		c = 1
	}
	if c > 3 {
		c = 3
	}
	L := make([]uint32, c)
	kb := make([]byte, c*4)
	copy(kb, key)
	for i := 0; i < c; i++ {
		L[i] = binary.LittleEndian.Uint32(kb[i*4:])
	}
	t := len(RC5_32{}.s)
	c32 := &RC5_32{}
	for i := 0; i < t; i++ {
		c32.s[i] = p32 + uint32(i)*q32
	}
	i, j := 0, 0
	var A, B uint32
	for n := 0; n < 3*t; n++ { // 3*max(t,c); t=14 ≥ c
		c32.s[i] = bits.RotateLeft32(c32.s[i]+A+B, 3)
		A = c32.s[i]
		L[j] = bits.RotateLeft32(L[j]+A+B, int((A+B)&31))
		B = L[j]
		i = (i + 1) % t
		j = (j + 1) % c
	}
	return c32
}

// EncryptBlock encrypts one 8-byte block.
func (r *RC5_32) EncryptBlock(b []byte) {
	A := binary.LittleEndian.Uint32(b[0:])
	B := binary.LittleEndian.Uint32(b[4:])
	A += r.s[0]
	B += r.s[1]
	for i := 1; i <= 6; i++ {
		A = bits.RotateLeft32(A^B, int(B&31)) + r.s[2*i]
		B = bits.RotateLeft32(B^A, int(A&31)) + r.s[2*i+1]
	}
	binary.LittleEndian.PutUint32(b[0:], A)
	binary.LittleEndian.PutUint32(b[4:], B)
}

// DecryptBlock decrypts one 8-byte block in place.
func (r *RC5_32) DecryptBlock(b []byte) {
	A := binary.LittleEndian.Uint32(b[0:])
	B := binary.LittleEndian.Uint32(b[4:])
	for i := 6; i >= 1; i-- {
		B = bits.RotateLeft32(B-r.s[2*i+1], -int(A&31)) ^ A
		A = bits.RotateLeft32(A-r.s[2*i], -int(B&31)) ^ B
	}
	B -= r.s[1]
	A -= r.s[0]
	binary.LittleEndian.PutUint32(b[0:], A)
	binary.LittleEndian.PutUint32(b[4:], B)
}

// RC5_16 is RC5 with 16-bit words, 4-byte blocks and 12 rounds, used only
// for the sid2 derivation transform (protocol §5.5).
type RC5_16 struct {
	s [26]uint16
}

// NewRC5_16 expands a key (LE 16-bit words).
func NewRC5_16(key []byte) *RC5_16 {
	c := (len(key)*8 + 15) / 16
	if c < 1 {
		c = 1
	}
	L := make([]uint16, c)
	kb := make([]byte, c*2)
	copy(kb, key)
	for i := 0; i < c; i++ {
		L[i] = binary.LittleEndian.Uint16(kb[i*2:])
	}
	t := len(RC5_16{}.s)
	r := &RC5_16{}
	for i := 0; i < t; i++ {
		r.s[i] = p16 + uint16(i)*q16
	}
	i, j := 0, 0
	var A, B uint16
	for n := 0; n < 3*t; n++ {
		r.s[i] = bits.RotateLeft16(r.s[i]+A+B, 3)
		A = r.s[i]
		L[j] = bits.RotateLeft16(L[j]+A+B, int((A+B)&15))
		B = L[j]
		i = (i + 1) % t
		j = (j + 1) % c
	}
	return r
}

// DecryptBlock decrypts one 4-byte block in place.
func (r *RC5_16) DecryptBlock(b []byte) {
	A := binary.LittleEndian.Uint16(b[0:])
	B := binary.LittleEndian.Uint16(b[2:])
	for i := 12; i >= 1; i-- {
		B = bits.RotateLeft16(B-r.s[2*i+1], -int(A&15)) ^ A
		A = bits.RotateLeft16(A-r.s[2*i], -int(B&15)) ^ B
	}
	B -= r.s[1]
	A -= r.s[0]
	binary.LittleEndian.PutUint16(b[0:], A)
	binary.LittleEndian.PutUint16(b[2:], B)
}

// DeriveCode2 implements p2pu_v2_rc5_dec: RC5-16-decrypt(LE(code2), key=LE(code1)).
func DeriveCode2(code1, code2 uint32) uint32 {
	kb := make([]byte, 4)
	binary.LittleEndian.PutUint32(kb, code1)
	db := make([]byte, 4)
	binary.LittleEndian.PutUint32(db, code2)
	NewRC5_16(kb).DecryptBlock(db)
	return binary.LittleEndian.Uint32(db)
}

// Chkval computes the gute frame checksum: u32 sum of the words at offsets
// 0x00/0x04/0x08/0x0C/0x14 (the field itself at 0x10 excluded).
func Chkval(frame []byte) uint32 {
	var s uint32
	for _, off := range []int{0x00, 0x04, 0x08, 0x0C, 0x14} {
		s += binary.LittleEndian.Uint32(frame[off:])
	}
	return s
}

// GWChksum is the u32 XOR checksum over whole-word prefixes (gw_chksum),
// used by sub-protocols distinct from chkval.
func GWChksum(buf []byte) uint32 {
	var chk uint32
	for i := 0; i+4 <= len(buf); i += 4 {
		chk ^= binary.LittleEndian.Uint32(buf[i:])
	}
	return chk
}

// HashKey reproduces the firmware's custom pubkey hash (init 0x4E67C6A7).
// The empty pubkey (128 zero bytes) hashes to 0xD4D79C1A.
func HashKey(data []byte) uint32 {
	h := uint32(0x4E67C6A7)
	for _, b := range data {
		h = h ^ ((h * 32) + uint32(b) + (h >> 2))
	}
	return h
}
