// Package av 实现生产版 v2 取流管线（真机实测验证行为的移植）：
//
//	CALLING(bit5 密码绕过) → 0x3A 对称应答 → MTPRES(0x24) → meter 双向
//	72B（双侧 acks≥6 门控）→ KCP 双 conv（ctl=link_id|0x80000000、
//	file=link_id&0x7fffffff，同一 UDP 出口 demux）→ AvStreamCtl cmd=1
//	×18（350/100/70ms）→ TLV3 ACCEPT → 舞蹈（cmd=6 走 file conv、cmd=1
//	走 ctl conv）→ 设备 START(6) + TLV4 媒体（实测到 file conv）→ GW AV
//	容器剥离 → H264 EBSP → NAL 回调。
//
// 与验证工具的差异（有意为之）：TLV len 锁定 LE（实测）；TLV4 pre-START
// 帧按参考实现语义丢弃；dump/stats 换成 OnH264 回调。
package av

import (
	"encoding/binary"
	"math/rand"
)

// RandCallID — 呼叫 id 抽法（与官方 SDK 语义一致）：
// rand()<<20 | rand()<<10 | rand()，为零重试。link_id 与 AvStreamCtl 会话
// id 必须各自独立抽取（实测：session≠link）。
func RandCallID() uint32 {
	for {
		v := rand.Uint32()<<20 | rand.Uint32()<<10 | rand.Uint32()
		if v != 0 {
			return v
		}
	}
}

// ---------------------------------------------------------------------------
// mtp 封包（与官方 SDK 参考实现一致）
// ---------------------------------------------------------------------------

// 4 字节封包头（通道类型 1=NAT/2=LAN）：
//
//	byte0 = 0x88 | (L&7)    L = seglen+4（11 位总长）
//	byte1 = L >> 3
//	[2:4] = mtpChval(全帧)
//
// 12 字节封包头（通道类型 0x87 UDP-relay）：
//
//	byte0 = 0x80 | 0x10 | (L&7)（bits4-5=01 表 0x87，bit3 清零）
//	byte1 = L >> 3           L = seglen+12
//	[2:4] = mtpChval
//	[4:12] = route u64 = mtp_session term id（relay 转发键）
const (
	kcpNATHead  = 0x88
	kcpRelayFlg = 0x80 | 0x10
)

// MtpChval — mtpfrm_calc_chval @0x1365e8，实测校验（2026-10-25）：
// chval = xor_{i=0..11}(rol16(u16_le@+4+2i, i)) ^ u16_le(f[0:2])。
// 只覆盖 [4:28]，不覆盖 +0x20 之后（设备重传帧 chval 同而 counter/ts 异可证）。
func MtpChval(f []byte) uint16 {
	var chk uint16
	for i := 0; i < 12; i++ {
		w := uint16(f[4+i*2]) | uint16(f[5+i*2])<<8
		chk ^= uint16(w<<uint(i&15) | w>>uint((16-i)&15))
	}
	return chk ^ (uint16(f[0]) | uint16(f[1])<<8)
}

// WrapKcp 给 KCP 段加 4B NAT 封包头（超长返回 nil）。
func WrapKcp(seg []byte) []byte {
	L := len(seg) + 4
	if L > 0x7ff {
		return nil
	}
	f := make([]byte, L)
	copy(f[4:], seg)
	f[0] = kcpNATHead | byte(L&7)
	f[1] = byte((L & 0x7f8) >> 3)
	cv := MtpChval(f)
	f[2], f[3] = byte(cv), byte(cv>>8)
	return f
}

// WrapKcpRelay 给 KCP 段加 12B as_relay 封包头（route = term id）。
func WrapKcpRelay(seg []byte, route uint64) []byte {
	L := len(seg) + 12
	if L > 0x7ff {
		return nil
	}
	f := make([]byte, L)
	copy(f[12:], seg)
	Put64(f, 4, route)
	f[0] = kcpRelayFlg | byte(L&7)
	f[1] = byte((L & 0x7f8) >> 3)
	cv := MtpChval(f)
	f[2], f[3] = byte(cv), byte(cv>>8)
	return f
}

// IsKcpFrame — 分发位：0x80 置位（非 gute）、0x40 清零
// （非 mtpCtrl）；4B（bit3 置位）与 12B（bit3 清零、bits4-5 置位）两种形态。
func IsKcpFrame(b []byte) bool {
	return len(b) >= 4 && b[0]&0x80 != 0 && b[0]&0x40 == 0
}

// UnwrapKcp 剥封包头取 KCP 段。
func UnwrapKcp(f []byte) ([]byte, bool) {
	if len(f) < 4 {
		return nil, false
	}
	L := int(f[1])<<3 | int(f[0]&7)
	if L < 4 || L > len(f) {
		return nil, false
	}
	if f[0]&0x30 != 0 { // 12B relay 封包头
		if L < 12 {
			return nil, false
		}
		return f[12:L], true
	}
	return f[4:L], true
}

// KcpConvOf 读 KCP 段头 conv u32（LE）。
func KcpConvOf(seg []byte) uint32 {
	if len(seg) < 4 {
		return 0
	}
	return binary.LittleEndian.Uint32(seg[0:4])
}

// SegCmd：ikcp cmd 字节（81 PUSH / 82 ACK / 83 WASK / 84 WINS）。
func SegCmd(seg []byte) byte {
	if len(seg) >= 5 {
		return seg[4]
	}
	return 0
}

// ---------------------------------------------------------------------------
// 小工具
// ---------------------------------------------------------------------------

func Put16(b []byte, off int, v uint16) {
	b[off], b[off+1] = byte(v), byte(v>>8)
}

func Put32(b []byte, off int, v uint32) {
	b[off], b[off+1], b[off+2], b[off+3] = byte(v), byte(v>>8), byte(v>>16), byte(v>>24)
}

func Put64(b []byte, off int, v uint64) {
	for i := 0; i < 8; i++ {
		b[off+i] = byte(v >> (8 * i))
	}
}

// ForgeKcpPush 构造裸 ikcp PUSH 段（不经 kcp-go，测试用：对端接受 conv
// 则必然在下次 flush 回 ACK）。
func ForgeKcpPush(conv uint32, sn uint32, data []byte) []byte {
	seg := make([]byte, 24+len(data))
	Put32(seg, 0, conv)
	seg[4] = 81 // IKCP_PUSH
	Put16(seg, 6, 64)
	Put32(seg, 8, 0)
	Put32(seg, 12, sn)
	Put32(seg, 16, 0)
	Put32(seg, 20, uint32(len(data)))
	copy(seg[24:], data)
	return seg
}
