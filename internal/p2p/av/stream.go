// 媒体流处理：TLV 切分（§6A.10，len 字段实测 LE）、TLV4 RC5 解密分支、
// GW AV 容器剥离（§6A.13）与 H264 Annex-B NAL 重组。
package av

import (
	"encoding/binary"

	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

// TlvStream 把 KCP 字节流切成 TLV 帧：帧可能粘连或跨包分裂，滑窗缓冲。
// len 字段锁定小端（实测 0x0034/0x0020/0x0282；早期的 BE 偏好
// 自动探测已移除）。len<4 的非法头按固件恢复路径滑动一字节。
type TlvStream struct {
	buf  []byte
	slid int
}

// Slid 返回累计滑动字节数（诊断用）。
func (s *TlvStream) Slid() int { return s.slid }

// Feed 追加字节并返回全部完整帧。
func (s *TlvStream) Feed(b []byte) []TlvFrame {
	s.buf = append(s.buf, b...)
	var out []TlvFrame
	for len(s.buf) >= 4 {
		l := int(binary.LittleEndian.Uint16(s.buf[2:4]))
		if l < 4 { // 非法长度：固件式滑动
			s.buf = s.buf[1:]
			s.slid++
			continue
		}
		if l > len(s.buf) {
			break // 半帧，等更多数据
		}
		out = append(out, TlvFrame{Typ: s.buf[0], Flags: s.buf[1], Raw: s.buf[:l]})
		s.buf = s.buf[l:]
	}
	return out
}

// DecryptTlv4 就地解密 TLV4 帧（§6A.10）：flags&1 置位时以帧首 8B 为
// RC5 key（帧首 8B），frame+8 起 8B 块解密到
// len-8。下游载荷仍取 frame[4:len]（与是否加密无关）。实测 enc=0，
// 分支按参考实现保留。
func DecryptTlv4(fr []byte) {
	if len(fr) < 24 { // [8, len-8) 内至少一块
		return
	}
	c := rc5.NewRC5_32(fr[0:8])
	for off := 8; off+8 <= len(fr)-8; off += 8 {
		c.DecryptBlock(fr[off : off+8])
	}
}

// gwAVMagic — GW AV 容器魔数（§6A.13：ff ff ff 88 | ts1 u64 | ts2 u64 |
// H264 EBSP）。
var gwAVMagic = [4]byte{0xff, 0xff, 0xff, 0x88}

// GwAVPayload 剥离 GW AV 容器头（4B 魔数 + 双 u64 时间戳 = 20B）；
// 无魔数的载荷（音频/未知形态）整段透传给字节流重组（保底）。
func GwAVPayload(p []byte) []byte {
	if len(p) >= 20 && p[0] == gwAVMagic[0] && p[1] == gwAVMagic[1] &&
		p[2] == gwAVMagic[2] && p[3] == gwAVMagic[3] {
		return p[20:]
	}
	return p
}

// findStartCode 找 b 中第一个 Annex-B 起始码；返回起始码完整起点（4B 码
// 的前导 00 归入起始码）与长度。
func findStartCode(b []byte) (pos, size int) {
	for i := 0; i+3 <= len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			if i > 0 && b[i-1] == 0 {
				return i - 1, 4
			}
			return i, 3
		}
	}
	return -1, 0
}

// AnnexStream 把 H264 EBSP 字节流重组为 NAL 单元（保留起始码）。NAL 可能
// 跨多个 GW AV 容器帧，未完成的尾部留在缓冲等待下一个起始码。
type AnnexStream struct {
	buf []byte
}

// Feed 追加字节并返回所有完整 NAL（含起始码；起始码前的垃圾字节丢弃）。
func (a *AnnexStream) Feed(b []byte) [][]byte {
	a.buf = append(a.buf, b...)
	var out [][]byte
	for {
		p, _ := findStartCode(a.buf)
		if p < 0 {
			// 无起始码：纯垃圾/未到，超限防跑飞
			if len(a.buf) > 4<<20 {
				a.buf = nil
			}
			return out
		}
		if p > 0 {
			a.buf = a.buf[p:]
		}
		q, _ := findStartCode(a.buf[3:]) // 下一个起始码（跳过当前码）
		if q < 0 {
			return out // 当前 NAL 未完整
		}
		cut := 3 + q
		out = append(out, a.buf[:cut])
		a.buf = a.buf[cut:]
	}
}

// Reset 丢弃缓冲（会话结束/重新起播）。
func (a *AnnexStream) Reset() { a.buf = nil }

// CountH264StartCodes 统计 3B（00 00 01）与 4B（00 00 00 01）起始码；
// 前导 00 计入 4B 码（诊断/测试用）。
func CountH264StartCodes(b []byte) (n3, n4 int) {
	for i := 0; i+3 <= len(b); i++ {
		if b[i] == 0 && b[i+1] == 0 && b[i+2] == 1 {
			if i > 0 && b[i-1] == 0 {
				n4++
			} else {
				n3++
			}
			i += 2
		}
	}
	return n3, n4
}
