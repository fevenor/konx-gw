// meter 帧 72B 构造与解析（真机验证）。
package av

import (
	"encoding/binary"
	"time"
)

// Meter 帧常量：[0]=0xc8 [1]=0x09 [2:4]=chval [+5]=subtype(1=req/2=ack)
// [6:8]=0x0044；+0x0C u32 req=1/ack=0；+0x10/+0x18 双方 id 原样；
// +0x20 自有计数（非回显对方！）；+0x24 自有 ts；+0x28 起全零（req 尾巴
// +0x30=03 +0x34=44 [+0x3A]=02 [+0x3B]=05）。
const (
	meterLen     = 72
	meterSubtype = 5
	MeterSubReq  = 1
	MeterSubAck  = 2
)

// ClassifyMeter 识别 72B meter 族帧（0xc8 + subtype 1/2）。
func ClassifyMeter(pkt []byte) (subtype byte, ok bool) {
	if len(pkt) < 0x30 || pkt[0] != 0xc8 {
		return 0, false
	}
	if pkt[5] == MeterSubReq || pkt[5] == MeterSubAck {
		return pkt[5], true
	}
	return 0, false
}

// MeterLinkID 读 +0x08 link_id（接收方在 wire+8 校验）。
func MeterLinkID(pkt []byte) uint32 { return binary.LittleEndian.Uint32(pkt[8:12]) }

// BuildMeterReq 镜像设备 72B req 的我方视角：我方 uid@+0x18、目标设备
// id@+0x10、自有计数/ts；link_id@+0x08 必须与 CALLING 0x18 一致（设备按
// 它匹配）。chval 只覆盖 [4:28]，不覆盖 +0x20 之后。
func BuildMeterReq(uid uint64, devID, linkID, counter uint32) []byte {
	f := make([]byte, meterLen)
	f[0] = 0xc8
	f[1] = 0x09
	f[5] = MeterSubReq
	f[6] = 0x44
	Put32(f, 0x08, linkID)
	Put32(f, 0x0C, 1)
	Put32(f, 0x10, uint32(uid)) // 发起者=我们
	Put32(f, 0x18, devID)       // 目标=设备
	Put32(f, 0x20, counter)
	Put32(f, 0x24, uint32(time.Now().UnixMilli()&0xFFFFFFFF))
	Put32(f, 0x30, 3)
	Put32(f, 0x34, 0x44)
	f[0x3A] = 0x02
	f[0x3B] = 0x05
	cv := MtpChval(f)
	f[2], f[3] = byte(cv), byte(cv>>8)
	return f
}

// BuildMeterAck 镜像实测设备 ground truth：
//   - 回显 req 的 link_id/+0x10/+0x18（原样，不交换）
//   - +0x05=2（ack）、+0x0C=0（req 为 1）
//   - +0x20=我方自有递增计数（设备 ack 用自序 4,5,6，非回显 req counter！）
//   - +0x24=我方当前 ts；+0x28 起全零（req 尾巴 03/44/0205 不拷贝）
//
// 回显对方 counter 或拷贝尾巴都会被设备以 40ms 重传拒绝（实测教训）。
func BuildMeterAck(req []byte, ownCounter uint32) []byte {
	f := make([]byte, meterLen)
	f[0] = 0xc8
	f[1] = 0x09
	f[5] = MeterSubAck
	f[6], f[7] = 0x44, 0x00
	copy(f[8:12], req[8:12])           // link_id
	copy(f[0x10:0x18], req[0x10:0x18]) // 发送者 id（原样）
	copy(f[0x18:0x20], req[0x18:0x20]) // 目标 id（原样）
	Put32(f, 0x20, ownCounter)
	Put32(f, 0x24, uint32(time.Now().UnixMilli()&0xFFFFFFFF))
	cv := MtpChval(f)
	f[2], f[3] = byte(cv), byte(cv>>8)
	return f
}
