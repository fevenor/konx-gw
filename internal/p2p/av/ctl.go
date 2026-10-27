// AvStreamCtl（class=3/52B，与官方 SDK 参考实现 byte-exact）与
// 重发节奏、ACCEPT 舞蹈。
package av

import (
	"encoding/binary"
	"time"
)

// AvStreamCtl cmd 集（按帧 +0x08 分发）。
const (
	CmdInitReq uint32 = 1 // 起播（呼叫）
	CmdAccept  uint32 = 2
	CmdReject  uint32 = 3
	CmdClose   uint32 = 4
	CmdStart   uint32 = 6
)

// BuildAvStreamCtl — AvStreamCtl 初始化帧（52B）：
//
//	memset(0,0x34); [0]=3; [2:4]=0x34; [4]=会话 id; [8]=cmd;
//	cmd!=4 → token(16B)@0x10；cmd==1 → [0x20]=EncodePassword(pwd)、
//	[0x24]=chn+0x108（v2 无 uTCP=0）。
func BuildAvStreamCtl(sessionID, cmd uint32, token []byte, pwdEnc, chn108 uint32) []byte {
	f := make([]byte, 0x34)
	f[0] = 3
	Put32(f, 4, sessionID)
	Put32(f, 8, cmd)
	if cmd != CmdClose && len(token) >= 16 {
		copy(f[0x10:0x20], token[:16])
	}
	if cmd == CmdInitReq {
		Put32(f, 0x20, pwdEnc)
		Put32(f, 0x24, chn108)
	}
	Put16(f, 2, 0x34)
	return f
}

// CtlPlan — 18 次重发间隔（与官方 SDK 参考实现一致）：计数从
// 18 起，常规 350ms、剩 16 次 70ms、剩 15 次 100ms（17 个间隔）。
func CtlPlan() []time.Duration {
	plan := make([]time.Duration, 0, 17)
	for r := 17; r >= 1; r-- { // r = 每次发送后剩余次数
		d := 350 * time.Millisecond
		switch r {
		case 16:
			d = 70 * time.Millisecond
		case 15:
			d = 100 * time.Millisecond
		}
		plan = append(plan, d)
	}
	return plan
}

// TlvFrame 是一个 [type u8][flags u8][len u16 LE][payload] 帧；len 含 4B
// 头、<4 非法。raw 保留整帧；TLV4 媒体载荷 = raw[4:]。
type TlvFrame struct {
	Typ, Flags byte
	Raw        []byte
}

func (f TlvFrame) Payload() []byte { return f.Raw[4:] }

// Cmd 读 TLV3 应答的 FRAME+0x08 cmd（实测教训：payload+8 会把 REJECT
// code 误读成 cmd；FRAME+0x0C 才是 REJECT code）。
func (f TlvFrame) Cmd() (uint32, bool) {
	if len(f.Raw) < 12 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(f.Raw[8:12]), true
}

// RejectCode 读 FRAME+0x0C 的 REJECT code（app 侧 reset 映射 1→0xC
// 2→0xD 3→0xF 4→5 7→0x12）。
func (f TlvFrame) RejectCode() (uint32, bool) {
	if len(f.Raw) < 16 {
		return 0, false
	}
	return binary.LittleEndian.Uint32(f.Raw[12:16]), true
}

// RejectReset 把 REJECT code 映射为 app 侧 reset 码（官方 SDK REJECT 处理语义）。
func RejectReset(code uint32) (uint32, bool) {
	m := map[uint32]uint32{1: 0xC, 2: 0xD, 3: 0xF, 4: 5, 7: 0x12}
	v, ok := m[code]
	return v, ok
}

// acceptDancer — ACCEPT 处理（与官方 SDK 参考实现语义一致）：
//
//	(a) 校验帧+4（= payload+0）sessionID 回显 == 我方会话 id（≠link_id）；
//	    不符仅告警，舞蹈照跳。
//	(b) 帧+0x10（= payload+0xC）16B 记录（参考实现仅记录
//	    不回发——发送 token 仍用会话侧的 pdwPrm 块）。
//	(c) AvStreamCtl(cmd=6)：经 file conv。
//	(d) AvStreamCtl(cmd=1)：经 ctl conv 再发一次。
//
// 一次性（fired 防重），绕过 CtlPlan 节流——18 次重发循环在 ACCEPT 后
// 已停止。之后设备回 START(6) 开媒体门。
type acceptDance struct {
	sid    uint32
	fired  bool
	EchoOK bool
	Block  []byte // ACCEPT 帧+0x10 起 16B（chn+0x15c 语义，仅记录）
}

// observe 记录 (a)(b) 并做一次性判定；返回 true 表示应触发 (c)(d)。
// 调用方负责串行化（Caller.mu）。
func (d *acceptDance) observe(fr TlvFrame, warn func(string, ...any)) bool {
	if len(fr.Raw) >= 8 {
		if echo := binary.LittleEndian.Uint32(fr.Raw[4:8]); echo != d.sid {
			if warn != nil {
				warn("ACCEPT sessionID echo mismatch (dance continues)",
					"echo", echo, "ours", d.sid)
			}
		} else {
			d.EchoOK = true
		}
	}
	if len(fr.Raw) >= 0x20 {
		d.Block = append([]byte(nil), fr.Raw[0x10:0x20]...)
	}
	if d.fired {
		return false
	}
	d.fired = true
	return true
}

// fire 按 (c)(d) 顺序发送舞蹈帧：cmd=6 → file conv，cmd=1 → ctl conv。
func (d *acceptDance) fire(send func(cmd uint32) error, warn func(string, ...any)) {
	go func() {
		for _, cmd := range []uint32{CmdStart, CmdInitReq} { // 6 先行（file），1 随后（ctl）
			if err := send(cmd); err != nil && warn != nil {
				warn("ACCEPT dance send failed", "cmd", cmd, "err", err)
			}
		}
	}()
}
