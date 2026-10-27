// treated 密码链：IsNeedEncrypt + EncryptGW1（MD5→4×u32 异或→mod
// 999999999→弱密码 XOR 轮换）。算法与官方 SDK 参考实现一致，真机验证。
package gwpass

import (
	"crypto/md5"
	"encoding/hex"
	"strconv"
)

// treatedMod — 弱密码轮换后的取模基数（与官方 SDK 常量一致，经
// 魔数乘 0x225c17d1>>59 实现，等价 u32 % 999999999）。
const treatedMod = 999999999

// weakTable — 弱密码表（与官方 SDK 算法一致的常量表，10×u32 LE）。
var weakTable = [10]uint32{
	0x177bce1f, 0x4208abfb, 0xbf50695e, 0x5c04bb9a, 0x13ecf425,
	0x76c479ad, 0x5b63c382, 0xac4217be, 0x8567656a, 0x568caae0,
}

// IsNeedEncrypt — 是否需要 EncryptGW1 变换（与官方 SDK 语义一致）：
// NULL/空/长度<6 或 >30 → 0；纯数字且 <10 位 → 0；纯数字 ≥10 位 → 1；
// 含非数字（且长度 6..30）→ 1。返回 1 表示需要 EncryptGW1 变换。
func IsNeedEncrypt(pwd string) int {
	if len(pwd) < 6 || len(pwd) > 0x1e {
		return 0
	}
	allDigits := true
	for i := 0; i < len(pwd); i++ {
		if pwd[i] < 0x30 || pwd[i] > 0x39 {
			allDigits = false
			break
		}
	}
	if allDigits {
		if len(pwd) < 10 {
			return 0
		}
		return 1
	}
	return 1
}

// EncryptGW1 — treated 密码变换（与官方 SDK 算法一致；中性黄金向量：
// "visitor0000000000sample42" → 387648820 / 0x171B0D34；"1234567890" →
// 970063241）：
//
//	MD5(明文) → 32 位小写 hex → 4 段 8 字符按 sscanf("%x") 解析为 u32 →
//	四段异或 → %999999999 → 最多 10 轮：若当前值取模后是"弱密码"
//	（isWeakPassword），XOR weakTable[i] 再查，直到非弱/用满。
func EncryptGW1(pwd string) uint32 {
	sum := md5.Sum([]byte(pwd))
	hexs := hex.EncodeToString(sum[:]) // "%02x" ×16 → 32 小写
	var x [4]uint32
	for i := 0; i < 4; i++ {
		v, _ := strconv.ParseUint(hexs[i*8:(i+1)*8], 16, 32) // sscanf %x
		x[i] = uint32(v)
	}
	v := x[0] ^ x[1] ^ x[2] ^ x[3]
	for i := 0; i < len(weakTable) && isWeakPassword(v%treatedMod); i++ {
		v ^= weakTable[i]
	}
	return v % treatedMod
}

// isWeakPassword — 弱密码判定（与官方 SDK 语义一致）：
// 十进制 "%u" 字符串 ≤5 位 → 弱；相邻数字差全部相等（等差序列，含全同
// 数字 d=0）→ 弱；全同字符检查（检查 A 的子集，照抄保留）；否则非弱。
func isWeakPassword(v uint32) bool {
	s := strconv.FormatUint(uint64(v), 10) // sprintf("%u")，无补零
	if len(s) <= 5 {
		return true
	}
	d := int(s[0]) - int(s[1]) // 检查 A：等差
	eq := true
	for i := 1; i+1 < len(s); i++ {
		if int(s[i])-int(s[i+1]) != d {
			eq = false
			break
		}
	}
	if eq {
		return true
	}
	if len(s) > 5 { // 检查 B：全同字符（照抄）
		for i := 0; i < len(s); i++ {
			if s[i] != s[0] {
				return false
			}
		}
		return true
	}
	return false
}

// Treated — 官方 SDK getTreatedPassword 的调用方语义：
// IsNeedEncrypt==0（数字短密码等）→ C atoi(明文) 原样返回（"123"→123，
// "85863677"→85863677）；==1 → P2PEncryptGW1。返回值即 fgP2PCall 的
// pwd(int) 参数，后续经 rc5.GwEncodePassword 进 AvStreamCtl/CALLING 帧。
func Treated(pwd string) uint32 {
	if IsNeedEncrypt(pwd) == 0 {
		return atoiPrefix(pwd)
	}
	return EncryptGW1(pwd)
}

// atoiPrefix — C atoi 语义：跳过前导空白、可选正负号、十进制前缀解析，
// 无数字返回 0，溢出按 u32 回绕（aarch64 clang 的 wmul 截断行为）。
func atoiPrefix(s string) uint32 {
	i := 0
	for i < len(s) {
		switch s[i] {
		case ' ', '\t', '\n', '\v', '\f', '\r':
			i++
			continue
		}
		break
	}
	neg := false
	if i < len(s) && (s[i] == '+' || s[i] == '-') {
		neg = s[i] == '-'
		i++
	}
	var v uint32
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		v = v*10 + uint32(s[i]-'0')
		i++
	}
	if neg {
		return -v
	}
	return v
}
