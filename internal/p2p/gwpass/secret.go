// secretKey 解密链（GW5）：kb 派生 + DES 变体 ECB + base64。
// 算法与官方 SDK 参考实现一致（EncodeKey/DecodeKey/加解密链），真机验证。
package gwpass

import (
	"encoding/base64"
	"errors"
)

// seedMacAddr — kb 派生静态种子（与官方 SDK 算法一致的常量，解密链逐字节
// 拷贝进 kb 栈缓冲；全库无写入者）。
var seedMacAddr = [8]byte{0x8d, 0x0d, 0x0a, 0x37, 0x49, 0x7c, 0x9d, 0xcd}

// desKeyStatic — DES 静态密钥（与官方 SDK 算法一致的常量），解密链把该
// 值直接作为 des 的 key 传入。
var desKeyStatic = [8]byte{0x8a, 0x0b, 0x05, 0x57, 0x39, 0x7e, 0x9a, 0xc1}

// DeriveKB — kb 派生（与官方 SDK 的 EncodeKey/DecodeKey 算法一致）：
//
//	kb 种子 = 8B 静态常量；
//	32 轮：k = k*0xDF7B0817 + i*3 + 0x98E59（u32 环绕，i=0..31）；
//	再 words 轮（GW5 链固定 words=2）：k = k*0x44087152 + i*2，
//	kb 的第 i 个 u32（小端） ^= k。
//
// seed 为会话种子：调用方传 atoi(userID)|0x80000000（官方 SDK 语义）；
// 实测访客 uid 本身已含最高位时直接传入同值。
func DeriveKB(seed uint32) [8]byte {
	kb := seedMacAddr
	k := seed
	for i := uint32(0); i < 0x20; i++ {
		k = k*0xDF7B0817 + i*3 + 0x98E59
	}
	for i := uint32(0); i < 2; i++ {
		k = k*0x44087152 + i*2
		kb[i*4+0] ^= byte(k)
		kb[i*4+1] ^= byte(k >> 8)
		kb[i*4+2] ^= byte(k >> 16)
		kb[i*4+3] ^= byte(k >> 24)
	}
	return kb
}

// DecryptSecretKey — secretKey 解密（中性黄金向量：secretKey
// "Sz/4eUNmcc8VfWFd8tbBfV+MmVIxMfkB82U+BNT1UAo=" + seed 3221225473 →
// "visitor0000000000sample42"）：
//
//	标准 base64 解码 → 逐 8B 块 先 XOR kb 后 des(解密) → 止于第一个 NUL
//	（strcpy 语义；Entryptx 以零填充，故 NUL 前即明文）。
//
// 块数 = ((v>>3)+1)*8（v=解码字节数）——v 为 8 的倍数时也比 v 多处理一块，
// 零填充参与运算，与官方 SDK 参考实现保持一致不"修正"。
func DecryptSecretKey(secretB64 string, seed uint32) (string, error) {
	raw, err := base64.StdEncoding.DecodeString(secretB64)
	if err != nil {
		return "", err
	}
	if len(raw) < 1 {
		return "", errors.New("gwpass: empty secretKey")
	}
	kb := DeriveKB(seed)
	n := ((len(raw) >> 3) + 1) * 8
	buf := make([]byte, n) // 解码结果落零填充缓冲（对应 pvVar4）
	copy(buf, raw)
	out := make([]byte, n) // 解密输出（对应 pvVar6）
	for off := 0; off < n; off += 8 {
		for j := 0; j < 8; j++ {
			buf[off+j] ^= kb[j]
		}
		des(buf[off:off+8], out[off:off+8], desKeyStatic[:], 1)
	}
	for i, b := range out {
		if b == 0 {
			return string(out[:i]), nil
		}
	}
	return string(out), nil
}

// EncryptSecretKey — DecryptSecretKey 的逆路径（明文 → base64 密文）：
// 零填充到 ((len>>3)+1)*8 字节 → 逐 8B 块 先 des(加密) 后 XOR kb → 标准
// base64。用于往返自测（等价官方 SDK 参考实现的 selftest）。
func EncryptSecretKey(plain string, seed uint32) string {
	kb := DeriveKB(seed)
	n := ((len(plain) >> 3) + 1) * 8
	in := make([]byte, n)
	copy(in, plain)
	out := make([]byte, n)
	for off := 0; off < n; off += 8 {
		des(in[off:off+8], out[off:off+8], desKeyStatic[:], 0)
		for j := 0; j < 8; j++ {
			out[off+j] ^= kb[j]
		}
	}
	return base64.StdEncoding.EncodeToString(out)
}
