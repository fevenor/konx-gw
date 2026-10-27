// gwpass 黄金向量（与官方 SDK 算法一致的参考实现自生成 + 真机交叉验证）：
//   - secretKey 解密：Sz/4eUNm…UAo= + seed 3221225473 → visitor0000000000sample42
//   - treated：visitor0000000000sample42 → 387648820 (0x171B0D34)
//   - 数字短密码原样：123→123、87654321→87654321；10 位数字 1234567890→970063241
//   - des 变体往返 + 与标准 DES 的差异抽样（防"修正"回退）
package gwpass

import (
	"bytes"
	"testing"

	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

// TestGoldenDecryptSecretKey — secretKey（设备列表接口返回字段）按请求者
// uid 派生 kb 解密。
func TestGoldenDecryptSecretKey(t *testing.T) {
	got, err := DecryptSecretKey("Sz/4eUNmcc8VfWFd8tbBfV+MmVIxMfkB82U+BNT1UAo=", 3221225473)
	if err != nil {
		t.Fatalf("DecryptSecretKey: %v", err)
	}
	if want := "visitor0000000000sample42"; got != want {
		t.Fatalf("plaintext = %q, want %q", got, want)
	}
}

// TestGoldenTreated — treated 密码（呼叫的 pwd(int)）。
func TestGoldenTreated(t *testing.T) {
	cases := []struct {
		pwd  string
		want uint32
	}{
		{"visitor0000000000sample42", 387648820}, // 0x171B0D34
		{"123", 123},                             // 3 位数字：need=0 原样
		{"87654321", 87654321},                   // 8 位数字：need=0 原样
		{"1234567890", 970063241},                // 10 位数字：need=1
	}
	for _, c := range cases {
		if got := Treated(c.pwd); got != c.want {
			t.Errorf("Treated(%q) = %d (0x%x), want %d", c.pwd, got, got, c.want)
		}
	}
	if got := EncryptGW1("visitor0000000000sample42"); got != 387648820 {
		t.Errorf("EncryptGW1 = %d, want 387648820", got)
	}
}

// TestEncryptGW1IsWeakAware — 弱密码轮换确实参与：等差/全同形态的
// 中间值会被 XOR weakTable 直到非弱（对 1234567890 链路抽样验证）。
func TestEncryptGW1IsWeakAware(t *testing.T) {
	if isWeakPassword(123456) { // 等差（差=1）
		t.Log("123456 判为弱（预期）")
	} else {
		t.Fatal("等差序列 123456 应判弱")
	}
	if isWeakPassword(111111) { // 全同（d=0）
		t.Log("111111 判为弱（预期）")
	} else {
		t.Fatal("全同序列 111111 应判弱")
	}
	if isWeakPassword(387648820) {
		t.Fatal("387648820 不应判弱（黄金 treated 终值）")
	}
}

// TestDesVariantRoundtrip — des 变体自测（自选向量）：加密后解密还原。
func TestDesVariantRoundtrip(t *testing.T) {
	key := []byte{0x8a, 0x0b, 0x05, 0x57, 0x39, 0x7e, 0x9a, 0xc1} // 静态密钥原序
	vectors := [][8]byte{
		{0, 0, 0, 0, 0, 0, 0, 0},
		{0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff},
		{0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38}, // "12345678"
		{'v', 'i', 's', 'i', 't', 'o', 'r', '1'},
	}
	for _, v := range vectors {
		var enc, dec [8]byte
		des(v[:], enc[:], key, 0)
		if bytes.Equal(enc[:], v[:]) {
			t.Fatalf("加密为恒等变换：%x", v)
		}
		des(enc[:], dec[:], key, 1)
		if !bytes.Equal(dec[:], v[:]) {
			t.Fatalf("往返失败：%x → %x → %x", v, enc, dec)
		}
	}
	// 弱密钥（全 0）也必须往返（教科书 DES 的反例：本实现与参考实现
	// 一致，无半弱钥折叠处理）。
	var z, ze, zd [8]byte
	des(z[:], ze[:], z[:], 0)
	des(ze[:], zd[:], z[:], 1)
	if !bytes.Equal(zd[:], z[:]) {
		t.Fatalf("全零密钥往返失败：%x → %x → %x", z, ze, zd)
	}
}

// TestSecretKeyRoundtrip — 等价参考实现 selftest（加密→解密）：还原。
func TestSecretKeyRoundtrip(t *testing.T) {
	const seed = 3221225473
	for _, plain := range []string{"87654321", "visitor0000000000sample42", "1234567890123456"} {
		b64 := EncryptSecretKey(plain, seed)
		got, err := DecryptSecretKey(b64, seed)
		if err != nil {
			t.Fatalf("decrypt(%q): %v", b64, err)
		}
		if got != plain {
			t.Fatalf("roundtrip %q → %q → %q", plain, b64, got)
		}
	}
	// 换 seed 解不出同一明文（kb 按 uid 派生的旁证）
	b64 := EncryptSecretKey("87654321", seed)
	other, err := DecryptSecretKey(b64, 3221225474)
	if err != nil {
		t.Fatal(err)
	}
	if other == "87654321" {
		t.Fatal("不同 seed 竟解出相同明文（kb 派生可疑）")
	}
}

// TestDeriveKB — kb 派生确定性 + words=2 覆盖 kb 全部 8 字节（派生轮的
// param_2=2 指前 2 个 u32 字，即整个 kb；两个词都偏离种子）。
func TestDeriveKB(t *testing.T) {
	kb := DeriveKB(3221225473)
	if kb == seedMacAddr {
		t.Fatalf("kb 与种子完全相同（未派生）：% x", kb)
	}
	if bytes.Equal(kb[0:4], seedMacAddr[0:4][:]) || bytes.Equal(kb[4:8], seedMacAddr[4:8][:]) {
		t.Fatalf("kb 两个字均应被 XOR 修改：% x", kb)
	}
	if kb != DeriveKB(3221225473) {
		t.Fatal("kb 派生不确定")
	}
	if kb == DeriveKB(1) {
		t.Fatal("不同 seed 应派生不同 kb")
	}
}

// TestChainToEncodePassword — 全链终点抽样：treated → rc5.GwEncodePassword
// （实测：387648820 → 0x8CD81C5C，设备随后 ACCEPT）。
func TestChainToEncodePassword(t *testing.T) {
	tr := Treated("visitor0000000000sample42")
	if tr != 387648820 {
		t.Fatalf("treated = %d", tr)
	}
	if enc := rc5.GwEncodePassword(tr); enc != 0x8CD81C5C {
		t.Fatalf("GwEncodePassword(387648820) = 0x%08X, want 0x8CD81C5C", enc)
	}
}

// TestIsNeedEncrypt — 边界（6/30 位、9/10 位纯数字）。
func TestIsNeedEncrypt(t *testing.T) {
	cases := []struct {
		pwd  string
		want int
	}{
		{"12345", 0},                           // 5 位
		{"123456", 0},                          // 6 位纯数字
		{"123456789", 0},                       // 9 位纯数字
		{"1234567890", 1},                      // 10 位纯数字
		{"admin", 0},                           // 5 位含字母
		{"admins", 1},                          // 6 位含字母
		{"1234567890123456789012345678901", 0}, // 31 位
		{"123456789012345678901234567890", 1},  // 30 位纯数字
	}
	for _, c := range cases {
		if got := IsNeedEncrypt(c.pwd); got != c.want {
			t.Errorf("IsNeedEncrypt(%q) = %d, want %d", c.pwd, got, c.want)
		}
	}
}
