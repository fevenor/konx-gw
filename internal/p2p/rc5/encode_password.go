// GwEncodePassword — 官方 SDK 的密码槽变换：AvStreamCtl 帧 +0x20 槽与
// v2 CALLING 帧 +0x68 槽使用（实测验证：变换后设备 ACCEPT）。
// r = pwd；17 轮 { r = r*0xA2E39FD9 + i }（mod 2^32，i=0..16）。
package rc5

func GwEncodePassword(p uint32) uint32 {
	r := p
	for i := uint32(0); i < 17; i++ {
		r = r*0xA2E39FD9 + i
	}
	return r
}
