package media

import "crypto/md5"

// deviceRTSPPassword derives the KW05 RTSP password: MD5(device id) lower
// hex (validated by NVR interop).
func deviceRTSPPassword(deviceID string) string {
	h := md5.Sum([]byte(deviceID))
	const hex = "0123456789abcdef"
	out := make([]byte, 32)
	for i, b := range h {
		out[i*2] = hex[b>>4]
		out[i*2+1] = hex[b&0xF]
	}
	return string(out)
}
