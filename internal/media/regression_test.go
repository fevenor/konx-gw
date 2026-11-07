package media

import (
	"testing"
)

// 独立画面的小 NAL、大 NAL、小 NAL 应保持连续且不重复的呈现时间。
func TestRegressionRTPFrameTimestamps(t *testing.T) {
	s := &p2pSource{pkt: h264RTPState{ts: 1000}}
	small := []byte{0x61, 1, 2, 3}
	large := make([]byte, rtpMTU+100)
	large[0] = 0x65
	a := s.packetizeLocked(small)
	b := s.packetizeLocked(large)
	c := s.packetizeLocked(small)
	got := []uint32{a[0].Timestamp, b[0].Timestamp, c[0].Timestamp}
	t.Logf("三幅画面的 RTP 时间戳：%v", got)
	if got[1]-got[0] != rtpTSStep || got[2]-got[1] != rtpTSStep {
		t.Fatalf("独立画面的时间戳应等间隔递增，得到 %v", got)
	}
}
