package talk

// G.711 codec, CCITT/Sun reference algorithm (public domain), the same
// tables used by libpcma in the firmware. 8kHz mono, 20ms = 160 samples.

// PCM16ToPCMA converts s16le samples to A-law bytes.
func PCM16ToPCMA(pcm []byte) []byte {
	n := len(pcm) / 2
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		s := int16(uint16(pcm[i*2]) | uint16(pcm[i*2+1])<<8)
		out[i] = linear2alaw(s)
	}
	return out
}

// PCM16ToPCMU converts s16le samples to μ-law bytes.
func PCM16ToPCMU(pcm []byte) []byte {
	n := len(pcm) / 2
	out := make([]byte, n)
	for i := 0; i < n; i++ {
		s := int16(uint16(pcm[i*2]) | uint16(pcm[i*2+1])<<8)
		out[i] = linear2ulaw(s)
	}
	return out
}

func linear2alaw(pcmVal int16) byte {
	// u-law style segment table then A-law re-map (Sun's g711.c)
	var mask byte

	v := pcmVal
	if v >= 0 {
		mask = 0xD5 // sign (7th) bit = 1
	} else {
		mask = 0x55 // sign bit = 0
		v = -v
	}
	u := int(v)
	seg := search(u, segAEnd)
	if seg >= 8 {
		return byte(0x7F ^ mask)
	}
	segv := int(seg)
	uval := (segv << 4) | ((u >> (segv + 3)) & 0x0F)
	return byte(uval) ^ mask
}

var segAEnd = [8]int{0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF, 0x3FFF, 0x7FFF}

func search(val int, table [8]int) byte {
	for i := 0; i < 8; i++ {
		if val <= table[i] {
			return byte(i)
		}
	}
	return 8
}

func linear2ulaw(pcmVal int16) byte {
	const clip = 8159
	const bias = 0x84 << 2
	var mask byte = 0xFF
	v := int(pcmVal)
	if v < 0 {
		v = bias - v
		mask = 0x7F
	} else {
		v = bias + v
	}
	if v > clip<<2 {
		v = clip << 2
	}
	v >>= 2 // 14-bit

	seg := search(v, segUEnd)
	if seg >= 8 {
		return byte(0x7F)
	}
	uval := (int(seg) << 4) | ((v >> (int(seg) + 3)) & 0x0F)
	return byte(uval) ^ mask
}

var segUEnd = [8]int{0x3F, 0x7F, 0xFF, 0x1FF, 0x3FF, 0x7FF, 0xFFF, 0x1FFF}
