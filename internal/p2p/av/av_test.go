// av 包测试：协议用例的生产版移植
// （TLV 锁定 LE），外加 meter ground truth golden、kcpPair 舞蹈
// 上线路由与 Caller meter 门控。
package av

import (
	"bytes"
	"context"
	"encoding/binary"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/rc5"
)

func quietLog() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// mkTlv 构造一个指定端序的 TLV 帧。
func mkTlv(typ, flags byte, payload []byte, be bool) []byte {
	f := make([]byte, 4+len(payload))
	f[0], f[1] = typ, flags
	if be {
		binary.BigEndian.PutUint16(f[2:4], uint16(len(f)))
	} else {
		binary.LittleEndian.PutUint16(f[2:4], uint16(len(f)))
	}
	copy(f[4:], payload)
	return f
}

// ---------------------------------------------------------------------------
// AvStreamCtl 52B 布局（§6A.7）
// ---------------------------------------------------------------------------

// TestAvStreamCtlLayout：52B、class 3、len 0x34、会话 id@+4、cmd@+8、
// token@0x10（cmd!=4 拷贝）、pwdEnc@0x20/chn108@0x24 仅 cmd=1。
func TestAvStreamCtlLayout(t *testing.T) {
	f := BuildAvStreamCtl(0xC0000A7B, CmdInitReq, nil, 0x39AEC90B, 0)
	if len(f) != 0x34 || f[0] != 3 {
		t.Fatalf("len=%d class=%d", len(f), f[0])
	}
	if le := binary.LittleEndian.Uint16(f[2:4]); le != 0x34 {
		t.Fatalf("len field = %#x", le)
	}
	if le := binary.LittleEndian.Uint32(f[4:8]); le != 0xC0000A7B {
		t.Fatalf("session id = %#x", le)
	}
	if le := binary.LittleEndian.Uint32(f[8:12]); le != 1 {
		t.Fatalf("cmd = %d", le)
	}
	for i, b := range f[0x10:0x20] {
		if b != 0 {
			t.Fatalf("token byte %#x at +%d not zero", b, 0x10+i)
		}
	}
	if le := binary.LittleEndian.Uint32(f[0x20:0x24]); le != 0x39AEC90B {
		t.Fatalf("pwdEnc = %#x (EncodePassword(123))", le)
	}

	tok := bytes.Repeat([]byte{0xAB}, 16)
	// cmd=4（CLOSE）：不拷 token、无 pwd 槽。
	f4 := BuildAvStreamCtl(0xC0000A7B, CmdClose, tok, 0x39AEC90B, 7)
	for i, b := range f4[0x10:0x20] {
		if b != 0 {
			t.Fatalf("cmd=4 token copied at +%d", 0x10+i)
		}
	}
	if le := binary.LittleEndian.Uint32(f4[0x20:0x24]); le != 0 {
		t.Fatalf("cmd=4 pwd slot = %#x", le)
	}
	// cmd=2（ACCEPT）：token 拷贝。
	if f2 := BuildAvStreamCtl(0xC0000A7B, CmdAccept, tok, 0, 0); !bytes.Equal(f2[0x10:0x20], tok) {
		t.Fatal("cmd=2 token not copied")
	}
	// cmd=6（舞蹈首帧，file conv）：带 token 无 pwdEnc。
	f6 := BuildAvStreamCtl(0xC0000A7B, CmdStart, tok, 0x39AEC90B, 9)
	if !bytes.Equal(f6[0x10:0x20], tok) {
		t.Fatal("cmd=6 token not copied")
	}
	if le := binary.LittleEndian.Uint32(f6[0x20:0x24]); le != 0 {
		t.Fatalf("cmd=6 pwdEnc slot = %#x", le)
	}
	if le := binary.LittleEndian.Uint32(f6[0x24:0x28]); le != 0 {
		t.Fatalf("cmd=6 chn108 slot = %#x", le)
	}
}

// TestTreatedPwdGolden：treated 密码 → GwEncodePassword 后设备
// ACCEPT（0x8CD81C5C）。
func TestTreatedPwdGolden(t *testing.T) {
	if got := rc5.GwEncodePassword(387648820); got != 0x8CD81C5C {
		t.Fatalf("GwEncodePassword(387648820) = %#x, want 0x8CD81C5C", got)
	}
}

// ---------------------------------------------------------------------------
// ctl 节奏
// ---------------------------------------------------------------------------

// TestCtlPlan：18 次发送 ⇒ 17 个间隔；剩 16 次 70ms、剩 15 次 100ms、
// 其余 350ms；总时长 ~5s。
func TestCtlPlan(t *testing.T) {
	plan := CtlPlan()
	if len(plan) != 17 {
		t.Fatalf("plan len = %d", len(plan))
	}
	if plan[1] != 70*time.Millisecond || plan[2] != 100*time.Millisecond {
		t.Fatalf("short waits = %v/%v", plan[1], plan[2])
	}
	var total time.Duration
	for i, d := range plan {
		if d != 350*time.Millisecond && i != 1 && i != 2 {
			t.Fatalf("wait[%d] = %v", i, d)
		}
		total += d
	}
	if total < 4*time.Second || total > 7*time.Second {
		t.Fatalf("span %v", total)
	}
}

// ---------------------------------------------------------------------------
// TLV 切分（LE 锁定）
// ---------------------------------------------------------------------------

// TestTlvStreamStickyLE：两帧粘连 + 跨包分裂（实测 len 字段 LE）。
func TestTlvStreamStickyLE(t *testing.T) {
	p1 := mkTlv(3, 0, make([]byte, 12), false)  // 16B
	p2 := mkTlv(4, 0, make([]byte, 300), false) // 304B
	sticky := append(append([]byte{}, p1...), p2...)

	var s TlvStream
	frs := s.Feed(sticky[:20])
	if len(frs) != 1 || frs[0].Typ != 3 || len(frs[0].Raw) != 16 {
		t.Fatalf("feed#1: %d frames", len(frs))
	}
	frs = s.Feed(sticky[20:])
	if len(frs) != 1 || frs[0].Typ != 4 || len(frs[0].Raw) != 304 {
		t.Fatalf("feed#2: %d frames", len(frs))
	}
	if len(s.buf) != 0 {
		t.Fatalf("leftover %d bytes", len(s.buf))
	}
}

// TestTlvStreamIllegalLen：len<4 非法头按固件路径滑动一字节、不崩溃
// （注：与参考实现/固件一致，仅 len<4 触发滑动；大 len 垃圾头按“更大的半帧”
// 等待——KCP 字节流在序，生产中不存在乱序垃圾）。
func TestTlvStreamIllegalLen(t *testing.T) {
	var s TlvStream
	frs := s.Feed([]byte{0x03, 0x00, 0x01, 0x00, 0x99, 0x99})
	if len(frs) != 0 {
		t.Fatalf("illegal-len head must not yield frames: %d", len(frs))
	}
	if s.Slid() == 0 {
		t.Fatal("len<4 must slide")
	}
}

// TestTlvFrameCmd：cmd@帧+0x08、REJECT code@帧+0x0C（实测教训）。
func TestTlvFrameCmd(t *testing.T) {
	payload := make([]byte, 12)
	binary.LittleEndian.PutUint32(payload[4:], 6) // START @ frame+8
	fr := TlvFrame{Typ: 3, Raw: mkTlv(3, 0, payload, false)}
	if cmd, ok := fr.Cmd(); !ok || cmd != 6 {
		t.Fatalf("cmd=%d ok=%v", cmd, ok)
	}
	short := TlvFrame{Typ: 3, Raw: mkTlv(3, 0, make([]byte, 4), false)}
	if _, ok := short.Cmd(); ok {
		t.Fatal("short frame must not yield cmd")
	}
	rej := make([]byte, 12)
	binary.LittleEndian.PutUint32(rej[4:], 3)
	binary.LittleEndian.PutUint32(rej[8:], 1)
	frRej := TlvFrame{Typ: 3, Raw: mkTlv(3, 0, rej, false)}
	if cmd, _ := frRej.Cmd(); cmd != 3 {
		t.Fatalf("reject cmd=%d", cmd)
	}
	if code, ok := frRej.RejectCode(); !ok || code != 1 {
		t.Fatalf("reject code=%d ok=%v", code, ok)
	}
	if reset, ok := RejectReset(1); !ok || reset != 0xC {
		t.Fatalf("reset mapping = %x", reset)
	}
}

// ---------------------------------------------------------------------------
// TLV4 RC5 解密
// ---------------------------------------------------------------------------

// TestDecryptTlv4Roundtrip：key=帧首 8B 镜像加密 [8, len-8) 块区，解密还原。
func TestDecryptTlv4Roundtrip(t *testing.T) {
	media := make([]byte, 60)
	for i := range media {
		media[i] = byte(i*37 + 11)
	}
	f := mkTlv(4, 1, media, false) // 64B
	orig := append([]byte(nil), f...)

	c := rc5.NewRC5_32(f[0:8])
	for off := 8; off+8 <= len(f)-8; off += 8 {
		c.EncryptBlock(f[off : off+8])
	}
	if bytes.Equal(f, orig) {
		t.Fatal("encryption no-op")
	}
	DecryptTlv4(f)
	if !bytes.Equal(f, orig) {
		t.Fatal("roundtrip mismatch")
	}
	tiny := mkTlv(4, 1, make([]byte, 8), false)
	dup := append([]byte(nil), tiny...)
	DecryptTlv4(tiny)
	if !bytes.Equal(tiny, dup) {
		t.Fatal("tiny frame touched")
	}
}

// ---------------------------------------------------------------------------
// GW AV 容器与 H264 NAL 重组（§6A.13）
// ---------------------------------------------------------------------------

func gwAVFrame(payload []byte, ts1 uint64) []byte {
	f := make([]byte, 20+len(payload))
	f[0], f[1], f[2], f[3] = 0xff, 0xff, 0xff, 0x88
	binary.LittleEndian.PutUint64(f[4:], ts1)
	binary.LittleEndian.PutUint64(f[12:], ts1+1)
	copy(f[20:], payload)
	return f
}

// TestGwAVSplit：容器剥离 + NAL 跨容器重组 + 起始码保留 + 前导垃圾丢弃。
func TestGwAVSplit(t *testing.T) {
	sps := append([]byte{0, 0, 0, 1}, []byte{0x67, 0x64, 0x00, 0x1e}...) // SPS NAL
	idrTail := bytes.Repeat([]byte{0x65}, 3000)                          // 大 IDR 载荷
	idr := append(append([]byte{}, []byte{0, 0, 0, 1, 0x65}...), idrTail...)
	pframe := append([]byte{0, 0, 1}, []byte{0x41, 0x9a}...) // sc3 P 帧

	var a AnnexStream
	var got [][]byte
	// IDR 拆到两个容器帧（模拟 642B 分片），前置垃圾字节
	got = append(got, a.Feed(append([]byte{0xde, 0xad}, GwAVPayload(gwAVFrame(idr[:1500], 1))...))...)
	got = append(got, a.Feed(GwAVPayload(gwAVFrame(idr[1500:], 2)))...)
	got = append(got, a.Feed(GwAVPayload(gwAVFrame(sps, 3)))...)
	got = append(got, a.Feed(GwAVPayload(gwAVFrame(pframe, 4)))...)
	got = append(got, a.Feed(GwAVPayload(gwAVFrame(sps, 5)))...) // 边界驱动：末 NAL 以下一起始码为界

	if len(got) != 3 {
		t.Fatalf("NALs = %d, want 3 (idr/sps/p)", len(got))
	}
	if !bytes.Equal(got[0], idr) {
		t.Fatalf("idr mismatch: %d vs %d bytes", len(got[0]), len(idr))
	}
	if !bytes.Equal(got[1], sps) {
		t.Fatal("sps mismatch")
	}
	if !bytes.Equal(got[2], pframe) {
		t.Fatal("p-frame mismatch（sc3 起始码应保留）")
	}
	n3, n4 := CountH264StartCodes(idr)
	if n4 != 1 || n3 != 0 {
		t.Fatalf("sc4=%d sc3=%d", n4, n3)
	}
}

// TestGwAVPayloadPassthrough：无魔数载荷整段透传（音频/未知形态保底）。
func TestGwAVPayloadPassthrough(t *testing.T) {
	raw := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}
	if got := GwAVPayload(raw); !bytes.Equal(got, raw) {
		t.Fatal("non-container payload must pass through")
	}
	if got := GwAVPayload(gwAVFrame([]byte("xyz"), 7)); string(got) != "xyz" {
		t.Fatalf("container strip = %q", got)
	}
}

// ---------------------------------------------------------------------------
// meter golden（实测 ground truth）
// ---------------------------------------------------------------------------

// deviceMeterReq 按实测布局构造设备 meter_req。
func deviceMeterReq(linkID, devID, uid, counter uint32) []byte {
	f := make([]byte, 72)
	f[0], f[1] = 0xc8, 0x09
	f[5] = 1
	f[6] = 0x44
	Put32(f, 8, linkID)
	Put32(f, 0x0C, 1)
	Put32(f, 0x10, devID)
	Put32(f, 0x18, uid)
	Put32(f, 0x20, counter)
	Put32(f, 0x24, 0x0464499c)
	Put32(f, 0x30, 3)
	Put32(f, 0x34, 0x44)
	f[0x3A] = 0x02
	f[0x3B] = 0x05
	Put32(f, 0x3C, 0x65496404)
	cv := MtpChval(f)
	f[2], f[3] = byte(cv), byte(cv>>8)
	return f
}

// TestMeterGolden：req 布局 + ack 镜像规则 + chval 覆盖域。
func TestMeterGolden(t *testing.T) {
	const (
		linkID, devID, uid, ctr = 0x44332211, 10000001, 0x573A1089, 5
	)
	req := deviceMeterReq(linkID, devID, uid, ctr)

	// req 布局断言（实测 ground truth）
	if len(req) != 72 || req[0] != 0xc8 || req[1] != 0x09 || req[5] != 1 || req[6] != 0x44 {
		t.Fatalf("req head: % x", req[:8])
	}
	if got := binary.LittleEndian.Uint32(req[8:12]); got != linkID {
		t.Fatalf("link_id@+8 = %#x", got)
	}
	if got := binary.LittleEndian.Uint32(req[0x10:0x14]); got != devID {
		t.Fatalf("dev@+0x10 = %#x", got)
	}
	if got := binary.LittleEndian.Uint32(req[0x18:0x1C]); got != uid {
		t.Fatalf("uid@+0x18 = %#x", got)
	}
	if req[0x30] != 3 || req[0x34] != 0x44 || req[0x3A] != 2 || req[0x3B] != 5 {
		t.Fatalf("req tail: % x", req[0x30:])
	}
	// chval 覆盖域：只覆盖 [4:28]，改动 +0x20 计数不影响 chval
	cvBefore := MtpChval(req)
	req2 := append([]byte(nil), req...)
	Put32(req2, 0x20, 999)
	if MtpChval(req2) != cvBefore {
		t.Fatal("chval must not cover +0x20 之后")
	}

	// ack 镜像规则
	ack := BuildMeterAck(req, 4)
	if len(ack) != 72 || ack[5] != 2 {
		t.Fatalf("ack head: % x", ack[:8])
	}
	if got := binary.LittleEndian.Uint32(ack[0x0C:0x10]); got != 0 {
		t.Fatalf("ack +0x0C = %d, want 0", got)
	}
	if !bytes.Equal(ack[8:12], req[8:12]) || !bytes.Equal(ack[0x10:0x20], req[0x10:0x20]) {
		t.Fatal("ack must echo link_id/+0x10/+0x18")
	}
	if got := binary.LittleEndian.Uint32(ack[0x20:0x24]); got != 4 {
		t.Fatalf("ack own counter = %d（非回显 req counter=%d）", got, ctr)
	}
	for i, b := range ack[0x28:] {
		if b != 0 {
			t.Fatalf("ack +0x28 起必须全零（+%#x=%#x，req 尾巴不拷贝）", 0x28+i, b)
		}
	}
	if cv := MtpChval(ack); cv != binary.LittleEndian.Uint16(ack[2:4]) {
		t.Fatal("ack chval mismatch")
	}

	// 我方 req：uid@+0x18=发起者、dev@+0x10=目标（与设备帧角色互换）
	our := BuildMeterReq(uint64(uid), devID, linkID, 1)
	if got := binary.LittleEndian.Uint32(our[0x10:0x14]); got != uid {
		t.Fatalf("our req +0x10 = %#x, want uid", got)
	}
	if got := binary.LittleEndian.Uint32(our[0x18:0x1C]); got != devID {
		t.Fatalf("our req +0x18 = %#x, want devID", got)
	}
	if sub, ok := ClassifyMeter(our); !ok || sub != 1 {
		t.Fatalf("classify our req: %d %v", sub, ok)
	}
	if sub, ok := ClassifyMeter(ack); !ok || sub != 2 {
		t.Fatalf("classify ack: %d %v", sub, ok)
	}
}

// ---------------------------------------------------------------------------
// ACCEPT 舞蹈（顺序/一次性/回显）
// ---------------------------------------------------------------------------

func mkAccept(echo uint32) TlvFrame {
	p := make([]byte, 0x1C)
	binary.LittleEndian.PutUint32(p[0:4], echo)
	binary.LittleEndian.PutUint32(p[4:8], CmdAccept)
	copy(p[0xC:], bytes.Repeat([]byte{0x5A}, 16))
	return TlvFrame{Typ: 3, Raw: mkTlv(3, 0, p, false)}
}

// TestAcceptDanceOrder：先 cmd=6（file conv）再 cmd=1（ctl conv）；
// 一次性；回显不符仅告警照跳；16B 块记录。
func TestAcceptDanceOrder(t *testing.T) {
	d := &acceptDance{sid: 0xC0000A7B}
	var mu sync.Mutex
	var seq []uint32
	fired := make(chan struct{})
	send := func(cmd uint32) error {
		mu.Lock()
		seq = append(seq, cmd)
		n := len(seq)
		mu.Unlock()
		if n == 2 {
			close(fired)
		}
		return nil
	}
	fr := mkAccept(0xC0000A7B)
	if !d.observe(fr, nil) {
		t.Fatal("first ACCEPT must fire")
	}
	d.fire(send, nil)
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatal("dance did not complete")
	}
	mu.Lock()
	if len(seq) != 2 || seq[0] != 6 || seq[1] != 1 {
		mu.Unlock()
		t.Fatalf("dance sequence = %v, want [6 1]", seq)
	}
	mu.Unlock()
	if !d.EchoOK || !bytes.Equal(d.Block, bytes.Repeat([]byte{0x5A}, 16)) {
		t.Fatalf("echo=%v block=% x", d.EchoOK, d.Block)
	}
	// 重复 ACCEPT 不重跳
	if d.observe(mkAccept(0xC0000A7B), nil) {
		t.Fatal("duplicate ACCEPT must not re-fire")
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	defer mu.Unlock()
	if len(seq) != 2 {
		t.Fatalf("dance re-fired: %v", seq)
	}

	// 回显不符：照跳
	d2 := &acceptDance{sid: 0x11111111}
	if !d2.observe(mkAccept(0xDEADBEEF), nil) {
		t.Fatal("mismatch echo must still fire")
	}
	if d2.EchoOK {
		t.Fatal("mismatch echo must not validate")
	}
}

// ---------------------------------------------------------------------------
// KCP demux + 舞蹈上线路由
// ---------------------------------------------------------------------------

// TestKcpDemuxRoutesByConv：两个 conv 的假段喂分流器，各到各会话队列。
func TestKcpDemuxRoutesByConv(t *testing.T) {
	eg := newKcpEgress(nilTransport{})
	ctlAd := newMtpKcpConn(eg)
	fileAd := newMtpKcpConn(eg)
	d := newKcpDemux()
	const ctlConv, fileConv = 0x80030001, 0x00030001
	d.addFlow(ctlConv, ctlAd)
	d.addFlow(fileConv, fileAd)
	from, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:9999")
	if !d.feed(ForgeKcpPush(ctlConv, 1, []byte("ctl-media")), from) {
		t.Fatal("ctl conv not routed")
	}
	if !d.feed(ForgeKcpPush(fileConv, 2, []byte("file-conv")), from) {
		t.Fatal("file conv not routed")
	}
	if d.feed(ForgeKcpPush(0xDEADBEEF, 3, []byte("stray")), from) {
		t.Fatal("unregistered conv must drop")
	}
	readOne := func(ad *mtpKcpConn, want string) {
		t.Helper()
		_ = ad.SetReadDeadline(time.Now().Add(time.Second))
		p := make([]byte, 1500)
		n, _, err := ad.ReadFrom(p)
		if err != nil {
			t.Fatalf("read %s: %v", want, err)
		}
		if got := string(p[24:n]); got != want { // 24B ikcp 头后是 payload
			t.Fatalf("payload = %q, want %q", got, want)
		}
		select {
		case f := <-ad.ch:
			t.Fatalf("unexpected extra %q", string(f.data[24:]))
		default:
		}
	}
	readOne(ctlAd, "ctl-media")
	readOne(fileAd, "file-conv")
}

// nilTransport 丢弃一切发送（demux 测试无需真实出口）。
type nilTransport struct{}

func (nilTransport) Send([]byte) error                 { return nil }
func (nilTransport) SendTo([]byte, *net.UDPAddr) error { return nil }

// udpTapTransport 把 KCP 出向包真发到目标（loopback 上线测试）。
type udpTapTransport struct{ c *net.UDPConn }

func (u udpTapTransport) Send(b []byte) error { return nil }
func (u udpTapTransport) SendTo(b []byte, a *net.UDPAddr) error {
	_, err := u.c.WriteToUDP(b, a)
	return err
}

// TestDanceWireRouting：舞蹈帧上线——cmd=6 经 file conv、cmd=1 经 ctl
// conv 的 KCP PUSH 到达“设备”侧；p2pc_send_av_stream_ctl_frm 路由。
func TestDanceWireRouting(t *testing.T) {
	dev, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer dev.Close()
	ourSock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer ourSock.Close()

	const (
		linkID    = uint32(0x00030001)
		sessionID = uint32(0xC0000A7B)
		pwdEnc    = uint32(0x39AEC90B)
	)
	ctlConv, fileConv := linkID|0x80000000, linkID&0x7fffffff
	tok := bytes.Repeat([]byte{0xAB}, 16)

	kp := startKcpPair(udpTapTransport{ourSock}, dev.LocalAddr().(*net.UDPAddr),
		linkID, 10000001, 1000, func([]byte) {}, quietLog())
	if kp == nil || kp.file == nil {
		t.Fatal("kcp pair start failed")
	}
	defer kp.close()

	if err := kp.danceSend(sessionID, CmdStart, tok, pwdEnc); err != nil { // cmd=6 → file
		t.Fatalf("dance 6: %v", err)
	}
	if err := kp.danceSend(sessionID, CmdInitReq, tok, pwdEnc); err != nil { // cmd=1 → ctl
		t.Fatalf("dance 1: %v", err)
	}

	got := map[uint32][][]byte{}
	deadline := time.Now().Add(2 * time.Second)
	_ = dev.SetReadDeadline(deadline)
	buf := make([]byte, 2048)
	for time.Now().Before(deadline) {
		n, _, err := dev.ReadFromUDP(buf)
		if err != nil {
			break
		}
		f := append([]byte(nil), buf[:n]...)
		if !IsKcpFrame(f) {
			continue
		}
		seg, ok := UnwrapKcp(f)
		if !ok || SegCmd(seg) != 81 { // 只看 PUSH
			continue
		}
		conv := KcpConvOf(seg)
		if conv != ctlConv && conv != fileConv {
			continue
		}
		got[conv] = append(got[conv], seg[24:])
	}
	has := func(set [][]byte, want []byte) bool {
		for _, p := range set {
			if bytes.Equal(p, want) {
				return true
			}
		}
		return false
	}
	if !has(got[fileConv], BuildAvStreamCtl(sessionID, CmdStart, tok, pwdEnc, 0)) {
		t.Fatalf("file conv %#x 未收到 cmd=6 帧: %v", fileConv, got)
	}
	if !has(got[ctlConv], BuildAvStreamCtl(sessionID, CmdInitReq, tok, pwdEnc, 0)) {
		t.Fatalf("ctl conv %#x 未收到 cmd=1 帧: %v", ctlConv, got)
	}
}

// ---------------------------------------------------------------------------
// Caller：meter 门控 → KCP 建立 → CLOSE 挂断
// ---------------------------------------------------------------------------

// fakeTransport 记录出向包；入向由测试直接喂 Caller.tap。
type fakeTransport struct {
	mu    sync.Mutex
	sent  [][]byte
	addrs []*net.UDPAddr
}

func (f *fakeTransport) Send(b []byte) error {
	f.mu.Lock()
	f.sent = append(f.sent, append([]byte(nil), b...))
	f.mu.Unlock()
	return nil
}
func (f *fakeTransport) SendTo(b []byte, a *net.UDPAddr) error {
	f.mu.Lock()
	f.sent = append(f.sent, append([]byte(nil), b...))
	f.addrs = append(f.addrs, a)
	f.mu.Unlock()
	return nil
}
func (f *fakeTransport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.sent)
}

// TestCallerMeterGateAndTeardown：喂 6+ 设备 req 与 dev ack → transfer/ctl
// 态（KCP 对建立、AvStreamCtl 循环开跑）；Stop 后 closed。
func TestCallerMeterGateAndTeardown(t *testing.T) {
	tr := &fakeTransport{}
	c := New(10000001, 3221225473, 387648820, Options{
		Transport: tr, Creds: CloudCreds{C1: 1, C2: 2, S1: 3, S2: 4},
		Log: quietLog(), MeterTickEvery: 10 * time.Millisecond,
	})
	states := make(chan State, 32)
	c.OnState(func(s State, _ error) { states <- s })
	if _, sid := c.IDs(); sid == 0 {
		t.Fatal("session id zero")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := c.Start(ctx); err != nil {
		t.Fatal(err)
	}
	linkID, _ := c.IDs()
	from, _ := net.ResolveUDPAddr("udp4", "127.0.0.1:55555")

	// 设备 meter_req ×7（我方应逐个回 ack）
	for i := 1; i <= 7; i++ {
		c.Tap()(deviceMeterReq(linkID, 10000001, 3221225473, uint32(i)), from)
	}
	// 设备 ack ×6（单侧门控保底）
	ack := deviceMeterReq(linkID, 10000001, 3221225473, 1)
	ack[5] = 2
	Put32(ack, 0x0C, 0)
	for i := 0; i < 6; i++ {
		c.Tap()(ack, from)
	}

	// 等 transfer/ctl 态
	deadline := time.Now().Add(3 * time.Second)
	seen := map[State]bool{}
	for !seen[StateCtl] && time.Now().Before(deadline) {
		select {
		case s := <-states:
			seen[s] = true
		case <-time.After(50 * time.Millisecond):
		}
	}
	if !seen[StateTransfer] || !seen[StateCtl] {
		t.Fatalf("gate did not open: states=%v", seen)
	}
	// 我方 ack 镜像了 7 个设备 req
	if n := tr.count(); n < 7 {
		t.Fatalf("acks sent = %d, want ≥7", n)
	}
	// ctl 循环在跑（AvStreamCtl 经 KCP 出口发送，包数增长）
	n1 := tr.count()
	time.Sleep(200 * time.Millisecond)
	if tr.count() <= n1 {
		t.Fatal("AvStreamCtl cycle not sending")
	}

	c.Stop()
	if c.State() != StateClosed {
		t.Fatalf("state after Stop = %s", c.State())
	}
}

// TestRandCallID：非零、独立。
func TestRandCallID(t *testing.T) {
	if RandCallID() == 0 {
		t.Fatal("zero id")
	}
	if RandCallID() == RandCallID() {
		t.Fatal("consecutive draws identical")
	}
}
