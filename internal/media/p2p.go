// p2p-av 媒体源：经 internal/p2p/av 呼叫取流，H264 EBSP → RTP 打包
// （与 rtspSource 相同的 source 契约）。
package media

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/p2p/av"
)

// P2PCaller 是 media 侧对一路 AV 呼叫的抽象（生产实现 *av.Caller，由
// session.AttachAV 建立并启动）。
type P2PCaller interface {
	OnH264(fn func(nal []byte))
	OnState(fn func(av.State, error))
	State() av.State
	Stop()
	// Pause/Resume：会话保持（media.p2p.session_hold）——观众离开暂停取流
	// 不拆链（KCP/meter 存活），新观众在同一条活链路上重发 AvStreamCtl
	// cmd=1 复活媒体（真机实测设备会再次 ACCEPT；完整拆链后重呼
	// 会被服务器节流 15-30 分钟）。
	Pause()
	Resume()
}

// P2PDialer 建立并启动一路呼叫（app 注入：session.AttachAV 包装）。
type P2PDialer func(devID uint32, treatedPwd int) (P2PCaller, error)

// P2PPwdResolver 解析设备 treated 密码（app 注入：cloud LoadDeviceInfo
// → secretKey → gwpass.DecryptSecretKey(secretKey, uid) → gwpass.Treated）。
type P2PPwdResolver func(devID uint32) (int, error)

const (
	rtpPT       = 96
	rtpMTU      = 1400
	rtpTSStep   = 3600 // 90kHz/25fps（门铃低帧率档的自管理步进）
	fuIndicator = 28   // FU-A NAL 类型
)

// p2pSource 驱动一路 AV 呼叫并打包 H264。首次收到 SPS+PPS 后合成
// description.Session（format.H264，sprop 由 SPS/PPS 字段派生）并转 Active。
type p2pSource struct {
	devID      uint32
	treatedPwd int
	dial       P2PDialer
	log        *slog.Logger

	mu      sync.Mutex
	sink    func(*description.Media, *rtp.Packet)
	stateCh chan SourceState
	cancel  context.CancelFunc
	done    chan struct{}
	running atomic.Bool // 重拨循环存活标记：Start 幂等（暂停/退避中不重复起）

	desc   *description.Session
	media  *description.Media
	call   P2PCaller
	sps    []byte
	pps    []byte
	pkt    h264RTPState
	nals   int
	closed chan struct{}
}

// h264RTPState 自管理 RTP 序列化状态（seq/ts/ssrc 随机起点）。
type h264RTPState struct {
	seq  uint16
	ts   uint32
	ssrc uint32
}

func newP2PSource(devID uint32, treatedPwd int, dial P2PDialer, log *slog.Logger) *p2pSource {
	return &p2pSource{
		devID:      devID,
		treatedPwd: treatedPwd,
		dial:       dial,
		log:        log,
		stateCh:    make(chan SourceState, 16),
		done:       make(chan struct{}),
		closed:     make(chan struct{}),
		pkt: h264RTPState{
			seq:  uint16(rand.Uint32()),
			ts:   rand.Uint32(),
			ssrc: rand.Uint32(),
		},
	}
}

func (s *p2pSource) States() <-chan SourceState { return s.stateCh }

func (s *p2pSource) Desc() *description.Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.desc
}

func (s *p2pSource) SetSink(fn func(*description.Media, *rtp.Packet)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sink = fn
}

func (s *p2pSource) emit(st State, err error) {
	select {
	case s.stateCh <- SourceState{State: st, Err: err}:
	default:
	}
}

// Start 启动呼叫循环（dial 内部已 Start 呼叫；断流退避重拨）。幂等：
// 循环已在跑（session_hold 暂停保持、或退避重拨中）时不再重复起——
// 暂停中的源由 Resume 复活，重拨中的源由自身循环收敛。
func (s *p2pSource) Start() {
	if !s.running.CompareAndSwap(false, true) {
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	s.mu.Lock()
	s.cancel = cancel
	s.mu.Unlock()
	go func() {
		defer s.running.Store(false)
		defer close(s.done)
		s.emit(StateStarting, nil)
		backoff := 3 * time.Second
		for {
			if ctx.Err() != nil {
				return
			}
			err := s.runOnce(ctx)
			if ctx.Err() != nil {
				return
			}
			s.emit(StateError, err)
			s.log.Warn("p2p source ended, retrying", "dev", s.devID, "err", err)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
		}
	}()
}

// runOnce 建立一路呼叫并泵到其终结。返回错误触发重拨。
func (s *p2pSource) runOnce(ctx context.Context) error {
	call, err := s.dial(s.devID, s.treatedPwd)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	ended := make(chan error, 1)
	s.mu.Lock()
	s.call = call
	s.resetMediaLocked() // 新呼叫：清 SPS/PPS/描述，重新合成
	sink := s.sink
	s.mu.Unlock()
	call.OnH264(func(nal []byte) { s.onNAL(nal, sink) })
	call.OnState(func(st av.State, err error) {
		if st == av.StateClosed {
			select {
			case ended <- err:
			default:
			}
		}
	})
	select {
	case <-ctx.Done():
		call.Stop() // 完整挂断（CLOSE×3+拆链，v1 语义）
		return ctx.Err()
	case err := <-ended:
		return err
	}
}

func (s *p2pSource) resetMediaLocked() {
	s.desc = nil
	s.media = nil
	s.sps = nil
	s.pps = nil
}

// Stop 停止源：取消重拨循环并完整挂断当前呼叫（session_hold 暂停中的
// 呼叫也走本方法彻底拆除——shutdown 路径）。
func (s *p2pSource) Stop() {
	s.mu.Lock()
	cancel := s.cancel
	call := s.call
	s.mu.Unlock()
	if cancel != nil {
		cancel()
	}
	if call != nil {
		call.Stop()
	}
	<-s.done
	s.emit(StateStopped, nil)
}

// Pause 暂停取流但保持呼叫链路（session_hold，Manager 空闲路径调用）：
// 转告 caller.Pause（停 cmd=1 重发、丢 TLV4，KCP/meter 存活）并广播
// StatePaused；已合成的描述（SPS/PPS → SDP）保留——暂停期 DESCRIBE
// 直接命中缓存，新观众 Acquire 走 Resume。
func (s *p2pSource) Pause() {
	s.mu.Lock()
	call := s.call
	s.mu.Unlock()
	if call == nil {
		return // 尚未建链（dial 中）——无从保持
	}
	call.Pause()
	s.emit(StatePaused, nil)
}

// Resume 在同一条活链路上复活取流：转告 caller.Resume（重跑 cmd=1 ×18，
// 设备再次 ACCEPT 后舞蹈与媒体自动恢复）并广播 StateActive——描述缓存
// 仍在，DESCRIBE 立即可服务。
func (s *p2pSource) Resume() {
	s.mu.Lock()
	call := s.call
	s.mu.Unlock()
	if call == nil {
		return
	}
	call.Resume()
	s.emit(StateActive, nil)
}

// stripStartCode 剥离 NAL 前导起始码（3B/4B）。
func stripStartCode(nal []byte) []byte {
	if len(nal) >= 4 && nal[0] == 0 && nal[1] == 0 && nal[2] == 0 && nal[3] == 1 {
		return nal[4:]
	}
	if len(nal) >= 3 && nal[0] == 0 && nal[1] == 0 && nal[2] == 1 {
		return nal[3:]
	}
	return nal
}

// onNAL 处理一个 Annex-B NAL：记录 SPS/PPS、首次双全时合成描述转
// Active，随后打 RTP 包给 sink。
func (s *p2pSource) onNAL(nal []byte, sink func(*description.Media, *rtp.Packet)) {
	if len(nal) < 4 {
		return
	}
	body := stripStartCode(nal)
	if len(body) == 0 {
		return
	}
	typ := body[0] & 0x1f
	s.mu.Lock()
	switch typ {
	case 7:
		s.sps = append([]byte(nil), body...)
	case 8:
		s.pps = append([]byte(nil), body...)
	}
	if s.desc == nil && len(s.sps) > 0 && len(s.pps) > 0 {
		h := &format.H264{PayloadTyp: rtpPT, SPS: s.sps, PPS: s.pps}
		md := &description.Media{Type: "video", Formats: []format.Format{h}}
		s.media = md
		s.desc = &description.Session{Title: "konx-gw p2p-av", Medias: []*description.Media{md}}
		s.log.Info("p2p-av description synthesized",
			"sps", len(s.sps), "pps", len(s.pps))
		s.emit(StateActive, nil)
	}
	var pkts []*rtp.Packet
	if s.desc != nil {
		pkts = s.packetizeLocked(body)
	}
	s.nals++
	s.mu.Unlock()
	if sink != nil {
		for _, p := range pkts {
			sink(s.currentMedia(), p)
		}
	}
}

func (s *p2pSource) currentMedia() *description.Media {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.media
}

// packetizeLocked 把去掉起始码的 NAL 打成 RTP 包（持有 mu）：≤MTU 单 NAL
// 直装；>MTU FU-A 分片（S/E 位置位、末包 marker=1）。seq/ts/ssrc 自管理。
func (s *p2pSource) packetizeLocked(body []byte) []*rtp.Packet {
	mk := func(payload []byte, marker bool) *rtp.Packet {
		s.pkt.seq++
		p := &rtp.Packet{
			Header: rtp.Header{
				Version:        2,
				PayloadType:    rtpPT,
				SequenceNumber: s.pkt.seq,
				Timestamp:      s.pkt.ts,
				SSRC:           s.pkt.ssrc,
				Marker:         marker,
			},
			Payload: payload,
		}
		return p
	}
	if len(body) <= rtpMTU {
		s.pkt.ts += rtpTSStep
		return []*rtp.Packet{mk(body, true)}
	}
	// FU-A 分片
	indicator := (body[0] & 0xe0) | fuIndicator
	nalType := body[0] & 0x1f
	chunk := rtpMTU - 2
	var out []*rtp.Packet
	for off := 1; off < len(body); off += chunk {
		end := off + chunk
		if end > len(body) {
			end = len(body)
		}
		last := end == len(body)
		fuHdr := nalType
		if off == 1 {
			fuHdr |= 0x80 // S
		}
		if last {
			fuHdr |= 0x40 // E
		}
		payload := make([]byte, 0, 2+end-off)
		payload = append(payload, indicator, fuHdr)
		payload = append(payload, body[off:end]...)
		out = append(out, mk(payload, last))
	}
	s.pkt.ts += rtpTSStep
	return out
}
