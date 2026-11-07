package rtspout

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/fevenor/konx-gw/internal/media"
	"github.com/pion/rtp"
)

// 内存监听替身让库初始化；不会创建网络 socket 或接收外部连接。
type reviewListener struct {
	done chan struct{}
	once sync.Once
}

func (l *reviewListener) Accept() (net.Conn, error) { <-l.done; return nil, net.ErrClosed }
func (l *reviewListener) Close() error              { l.once.Do(func() { close(l.done) }); return nil }
func (l *reviewListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 8554} }

func TestRegressionRTSPRecoversAfterSourceReconnect(t *testing.T) {
	mgr := newFakeSourceManager()
	newDesc := func() *description.Session {
		return &description.Session{Medias: []*description.Media{{Type: "video", Formats: []format.Format{&format.H264{PayloadTyp: 96}}}}}
	}
	k := key("10000001", media.ProfileMain)
	mgr.descs[k] = newDesc()
	mgr.slots[k] = true
	s := New("127.0.0.1:8554", mgr, nil, nil, quietLog())
	l := &reviewListener{done: make(chan struct{})}
	s.srv.Listen = func(string, string) (net.Listener, error) { return l, nil }
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	e, err := s.streamFor("10000001", media.ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	defer s.OnSourceState(media.SourceState{Device: "10000001", Profile: media.ProfileMain, State: media.StateStopped})
	oldViewer := &gortsplib.ServerSession{}
	setupOK(t, s, oldViewer, "konx/10000001/main")
	s.OnSourceState(media.SourceState{Device: "10000001", Profile: media.ProfileMain, State: media.StateError})
	mgr.descs[k] = newDesc()
	s.OnSourceState(media.SourceState{Device: "10000001", Profile: media.ProfileMain, State: media.StateActive})
	reconnected, err := s.streamFor("10000001", media.ProfileMain)
	if err != nil {
		t.Fatal(err)
	}
	if reconnected == e {
		t.Fatal("源重连后仍返回旧出口流")
	}
	newViewer := &gortsplib.ServerSession{}
	setupOK(t, s, newViewer, "konx/10000001/main")
	closeSession(s, oldViewer)
	if s.streams[k] != reconnected || reconnected.viewers != 1 {
		t.Fatal("旧观众收尾误关闭新出口流")
	}
	if err := reconnected.ss.WritePacketRTPWithNTP(mgr.descs[k].Medias[0], &rtp.Packet{Header: rtp.Header{Version: 2, PayloadType: 96}, Payload: []byte{0x61, 1}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	closeSession(s, newViewer)
}
