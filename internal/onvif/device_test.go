package onvif

import (
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/fevenor/konx-gw/internal/config"
)

// newDeviceTestServer 构造匿名访问的测试 ONVIF 服务。
func newDeviceTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Defaults()
	cfg.ONVIF.Listen = "127.0.0.1:8000"
	cfg.Advertise.Host = "127.0.0.1"
	return New(Env{Cfg: cfg})
}

func postSOAP(t *testing.T, s *Server, path, action, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "http://127.0.0.1:8000"+path, strings.NewReader(body))
	req.Header.Set("SOAPAction", action)
	rec := httptest.NewRecorder()
	s.Handler().ServeHTTP(rec, req)
	return rec
}

// capsEnvelope 是 GetCapabilities 应答的结构化映射：标准库
// encoding/xml 必须能完整解出全部能力字段（任何畸形标签——例如把
// 属性写进标签名——都会让 Unmarshal 直接报错）。
type capsEnvelope struct {
	XMLName xml.Name `xml:"Envelope"`
	Body    struct {
		GetCapabilitiesResponse struct {
			Capabilities struct {
				Media struct {
					XAddr                 string `xml:"XAddr"`
					StreamingCapabilities struct {
						RTPMulticast        bool `xml:"RTPMulticast"`
						RTP_TCP             bool `xml:"RTP_TCP"`
						RTP_RTSP_TCP        bool `xml:"RTP_RTSP_TCP"`
						NonAggregateControl bool `xml:"NonAggregateControl"`
					} `xml:"StreamingCapabilities"`
				} `xml:"Media"`
				Events struct {
					XAddr                       string `xml:"XAddr"`
					WSSubscriptionPolicySupport bool   `xml:"WSSubscriptionPolicySupport"`
					WSPullPointSupport          bool   `xml:"WSPullPointSupport"`
				} `xml:"Events"`
				Device struct {
					XAddr string `xml:"XAddr"`
				} `xml:"Device"`
			} `xml:"Capabilities"`
		} `xml:"GetCapabilitiesResponse"`
	} `xml:"Body"`
}

// TestGetCapabilitiesWellFormed：S6 回归防护——整个 GetCapabilities
// 应答必须是良构 XML，且流能力（支持 RTSP over TCP）等字段全部可
// 结构化解出。
func TestGetCapabilitiesWellFormed(t *testing.T) {
	s := newDeviceTestServer(t)
	rec := postSOAP(t, s, "/onvif/device_service", "GetCapabilities",
		`<s:Body><tds:GetCapabilities Category="All"/></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Fatalf("状态码 = %d, 期望 200; body: %s", rec.Code, rec.Body.String())
	}

	var env capsEnvelope
	if err := xml.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("GetCapabilities 应答不是良构 XML: %v\n应答全文: %s", err, rec.Body.String())
	}
	caps := env.Body.GetCapabilitiesResponse.Capabilities

	sc := caps.Media.StreamingCapabilities
	if !sc.RTP_TCP || !sc.RTP_RTSP_TCP {
		t.Errorf("流能力语义应支持 RTSP over TCP: RTP_TCP=%v RTP_RTSP_TCP=%v", sc.RTP_TCP, sc.RTP_RTSP_TCP)
	}
	if sc.RTPMulticast {
		t.Errorf("RTPMulticast 应为 false，实际 true")
	}
	if want := "http://127.0.0.1:8000/onvif/media_service"; caps.Media.XAddr != want {
		t.Errorf("Media.XAddr = %q, 期望 %q", caps.Media.XAddr, want)
	}
	if caps.Events.WSSubscriptionPolicySupport {
		t.Errorf("WSSubscriptionPolicySupport 应为 false，实际 true")
	}
	if !caps.Events.WSPullPointSupport {
		t.Errorf("WSPullPointSupport 应为 true（本服务实现 PullPoint），实际 false")
	}
	if want := "http://127.0.0.1:8000/onvif/events_service"; caps.Events.XAddr != want {
		t.Errorf("Events.XAddr = %q, 期望 %q", caps.Events.XAddr, want)
	}
	if want := "http://127.0.0.1:8000/onvif/device_service"; caps.Device.XAddr != want {
		t.Errorf("Device.XAddr = %q, 期望 %q", caps.Device.XAddr, want)
	}
}

// TestDeviceServiceRepliesWellFormed：device_service 全部已知操作应答
// 均须为良构 XML（encoding/xml 可无错解析）。
func TestDeviceServiceRepliesWellFormed(t *testing.T) {
	s := newDeviceTestServer(t)
	ops := []string{
		"GetServices", "GetServiceCapabilities", "GetDeviceInformation",
		"GetCapabilities", "GetSystemDateAndTime", "GetHostname",
		"GetUsers", "GetSystemLog", "GetRelayOutputs",
	}
	for _, op := range ops {
		rec := postSOAP(t, s, "/onvif/device_service", op,
			`<s:Body><tds:`+op+` Category="All"/></s:Body>`)
		if rec.Code != http.StatusOK {
			t.Errorf("%s: 状态码 = %d, 期望 200; body: %s", op, rec.Code, rec.Body.String())
			continue
		}
		var v interface{}
		if err := xml.Unmarshal(rec.Body.Bytes(), &v); err != nil {
			t.Errorf("%s 应答不是良构 XML: %v\n应答全文: %s", op, err, rec.Body.String())
		}
	}
}
