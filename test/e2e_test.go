// Package test runs the full-stack E2E suite: a fake upstream RTSP camera
// (gortsplib server) → konx-gw source → RTSP egress → downstream client,
// plus the ONVIF SOAP surface and the REST/WS API.
package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/base"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"

	"github.com/fevenor/konx-gw/internal/app"
	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

// ---------------------------------------------------------------------------
// fake camera: an RTSP server that DESCRIBEs an H264+G711 stream and
// emits synthetic RTP packets on PLAY.
// ---------------------------------------------------------------------------

type fakeCam struct {
	srv    *gortsplib.Server
	stream *gortsplib.ServerStream
	desc   *description.Session
}

func startFakeCam(t *testing.T) (*fakeCam, string) {
	t.Helper()
	// pick a free port
	ln, _ := net.Listen("tcp", "127.0.0.1:0")
	port := ln.Addr().(*net.TCPAddr).Port
	ln.Close()

	mediav := &description.Media{Type: description.MediaTypeVideo}
	h264 := &format.H264{PayloadTyp: 96}
	mediav.Formats = append(mediav.Formats, h264)
	mediaa := &description.Media{Type: description.MediaTypeAudio}
	g711 := &format.G711{MULaw: false}
	mediaa.Formats = append(mediaa.Formats, g711)

	desc := &description.Session{Medias: []*description.Media{mediav, mediaa}}

	cam := &fakeCam{desc: desc}
	cam.srv = &gortsplib.Server{
		Handler:     cam,
		RTSPAddress: fmt.Sprintf("127.0.0.1:%d", port),
	}
	if err := cam.srv.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { cam.srv.Close() })
	return cam, fmt.Sprintf("rtsp://127.0.0.1:%d/ondemand", port)
}

func (c *fakeCam) OnDescribe(*gortsplib.ServerHandlerOnDescribeCtx) (*base.Response, *gortsplib.ServerStream, error) {
	if c.stream == nil {
		c.stream = &gortsplib.ServerStream{Server: c.srv, Desc: c.desc}
		if err := c.stream.Initialize(); err != nil {
			return nil, nil, err
		}
		go c.pump()
	}
	return &base.Response{StatusCode: base.StatusOK}, c.stream, nil
}

func (c *fakeCam) OnSetup(*gortsplib.ServerHandlerOnSetupCtx) (*base.Response, *gortsplib.ServerStream, error) {
	return &base.Response{StatusCode: base.StatusOK}, c.stream, nil
}

func (c *fakeCam) OnPlay(*gortsplib.ServerHandlerOnPlayCtx) (*base.Response, error) {
	return &base.Response{StatusCode: base.StatusOK}, nil
}

// pump emits tiny synthetic RTP packets so downstream clients see data.
func (c *fakeCam) pump() {
	seq := uint16(0)
	for {
		time.Sleep(50 * time.Millisecond)
		pkt := &rtp.Packet{
			Header:  rtp.Header{Version: 2, SequenceNumber: seq, Timestamp: uint32(seq) * 3600, PayloadType: 96},
			Payload: []byte{0x00, 0x00, 0x00, 0x01, 0x65, byte(seq)}, // fake IDR-ish
		}
		if err := c.stream.WritePacketRTP(c.desc.Medias[0], pkt); err != nil {
			return
		}
		seq++
	}
}

// ---------------------------------------------------------------------------
// gateway harness
// ---------------------------------------------------------------------------

// E2E 套件缺省跳过：设置 KONX_E2E=1 启用。套件全程本地回环 mock（假
// 摄像头 + 随机端口），不依赖任何真实设备或凭据；设备 ID 缺省为中性
// 占位值，可用 KONX_E2E_DEVICE_ID 覆盖注入。
func requireE2E(t *testing.T) {
	t.Helper()
	if os.Getenv("KONX_E2E") != "1" {
		t.Skip("E2E disabled by default; set KONX_E2E=1 to run")
	}
}

var e2eDeviceID = func() string {
	if v := os.Getenv("KONX_E2E_DEVICE_ID"); v != "" {
		return v
	}
	return "10000001"
}()

type harness struct {
	app     *app.App
	baseURL string
	token   string
	dataDir string
}

func startGateway(t *testing.T, upstreamURL string) *harness {
	t.Helper()
	dataDir := t.TempDir()
	_, rtspP, onvifP, webP := freePorts(t, 3)

	cfg := config.Defaults()
	cfg.GwID = "test-gw"
	cfg.Cloud.User = "test-user"
	cfg.Cloud.PassMD5 = strings.Repeat("a", 32)
	cfg.Web.DataDir = dataDir
	cfg.Web.Listen = fmt.Sprintf("127.0.0.1:%d", webP)
	cfg.RTSP.Listen = fmt.Sprintf("127.0.0.1:%d", rtspP)
	cfg.ONVIF.Listen = fmt.Sprintf("127.0.0.1:%d", onvifP)
	cfg.MQTT.Enable = false
	cfg.Media.Source = "external-rtsp"
	cfg.Media.ExternalURL = upstreamURL
	cfg.Media.FFmpegPath = findFFmpeg()
	cfg.Media.IdleStop = 2 * time.Second
	cfg.Devices = []config.Device{{ID: e2eDeviceID, Name: "Front Door"}}
	cfg.Devices[0].RTSP.Pass = "auto"
	cfg.Advertise.Host = "127.0.0.1"
	cfg.Advertise.RTSPPort = rtspP
	cfg.Advertise.HTTPPort = webP

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	a, err := app.Build(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		time.Sleep(200 * time.Millisecond)
	})
	go func() { _ = a.Run(ctx) }()

	// wait for web port
	waitTCP(t, webP, 5*time.Second)

	h := &harness{app: a, baseURL: fmt.Sprintf("http://127.0.0.1:%d", webP), dataDir: dataDir}
	// mint a read token through auth directly
	tok, err := a.Auth.CreateToken("test", []string{auth.ScopeRead, auth.ScopeTalk, auth.ScopeAdmin})
	if err != nil {
		t.Fatal(err)
	}
	h.token = tok.Secret
	return h
}

func freePorts(t *testing.T, n int) (int, int, int, int) {
	t.Helper()
	var ports []int
	for i := 0; i < n+1; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ports = append(ports, ln.Addr().(*net.TCPAddr).Port)
		ln.Close()
	}
	return ports[0], ports[1], ports[2], ports[3]
}

func waitTCP(t *testing.T, port int, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		c, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
		if err == nil {
			c.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("port %d never opened", port)
}

func findFFmpeg() string {
	for _, p := range []string{"/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "ffmpeg"
}

func (h *harness) get(path string) (*http.Response, error) {
	req, _ := http.NewRequest("GET", h.baseURL+path, nil)
	req.Header.Set("Authorization", "Bearer "+h.token)
	return http.DefaultClient.Do(req)
}

func (h *harness) post(path string, body any) (*http.Response, error) {
	var buf io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewReader(b)
	}
	req, _ := http.NewRequest("POST", h.baseURL+path, buf)
	req.Header.Set("Authorization", "Bearer "+h.token)
	req.Header.Set("Content-Type", "application/json")
	return http.DefaultClient.Do(req)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestStateAndEventsAPI(t *testing.T) {
	requireE2E(t)
	cam, camURL := startFakeCam(t)
	_ = cam
	h := startGateway(t, camURL)

	resp, err := h.get("/api/state")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("state: %v %v", err, resp)
	}
	var snap map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&snap)
	if snap["version"] == nil {
		t.Fatal("no version in state")
	}
	if snap["source"] != "external-rtsp" {
		t.Fatalf("source = %v", snap["source"])
	}

	// inject an event through the engine and read it back
	h.app.Eng.Submit(events.Event{DeviceID: e2eDeviceID, Type: events.TypeDoorbellPress, MsgID: "42", Payload: `{"alarm_type":2}`})
	resp, err = h.get("/api/events?limit=10")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("events: %v", err)
	}
	var evd struct {
		Events []map[string]any `json:"events"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&evd)
	if len(evd.Events) == 0 {
		t.Fatal("no events returned")
	}
	found := false
	for _, e := range evd.Events {
		if e["type"] == "doorbell.press" && e["device_id"] == e2eDeviceID {
			found = true
		}
	}
	if !found {
		t.Fatalf("doorbell.press not in %v events", len(evd.Events))
	}

	// unauthorized is rejected
	r2, _ := http.Get(h.baseURL + "/api/state")
	if r2.StatusCode != 401 {
		t.Fatalf("unauth status = %d", r2.StatusCode)
	}
}

func TestRTSPFlowThroughGateway(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	rtspPort := h.app.Cfg.AdvertisedRTSPPort()

	// downstream client pulls through the gateway egress
	u, err := base.ParseURL(fmt.Sprintf("rtsp://127.0.0.1:%d/konx/%s/main", rtspPort, e2eDeviceID))
	if err != nil {
		t.Fatal(err)
	}
	lu, lp := h.app.Auth.Loopback()
	authHeader := base.HeaderValue{"Basic " + basic64(lu+":"+lp)}
	c := &gortsplib.Client{
		Scheme: u.Scheme, Host: u.Host,
		OnRequest: func(req *base.Request) {
			req.Header["Authorization"] = authHeader
		},
	}
	if err := c.Start2(); err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	var desc *description.Session
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		desc, _, err = c.Describe(u)
		if err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("describe through gateway: %v", err)
	}
	if len(desc.Medias) != 2 {
		t.Fatalf("expected 2 medias, got %d", len(desc.Medias))
	}
	if err := c.SetupAll(desc.BaseURL, desc.Medias); err != nil {
		t.Fatalf("setup: %v", err)
	}
	got := make(chan *rtp.Packet, 8)
	c.OnPacketRTPAny(func(m *description.Media, _ format.Format, pkt *rtp.Packet) {
		select {
		case got <- pkt:
		default:
		}
	})
	if _, err := c.Play(nil); err != nil {
		t.Fatalf("play: %v", err)
	}
	select {
	case <-got:
		// packet traversed fakecam → gateway → egress → client
	case <-time.After(5 * time.Second):
		t.Fatal("no RTP packet flowed through the gateway within 5s")
	}

	// unauthorized RTSP is rejected (no Authorization header)
	c2 := &gortsplib.Client{Scheme: u.Scheme, Host: u.Host}
	if err := c2.Start2(); err != nil {
		t.Fatal(err)
	}
	defer c2.Close()
	if _, _, err := c2.Describe(u); err == nil {
		t.Fatal("describe without credentials should fail")
	}
}

func TestONVIFSurface(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	onvifURL := fmt.Sprintf("http://127.0.0.1:%d", h.app.Cfg.AdvertisedONVIFPort())

	post := func(path, body string) (int, string) {
		resp, err := http.Post(onvifURL+path, "application/soap+xml",
			strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	// GetDeviceInformation
	code, body := post("/onvif/device_service", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetDeviceInformation xmlns="http://www.onvif.org/ver10/device/wsdl"/></s:Body></s:Envelope>`)
	if code != 200 || !strings.Contains(body, "Konx") || !strings.Contains(body, "konx-gw") {
		t.Fatalf("GetDeviceInformation: %d %.200s", code, body)
	}

	// GetProfiles
	_, body = post("/onvif/media_service", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetProfiles xmlns="http://www.onvif.org/ver10/media/wsdl"/></s:Body></s:Envelope>`)
	if !strings.Contains(body, "profile_"+e2eDeviceID+"_main") || !strings.Contains(body, "H264") || !strings.Contains(body, "G711") {
		t.Fatalf("GetProfiles missing tokens: %.300s", body)
	}

	// GetStreamUri points at the egress
	_, body = post("/onvif/media_service", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><GetStreamUri xmlns="http://www.onvif.org/ver10/media/wsdl"><ProfileToken>profile_" + e2eDeviceID + "_main</ProfileToken></GetStreamUri></s:Body></s:Envelope>`)
	if !strings.Contains(body, "rtsp://127.0.0.1:") || !strings.Contains(body, "/konx/"+e2eDeviceID+"/main") {
		t.Fatalf("GetStreamUri: %.300s", body)
	}

	// PullPoint: subscribe then pull (doorbell injected in between)
	_, body = post("/onvif/events_service", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope"><s:Body><CreatePullPointSubscription xmlns="http://www.onvif.org/ver10/events/wsdl"/></s:Body></s:Envelope>`)
	if !strings.Contains(body, "SubscriptionReference") {
		t.Fatalf("CreatePullPointSubscription: %.300s", body)
	}
	addr := body[strings.Index(body, "<wsa:Address>")+len("<wsa:Address>"):]
	addr = addr[:strings.Index(addr, "</wsa:Address>")]
	h.app.Eng.Submit(events.Event{DeviceID: e2eDeviceID, Type: events.TypeDoorbellPress, MsgID: "77"})
	_, body = post("/onvif/events_service", `<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://www.w3.org/2005/08/addressing"><s:Body><PullMessages xmlns="http://www.onvif.org/ver10/events/wsdl"><Timeout>PT2S</Timeout><MessageLimit>10</MessageLimit><SubscriptionReference><wsa:Address>`+addr+`</wsa:Address></SubscriptionReference></PullMessages></s:Body></s:Envelope>`)
	if !strings.Contains(body, "Doorbell") || !strings.Contains(body, "doorbell.press") {
		t.Fatalf("PullMessages missing doorbell: %.400s", body)
	}
}

func TestTalkUnavailable501(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	resp, err := h.post("/api/devices/"+e2eDeviceID+"/talk/start", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 501 {
		t.Fatalf("talk start = %d, want 501", resp.StatusCode)
	}
	var e struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&e)
	if e.Error.Code != "talk_unavailable" || !strings.Contains(e.Error.Message, "p2pv3") {
		t.Fatalf("talk error body: %+v", e)
	}
}

func TestSnapshotPipeline(t *testing.T) {
	requireE2E(t)
	if _, err := os.Stat(findFFmpeg()); err != nil {
		t.Skip("ffmpeg not available")
	}
	// A raw synthetic H264 stream won't decode into a frame; this test
	// verifies the pipeline plumbing (acquire → ffmpeg → response) using
	// the fake camera; ffmpeg will fail decode → 503 snapshot_failed,
	// which is the documented warm-up path. Success is asserted on the
	// error CONTRACT, not on pixels.
	cam, camURL := startFakeCam(t)
	_ = cam
	h := startGateway(t, camURL)
	resp, err := h.get("/api/devices/" + e2eDeviceID + "/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		b, _ := io.ReadAll(resp.Body)
		if len(b) > 0 && b[0] == 0xFF && b[1] == 0xD8 {
			return // real JPEG
		}
		t.Fatalf("snapshot 200 but not JPEG: %d bytes head=%v", len(b), b[:min(4, len(b))])
	}
	// documented warm-up failure path
	var e map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&e)
	if ec, _ := e["error"].(map[string]any); ec == nil || ec["code"] != "snapshot_failed" {
		t.Fatalf("snapshot error contract: %v", e)
	}
}

func TestHealthzAndMetrics(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	resp, err := http.Get(h.baseURL + "/healthz")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("healthz: %v", err)
	}
	resp, err = h.get("/metrics")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("metrics: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "konx_p2p_up") {
		t.Fatalf("metrics missing konx_p2p_up: %.200s", b)
	}
}

func TestWebUI(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	resp, err := http.Get(h.baseURL + "/")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("ui: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(b), "konx-gw") || !strings.Contains(string(b), "客户端") {
		t.Fatalf("ui content missing: %.200s", b)
	}
}

func TestLoginAndRateLimit(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	// wrong password
	resp, _ := http.Post(h.baseURL+"/api/login", "application/json",
		strings.NewReader(`{"password":"wrong"}`))
	if resp.StatusCode != 401 {
		t.Fatalf("login wrong = %d", resp.StatusCode)
	}
	// hammer the endpoint: rate limited at 5/min
	last := 0
	for i := 0; i < 8; i++ {
		resp, _ := http.Post(h.baseURL+"/api/login", "application/json",
			strings.NewReader(`{"password":"wrong"}`))
		last = resp.StatusCode
	}
	if last != 429 {
		t.Fatalf("expected 429 after hammering, got %d", last)
	}
}

// URL is used in snapshot doc building; silence import when unused.
var _ = url.Values{}

func TestConfigValidation(t *testing.T) {
	requireE2E(t)
	// device-rtsp without exclusive ack must be rejected
	cfg := config.Defaults()
	cfg.Cloud.User = "u"
	cfg.Cloud.PassMD5 = strings.Repeat("a", 32)
	cfg.Media.Source = "device-rtsp"
	if err := cfg.Validate(); err == nil {
		t.Fatal("device-rtsp without ack must fail")
	}
	// p2p-av：合法（数字设备 id；treated 密码可直配或运行时云解析）
	cfg.Media.Source = "p2p-av"
	cfg.Devices = []config.Device{{ID: e2eDeviceID}}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("p2p-av validation: %v", err)
	}
	cfg.Devices = nil
	if err := cfg.Validate(); err == nil {
		t.Fatal("p2p-av without devices must fail")
	}
	// external-rtsp without url must fail
	cfg.Media.Source = "external-rtsp"
	if err := cfg.Validate(); err == nil {
		t.Fatal("external-rtsp without url must fail")
	}
}

func TestGoldenDBPersists(t *testing.T) {
	// events survive a "restart" (new engine over the same db file)
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "x.db")
	st, err := events.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	eng := events.NewEngine(events.NewRegistry(), st)
	eng.Submit(events.Event{DeviceID: "d", Type: "gw.system"})
	st.Close()

	st2, err := events.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer st2.Close()
	got, _ := st2.Query("", "gw.system", 0, 10)
	if len(got) != 1 {
		t.Fatalf("persisted events = %d", len(got))
	}
}

func basic64(s string) string {
	const tbl = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+"
	var out []byte
	var buf, bits int
	for _, ch := range []byte(s) {
		buf = buf<<8 | int(ch)
		bits += 8
		for bits >= 6 {
			bits -= 6
			out = append(out, tbl[(buf>>bits)&0x3F])
		}
	}
	if bits > 0 {
		out = append(out, tbl[(buf<<(6-bits))&0x3F])
	}
	for len(out)%4 != 0 {
		out = append(out, '=')
	}
	return string(out)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func TestEventScopeEnrichment(t *testing.T) {
	requireE2E(t)
	_, camURL := startFakeCam(t)
	h := startGateway(t, camURL)
	h.app.Eng.Submit(events.Event{DeviceID: e2eDeviceID, Type: events.TypeDoorbellPress, MsgID: "s1"})
	h.app.Eng.Submit(events.Event{DeviceID: "cloud", Type: events.TypeCloudSession})
	h.app.Eng.Submit(events.Event{DeviceID: "gw", Type: events.TypeSystem})
	time.Sleep(200 * time.Millisecond)

	resp, err := h.get("/api/events?limit=20")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("events: %v", err)
	}
	var evd struct {
		Events []map[string]any `json:"events"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&evd)
	scopes := map[string]string{}
	for _, e := range evd.Events {
		if _, ok := scopes[e["device_id"].(string)]; !ok {
			scopes[e["device_id"].(string)] = e["scope"].(string)
		}
	}
	if scopes[e2eDeviceID] != "device" || scopes["cloud"] != "cloud" || scopes["gw"] != "gw" {
		t.Fatalf("scope mapping: %v", scopes)
	}
	// device_name enrichment
	for _, e := range evd.Events {
		if e["device_id"] == e2eDeviceID && e["scope"] == "device" {
			if e["device_name"] != "Front Door" {
				t.Fatalf("device_name = %v", e["device_name"])
			}
			break
		}
	}
	// scope filter
	resp, _ = h.get("/api/events?scope=cloud&limit=20")
	_ = json.NewDecoder(resp.Body).Decode(&evd)
	for _, e := range evd.Events {
		if e["scope"] != "cloud" {
			t.Fatalf("scope filter leaked: %v", e)
		}
	}
}
