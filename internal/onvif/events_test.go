package onvif

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

// TestTopicForEmptyTopicsNoPanic：S9 回归防护——onvif.topics 配置为
// 空数组（YAML `topics: []`，合法配置）时不得 panic，门铃事件回落
// 到安全默认 topic 继续下发。
func TestTopicForEmptyTopicsNoPanic(t *testing.T) {
	cfg := config.Defaults()
	cfg.ONVIF.Topics = []string{} // 空数组：合法配置
	s := &Server{env: Env{Cfg: cfg}}

	var topic, val string
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Topics 为空切片时 topicFor panic: %v", r)
			}
		}()
		topic, val = s.topicFor(events.Event{Type: events.TypeDoorbellPress})
	}()
	if topic != defaultDoorbellTopic {
		t.Errorf("空 topics 时应回落到默认 topic %q, 实际 %q", defaultDoorbellTopic, topic)
	}
	if val != "true" {
		t.Errorf("value = %q, 期望 \"true\"", val)
	}
}

// TestTopicForNilTopicsNoPanic：topics 字段整体缺省（nil）时同样不得
// panic。
func TestTopicForNilTopicsNoPanic(t *testing.T) {
	cfg := config.Defaults()
	cfg.ONVIF.Topics = nil
	s := &Server{env: Env{Cfg: cfg}}

	topic, _ := s.topicFor(events.Event{Type: events.TypeDoorbellPress})
	if topic != defaultDoorbellTopic {
		t.Errorf("nil topics 时应回落到默认 topic %q, 实际 %q", defaultDoorbellTopic, topic)
	}
}

// TestTopicForConfiguredTopics：正常配置时仍取配置的第一项。
func TestTopicForConfiguredTopics(t *testing.T) {
	cfg := config.Defaults()
	cfg.ONVIF.Topics = []string{"tns1:Doorbell/Stimulus", "tns1:Doorbell/Custom"}
	s := &Server{env: Env{Cfg: cfg}}

	topic, val := s.topicFor(events.Event{Type: events.TypeDoorbellPress})
	if topic != "tns1:Doorbell/Stimulus" {
		t.Errorf("topic = %q, 期望使用配置第一项 \"tns1:Doorbell/Stimulus\"", topic)
	}
	if val != "true" {
		t.Errorf("value = %q, 期望 \"true\"", val)
	}
}

// newEventsTestServer 构造带事件引擎的测试服务（PullPoint 订阅需要）。
func newEventsTestServer(t *testing.T) *Server {
	t.Helper()
	cfg := config.Defaults()
	cfg.ONVIF.Listen = "127.0.0.1:8000"
	cfg.Advertise.Host = "127.0.0.1"
	return New(Env{Cfg: cfg, Engine: events.NewEngine(nil, nil)})
}

// TestPullPointSubscriptionAddressRoutable：S7 回归防护——
// CreatePullPointSubscription 应答的 SubscriptionReference 地址（含
// 订阅 id 尾段）必须可直接 POST PullMessages（修复前 mux 只注册精确
// 路径，直达 404）；未知 id 返回明确 SubscriptionReferenceFault。
func TestPullPointSubscriptionAddressRoutable(t *testing.T) {
	s := newEventsTestServer(t)

	rec := postSOAP(t, s, "/onvif/events_service", "CreatePullPointSubscription",
		`<s:Body><tev:CreatePullPointSubscription/></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Fatalf("CreatePullPointSubscription: %d %.300s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	i := strings.Index(body, "<wsa:Address>")
	j := strings.Index(body, "</wsa:Address>")
	if i < 0 || j <= i {
		t.Fatalf("应答缺少 SubscriptionReference 地址: %.300s", body)
	}
	addr := body[i+len("<wsa:Address>") : j]
	k := strings.LastIndex(addr, "/onvif/events_service/")
	if k < 0 {
		t.Fatalf("订阅地址不含服务路径: %q", addr)
	}
	subPath := addr[k:]

	rec = postSOAP(t, s, subPath, "PullMessages",
		`<s:Body><tev:PullMessages><tev:Timeout>PT1S</tev:Timeout><tev:MessageLimit>5</tev:MessageLimit></tev:PullMessages></s:Body>`)
	if rec.Code != http.StatusOK {
		t.Fatalf("按订阅地址直达 PullMessages: 状态码 = %d（期望 200，修复前为 404）; body: %.300s",
			rec.Code, rec.Body.String())
	}
	if pull := rec.Body.String(); !strings.Contains(pull, "PullMessagesResponse") || strings.Contains(pull, "Fault") {
		t.Errorf("订阅地址 PullMessages 应答异常: %.300s", pull)
	}

	// 未知/失效 id：明确 SOAP 错误而非 404 或静默成功
	rec = postSOAP(t, s, "/onvif/events_service/konx-nonexistent", "PullMessages",
		`<s:Body><tev:PullMessages><tev:Timeout>PT1S</tev:Timeout></tev:PullMessages></s:Body>`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "SubscriptionReferenceFault") {
		t.Errorf("未知订阅 id 应返回 SubscriptionReferenceFault, 实际: %d %.200s",
			rec.Code, rec.Body.String())
	}
}

// TestParseXSDDuration：D4 回归防护——xs:duration 完整组合解析。
func TestParseXSDDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"PT2S", 2 * time.Second, true},
		{"PT45S", 45 * time.Second, true},
		{"PT0.5S", 500 * time.Millisecond, true},
		{"PT1M", time.Minute, true},
		{"PT1M30S", 90 * time.Second, true},
		{"PT1H", time.Hour, true},
		{"PT1H30M15S", time.Hour + 30*time.Minute + 15*time.Second, true},
		{"P1D", 24 * time.Hour, true},
		{"P1DT1H", 25 * time.Hour, true},
		{"P1DT2H3M4S", 26*time.Hour + 3*time.Minute + 4*time.Second, true},
		{"-PT5S", -5 * time.Second, true},
		{"PT", 0, false},      // 空时长
		{"P", 0, false},       // 空时长
		{"", 0, false},        // 空
		{"1M", 0, false},      // 缺少 P 前缀
		{"PT1X", 0, false},    // 非法单位
		{"garbage", 0, false}, // 垃圾
		{"P1DT", 0, false},    // T 后无分量
		{"PT1M0.5S", 60500 * time.Millisecond, true},
	}
	for _, c := range cases {
		got, err := parseXSDDuration(c.in)
		if c.ok {
			if err != nil {
				t.Errorf("parseXSDDuration(%q) 意外报错: %v", c.in, err)
				continue
			}
			if got != c.want {
				t.Errorf("parseXSDDuration(%q) = %v, 期望 %v", c.in, got, c.want)
			}
		} else if err == nil {
			t.Errorf("parseXSDDuration(%q) 应报错, 实际返回 %v", c.in, got)
		}
	}
}

// TestPullWait：D4 回归防护——元素式/属性式 Timeout 均解析；超上限
// 钳制到 30s 而不是静默缩回 5s（修复前 PT1M/PT45S 均被丢弃退回 5s）。
func TestPullWait(t *testing.T) {
	s := &Server{env: Env{Cfg: config.Defaults()}}
	cases := []struct {
		name string
		body string
		want time.Duration
	}{
		{"无 Timeout 用默认", `<tev:PullMessages/>`, 5 * time.Second},
		{"元素式 PT2S", `<tev:PullMessages><tev:Timeout>PT2S</tev:Timeout></tev:PullMessages>`, 2 * time.Second},
		{"元素式带命名空间", `<tev:PullMessages><tev:Timeout>PT3S</tev:Timeout></tev:PullMessages>`, 3 * time.Second},
		{"属性式 PT8S", `<tev:PullMessages Timeout="PT8S"/>`, 8 * time.Second},
		{"PT45S 钳到上限", `<tev:PullMessages><tev:Timeout>PT45S</tev:Timeout></tev:PullMessages>`, maxPullWait},
		{"PT1M 钳到上限", `<tev:PullMessages><tev:Timeout>PT1M</tev:Timeout></tev:PullMessages>`, maxPullWait},
		{"PT1H 钳到上限", `<tev:PullMessages><tev:Timeout>PT1H</tev:Timeout></tev:PullMessages>`, maxPullWait},
		{"非法值回默认", `<tev:PullMessages><tev:Timeout>banana</tev:Timeout></tev:PullMessages>`, 5 * time.Second},
		{"零值回默认", `<tev:PullMessages><tev:Timeout>PT0S</tev:Timeout></tev:PullMessages>`, 5 * time.Second},
		{"负值回默认", `<tev:PullMessages><tev:Timeout>-PT5S</tev:Timeout></tev:PullMessages>`, 5 * time.Second},
	}
	for _, c := range cases {
		if got := s.pullWait(c.body); got != c.want {
			t.Errorf("%s: pullWait = %v, 期望 %v", c.name, got, c.want)
		}
	}
}
