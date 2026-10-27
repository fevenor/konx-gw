package onvif

import (
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fevenor/konx-gw/internal/events"
)

// eventsService implements ONVIF Events with Basic Profile PullPoint
// subscriptions (DESIGN §7.1). Doorbell presses arrive as
// tns1:Doorbell/Stimulus (configurable alias list).
type pullSub struct {
	id       string
	queue    []events.Event
	lastPull time.Time
	cancel   func()
}

var reTermination = regexp.MustCompile(`TerminationTime="[^"]*"`)
var reTimeout = regexp.MustCompile(`Timeout="([^"]*)"`)
var reAddr = regexp.MustCompile(`<wsa:Address>([^<]*)</wsa:Address>`)

func (s *Server) eventsService(w http.ResponseWriter, r *http.Request, action, body string) {
	if !s.checkUserToken(body) {
		soapFault(w, "s:Sender", "ter:NotAuthorized", "invalid user token")
		return
	}
	op := actionOf(action, body, "")
	switch op {
	case "GetServiceCapabilities":
		s.eventsCapabilities(w)
	case "CreatePullPointSubscription":
		s.createPullPoint(w, r)
	case "PullMessages":
		s.pullMessages(w, r, body)
	case "Unsubscribe":
		// SubscriptionReference Address carries our sub id
		if m := reAddr.FindStringSubmatch(body); m != nil {
			s.unsubscribe(stripAddr(m[1]))
		}
		reply(w, `<s:Body><tev:UnsubscribeResponse/></s:Body>`)
	case "Subscribe":
		soapFault(w, "s:Sender", "ter:ActionNotSupported", "use PullPoint")
	case "GetEventProperties":
		reply(w, `<s:Body><tev:GetEventPropertiesResponse>`+
			`<tev:TopicNamespaceLocation>http://www.onvif.org/onvif/ver10/topics/topicns.xml</tev:TopicNamespaceLocation>`+
			`<wsnt:FixedTopicSet>true</wsnt:FixedTopicSet>`+
			`<tev:TopicExpressionDialect>http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet</tev:TopicExpressionDialect>`+
			`<tev:MessageContentFilterDialect>http://www.onvif.org/ver10/tev/messageContentFilter/ItemFilter</tev:MessageContentFilterDialect>`+
			`</tev:GetEventPropertiesResponse></s:Body>`)
	default:
		if op == "" {
			soapFault(w, "s:Sender", "ter:ActionNotSupported", "missing operation")
			return
		}
		reply(w, `<s:Body><`+opLocal(op)+`Response/></s:Body>`)
	}
}

func (s *Server) eventsCapabilities(w http.ResponseWriter) {
	reply(w, `<s:Body><tev:GetServiceCapabilitiesResponse><tev:Capabilities`+
		` WSSubscriptionPolicySupport="false" PersistentNotificationStorage="false"/>`+
		`</tev:GetServiceCapabilitiesResponse></s:Body>`)
}

func stripAddr(a string) string {
	if i := strings.LastIndex(a, "/"); i >= 0 {
		return a[i+1:]
	}
	return a
}

func (s *Server) createPullPoint(w http.ResponseWriter, r *http.Request) {
	id := randID()
	host := s.advertiseHost(r)
	port := s.env.Cfg.AdvertisedONVIFPort()
	addr := "http://" + host + ":" + itoa(port) + "/onvif/events_service/" + id

	ctx := &subCtx{id: id, srv: s}
	cancel := s.env.Engine.Subscribe(func(ev events.Event) {
		ctx.push(ev)
	})
	s.mu.Lock()
	s.subs[id] = &pullSub{id: id, cancel: cancel, lastPull: time.Now()}
	s.mu.Unlock()

	// expire subscriptions idle for 10 minutes
	go func() {
		for range time.Tick(time.Minute) {
			s.mu.Lock()
			for sid, sub := range s.subs {
				if time.Since(sub.lastPull) > 10*time.Minute {
					sub.cancel()
					delete(s.subs, sid)
				}
			}
			s.mu.Unlock()
		}
	}()

	reply(w, `<s:Body><tev:CreatePullPointSubscriptionResponse>`+
		`<tev:SubscriptionReference><wsa:Address>`+addr+`</wsa:Address></tev:SubscriptionReference>`+
		`<wsnt:CurrentTime>`+xsdTime(time.Now())+`</wsnt:CurrentTime>`+
		`<wsnt:TerminationTime>`+xsdTime(time.Now().Add(10*time.Minute))+`</wsnt:TerminationTime>`+
		`</tev:CreatePullPointSubscriptionResponse></s:Body>`)
}

// subCtx fan-outs engine events into per-subscription queues.
type subCtx struct {
	id  string
	srv *Server
}

func (c *subCtx) push(ev events.Event) {
	s := c.srv
	s.mu.Lock()
	defer s.mu.Unlock()
	if sub, ok := s.subs[c.id]; ok {
		sub.queue = append(sub.queue, ev)
		if len(sub.queue) > 256 {
			sub.queue = sub.queue[len(sub.queue)-256:]
		}
	}
}

func (s *Server) unsubscribe(id string) {
	s.mu.Lock()
	if sub, ok := s.subs[id]; ok {
		sub.cancel()
		delete(s.subs, id)
	}
	s.mu.Unlock()
}

var reSubIDPath = regexp.MustCompile(`/onvif/events_service/([A-Za-z0-9\-]+)`)
var reMsgLimit = regexp.MustCompile(`MessageLimit="(\d+)"`)
var reMsgLimitElem = regexp.MustCompile(`<(?:[A-Za-z0-9]+:)?MessageLimit[^>]*>\s*(\d+)\s*</(?:[A-Za-z0-9]+:)?MessageLimit>`)
var reTimeoutElem = regexp.MustCompile(`<(?:[A-Za-z0-9]+:)?Timeout[^>]*>\s*([^<]*)\s*</(?:[A-Za-z0-9]+:)?Timeout>`)

// reXSDDuration 匹配 xs:duration 的日/时/分/秒组合（P1DT2H3M4.5S）。
var reXSDDuration = regexp.MustCompile(`^(-)?P(?:(\d+)D)?(?:T(?:(\d+(?:\.\d+)?)H)?(?:(\d+(?:\.\d+)?)M)?(?:(\d+(?:\.\d+)?)S)?)?$`)

// parseXSDDuration 解析 ISO8601/xs:duration 时长（PnDTnHnMnS，支持
// 小数秒），不识别的形态返回错误。
func parseXSDDuration(s string) (time.Duration, error) {
	m := reXSDDuration.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return 0, fmt.Errorf("invalid xs:duration %q", s)
	}
	if m[2] == "" && m[3] == "" && m[4] == "" && m[5] == "" {
		return 0, fmt.Errorf("empty xs:duration %q", s)
	}
	if strings.Contains(m[0], "T") && m[3] == "" && m[4] == "" && m[5] == "" {
		// "P1DT" 之类：声明了时间部分却没有任何时间分量，非法
		return 0, fmt.Errorf("missing time components in %q", s)
	}
	neg := m[1] == "-"
	num := func(str string) float64 {
		v, _ := strconv.ParseFloat(str, 64)
		return v
	}
	d := time.Duration(num(m[2]) * float64(24*time.Hour))
	d += time.Duration(num(m[3]) * float64(time.Hour))
	d += time.Duration(num(m[4]) * float64(time.Minute))
	d += time.Duration(num(m[5]) * float64(time.Second))
	if neg {
		d = -d
	}
	return d, nil
}

// maxPullWait 是 PullMessages 长轮询等待上限：该请求同步占用一条
// HTTP 连接直至有消息或超时，上限 30s 避免连接被长时间钉死
// （同时也是订阅清理周期的量级），ONVIF 允许服务端自行限定。
const maxPullWait = 30 * time.Second

// warnLog 记录警告日志；Env 未注入 Logger 时退回默认日志器，保证
// 测试与轻量嵌入场景可用。
func (s *Server) warnLog(msg string, args ...any) {
	log := s.env.Log
	if log == nil {
		log = slog.Default()
	}
	log.Warn(msg, args...)
}

// pullWait 解析 PullMessages 请求的等待时长：优先元素式
// <tev:Timeout>，回退属性式 Timeout="..."。解析失败或非正值时回
// 到默认 5s 并记录日志；超过上限时不静默丢值，钳制到 30s 并记录。
func (s *Server) pullWait(body string) time.Duration {
	const defaultWait = 5 * time.Second
	raw := ""
	if m := reTimeoutElem.FindStringSubmatch(body); m != nil {
		raw = m[1]
	} else if m := reTimeout.FindStringSubmatch(body); m != nil {
		raw = m[1]
	}
	if raw == "" {
		return defaultWait
	}
	d, err := parseXSDDuration(raw)
	if err != nil || d <= 0 {
		s.warnLog("onvif pullmessages: 无法解析的 Timeout，使用默认等待",
			"timeout", raw, "err", err, "default", defaultWait.String())
		return defaultWait
	}
	if d > maxPullWait {
		s.warnLog("onvif pullmessages: Timeout 超过上限，已钳制",
			"timeout", raw, "capped_to", maxPullWait.String())
		return maxPullWait
	}
	return d
}

func (s *Server) pullMessages(w http.ResponseWriter, r *http.Request, body string) {
	// subscription id from Address element or path
	id := ""
	if m := reAddr.FindStringSubmatch(body); m != nil {
		id = stripAddr(m[1])
	}
	if id == "" {
		if m := reSubIDPath.FindStringSubmatch(r.RequestURI); m != nil {
			id = m[1]
		}
	}
	s.mu.Lock()
	sub, ok := s.subs[id]
	s.mu.Unlock()
	if !ok {
		soapFault(w, "s:Sender", "ter:SubscriptionReferenceFault", "unknown subscription")
		return
	}

	// honor the requested wait (capped at maxPullWait) for at least one message
	timeout := s.pullWait(body)
	limit := 20
	if m := reMsgLimitElem.FindStringSubmatch(body); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			limit = n
		}
	} else if m := reMsgLimit.FindStringSubmatch(body); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil && n > 0 {
			limit = n
		}
	}
	deadline := time.Now().Add(timeout)
	for {
		s.mu.Lock()
		q := sub.queue
		sub.lastPull = time.Now()
		s.mu.Unlock()
		if len(q) > 0 || time.Now().After(deadline) {
			n := len(q)
			if n > limit {
				n = limit
			}
			taken := q[:n]
			s.mu.Lock()
			sub.queue = sub.queue[n:]
			s.mu.Unlock()
			reply(w, s.messagesBody(taken))
			return
		}
		time.Sleep(150 * time.Millisecond)
	}
}

// messagesBody renders engine events as ONVIF notification messages.
func (s *Server) messagesBody(evs []events.Event) string {
	var b strings.Builder
	b.WriteString(`<s:Body><tev:PullMessagesResponse><tev:CurrentTime>`)
	b.WriteString(xsdTime(time.Now()))
	b.WriteString(`</tev:CurrentTime><tev:TerminationTime>`)
	b.WriteString(xsdTime(time.Now().Add(10 * time.Minute)))
	b.WriteString(`</tev:TerminationTime>`)
	for _, ev := range evs {
		topic, val := s.topicFor(ev)
		if topic == "" {
			continue
		}
		b.WriteString(`<wsnt:NotificationMessage>`)
		b.WriteString(`<wsnt:Topic Dialect="http://www.onvif.org/ver10/tev/topicExpression/ConcreteSet">`)
		b.WriteString(xmlEscape(topic))
		b.WriteString(`</wsnt:Topic><wsnt:Message>`)
		b.WriteString(`<tt:Message UtcTime="` + xsdTime(time.Unix(ev.TSRecv, 0)) + `" PropertyOperation="Changed">`)
		b.WriteString(`<tt:Source><tt:SimpleItem Name="Device" Value="` + xmlEscape(ev.DeviceID) + `"/></tt:Source>`)
		b.WriteString(`<tt:Data><tt:SimpleItem Name="State" Value="` + xmlEscape(val) + `"/>`)
		b.WriteString(`<tt:SimpleItem Name="Type" Value="` + xmlEscape(ev.Type) + `"/>`)
		b.WriteString(`<tt:SimpleItem Name="MsgID" Value="` + xmlEscape(ev.MsgID) + `"/>`)
		b.WriteString(`</tt:Data></tt:Message></wsnt:Message></wsnt:NotificationMessage>`)
	}
	b.WriteString(`</tev:PullMessagesResponse></s:Body>`)
	return b.String()
}

// defaultDoorbellTopic 是 onvif.topics 未配置（含空数组）时的安全
// 回落值，与 config 默认值保持一致。
const defaultDoorbellTopic = "tns1:Doorbell/Stimulus"

// topicFor maps event types to configured ONVIF topics.
func (s *Server) topicFor(ev events.Event) (topic, value string) {
	switch ev.Type {
	case events.TypeDoorbellPress:
		// topics 允许配置为空数组（YAML `topics: []`）。该回调经
		// 事件总线在推送读协程内执行，当前无 recover 保护，直接
		// 下标访问会 panic 拖垮整个进程，因此空配置时回落到安全
		// 默认 topic（保证门铃事件继续下发），而不是崩溃或丢弃。
		if len(s.env.Cfg.ONVIF.Topics) == 0 {
			return defaultDoorbellTopic, "true"
		}
		return s.env.Cfg.ONVIF.Topics[0], "true"
	case events.TypeDeviceCalling:
		return "tns1:VideoSource/MotionAlarm", "true"
	case events.TypeDeviceAlarm:
		return "tns1:Device/Trigger/DigitalInput", "true"
	case events.TypeDeviceOnline:
		return "tns1:Device/HaDeviceEvent/DeviceDisconnectedIn", "false"
	case events.TypeDeviceOffline:
		return "tns1:Device/HaDeviceEvent/DeviceDisconnectedIn", "true"
	}
	return "", ""
}

func xsdTime(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05Z")
}

func systemDateAndTime() string {
	n := time.Now().UTC()
	return `<s:Body><tds:GetSystemDateAndTimeResponse><tds:SystemDateAndTime>` +
		`<tt:DateTimeType>NTP</tt:DateTimeType><tt:DaylightSavings>false</tt:DaylightSavings>` +
		`<tt:UTCDateTime><tt:Time><tt:Hour>` + itoa(n.Hour()) + `</tt:Hour><tt:Minute>` + itoa(n.Minute()) +
		`</tt:Minute><tt:Second>` + itoa(n.Second()) + `</tt:Second></tt:Time>` +
		`<tt:Date><tt:Year>` + itoa(n.Year()) + `</tt:Year><tt:Month>` + itoa(int(n.Month())) +
		`</tt:Month><tt:Day>` + itoa(n.Day()) + `</tt:Day></tt:Date></tt:UTCDateTime>` +
		`</tds:SystemDateAndTime></tds:GetSystemDateAndTimeResponse></s:Body>`
}

var _ sync.Mutex
