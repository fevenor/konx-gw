package onvif

import (
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

	// honor the requested wait (cap 30s) for at least one message
	timeout := 5 * time.Second
	if m := reTimeout.FindStringSubmatch(body); m != nil {
		if d, err := time.ParseDuration(strings.ToLower(strings.ReplaceAll(m[1], "PT", ""))); err == nil && d > 0 && d < 30*time.Second {
			timeout = d
		}
	}
	limit := 20
	if m := reMsgLimit.FindStringSubmatch(body); m != nil {
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

// topicFor maps event types to configured ONVIF topics.
func (s *Server) topicFor(ev events.Event) (topic, value string) {
	switch ev.Type {
	case events.TypeDoorbellPress:
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
