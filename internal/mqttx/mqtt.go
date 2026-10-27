// Package mqttx publishes gateway state and events to an optional broker
// (DESIGN §9) including HA MQTT discovery.
package mqttx

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"

	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

type Publisher struct {
	cfg   *config.App
	log   *slog.Logger
	cli   paho.Client
	base  string
	gwUp  func() bool
	p2pUp func() bool
}

// StateFuncs report session states for the status topic.
type StateFuncs struct {
	GWUp  func() bool
	P2PUp func() bool
}

func New(cfg *config.App, log *slog.Logger, st StateFuncs) *Publisher {
	return &Publisher{cfg: cfg, log: log, base: "konx-gw/" + cfg.GwID, gwUp: st.GWUp, p2pUp: st.P2PUp}
}

// Start connects (with LWT) and returns; offline until connected.
func (p *Publisher) Start(broker, user, pass string) error {
	lwt := p.base + "/status"
	opts := paho.NewClientOptions().
		AddBroker(broker).
		SetClientID("konx-gw-"+p.cfg.GwID).
		SetWill(lwt, `{"status":"offline"}`, 1, true).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(c paho.Client) {
			p.log.Info("mqtt connected", "broker", broker)
			p.PublishStatus()
			if p.cfg.MQTT.Discovery {
				p.publishDiscovery()
			}
		})
	if user != "" {
		opts.SetUsername(user).SetPassword(pass)
	}
	p.cli = paho.NewClient(opts)
	if tok := p.cli.Connect(); tok.WaitTimeout(10*time.Second) && tok.Error() != nil {
		return tok.Error()
	}
	return nil
}

// PublishStatus refreshes the availability/status topic.
func (p *Publisher) PublishStatus() {
	if p.cli == nil || !p.cli.IsConnected() {
		return
	}
	p2p := "down"
	if p.p2pUp != nil && p.p2pUp() {
		p2p = "up"
	}
	payload := fmt.Sprintf(`{"status":"online","gw":"%s","p2p":"%s","ts":%d}`,
		p.cfg.GwID, p2p, time.Now().Unix())
	p.cli.Publish(p.base+"/status", 1, true, payload)
}

// OnEvent fans one engine event out to MQTT.
func (p *Publisher) OnEvent(ev events.Event) {
	if p.cli == nil || !p.cli.IsConnected() {
		return
	}
	topic := fmt.Sprintf("%s/device/%s/event/%s", p.base, ev.DeviceID, ev.Type)
	payload := fmt.Sprintf(`{"msg_id":%q,"type":%q,"ts":%d,"payload":%s}`,
		ev.MsgID, ev.Type, ev.TSRecv, ev.Payload)
	p.cli.Publish(topic, 0, false, payload)
	if ev.Type == events.TypeDoorbellPress {
		// canonical doorbell topic (non-retain)
		p.cli.Publish(fmt.Sprintf("%s/device/%s/event/doorbell", p.base, ev.DeviceID), 0, false,
			fmt.Sprintf(`{"msg_id":%q,"ts":%d,"type":"press"}`, ev.MsgID, ev.TSRecv))
	}
}

// publishDiscovery emits HA MQTT discovery messages.
func (p *Publisher) publishDiscovery() {
	for _, d := range p.cfg.Devices {
		// doorbell event entity
		p.publishJSON(fmt.Sprintf("homeassistant/event/konx_%s_doorbell/config", d.ID), map[string]any{
			"name":        d.Name + " Doorbell",
			"state_topic": fmt.Sprintf("%s/device/%s/event/doorbell", p.base, d.ID),
			"event_types": []string{"press"},
			"device":      p.deviceInfo(d),
			"unique_id":   "konx_" + d.ID + "_doorbell",
		})
		// online binary sensor
		p.publishJSON(fmt.Sprintf("homeassistant/binary_sensor/konx_%s_online/config", d.ID), map[string]any{
			"name":        d.Name + " Online",
			"state_topic": fmt.Sprintf("%s/device/%s/availability", p.base, d.ID),
			"device":      p.deviceInfo(d),
			"unique_id":   "konx_" + d.ID + "_online",
		})
	}
}

func (p *Publisher) deviceInfo(d config.Device) map[string]any {
	return map[string]any{
		"identifiers":  []string{"konx_" + d.ID},
		"name":         d.Name,
		"manufacturer": "Konx",
		"model":        "KW05 via konx-gw",
		"via_device":   "konx-gw-" + p.cfg.GwID,
	}
}

func (p *Publisher) publishJSON(topic string, payload any) {
	b, err := json.Marshal(payload)
	if err != nil {
		return
	}
	p.cli.Publish(topic, 1, true, b)
}

// Stop disconnects cleanly.
func (p *Publisher) Stop() {
	if p.cli != nil && p.cli.IsConnected() {
		p.cli.Publish(p.base+"/status", 1, true, `{"status":"offline"}`)
		p.cli.Disconnect(500)
	}
}
