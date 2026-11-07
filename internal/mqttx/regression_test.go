package mqttx

import (
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
)

// 记录实际发布的负载，不连接代理。
type reviewMQTTClient struct {
	paho.Client
	messages map[string]interface{}
}

func (c *reviewMQTTClient) IsConnected() bool { return true }
func (c *reviewMQTTClient) Publish(topic string, qos byte, retained bool, payload interface{}) paho.Token {
	c.messages[topic] = payload
	return nil
}

func TestRegressionMQTTDoorbellContract(t *testing.T) {
	p := New(config.Defaults(), slog.Default(), StateFuncs{})
	c := &reviewMQTTClient{messages: map[string]interface{}{}}
	p.cli = c
	p.OnEvent(events.Event{DeviceID: "10000001", Type: events.TypeDoorbellPress, Payload: `{}`})
	for topic, payload := range c.messages {
		if strings.HasSuffix(topic, "/event/doorbell") {
			var ev map[string]interface{}
			if err := json.Unmarshal([]byte(payload.(string)), &ev); err != nil {
				t.Fatal(err)
			}
			if ev["event_type"] != "press" {
				t.Fatalf("HA 事件负载缺少 event_type：%s", payload)
			}
			return
		}
	}
	t.Fatal("未发布门铃事件")
}

func TestMediaAvailabilityMatchesDiscovery(t *testing.T) {
	cfg := config.Defaults()
	cfg.Devices = []config.Device{{ID: "10000001", Name: "测试门铃"}}
	online := false
	p := New(cfg, slog.Default(), StateFuncs{DeviceOnline: func(string) bool { return online }})
	c := &reviewMQTTClient{messages: map[string]interface{}{}}
	p.cli = c
	p.publishDiscovery()
	var discovery map[string]interface{}
	if err := json.Unmarshal(c.messages["homeassistant/binary_sensor/konx_10000001_online/config"].([]byte), &discovery); err != nil {
		t.Fatal(err)
	}
	topic := discovery["state_topic"].(string)
	p.PublishStatus()
	if c.messages[topic] != "OFF" {
		t.Fatal("启动时未发布媒体连接状态")
	}
	online = true
	p.OnEvent(events.Event{Type: events.TypeSource, DeviceID: "10000001", Payload: `{}`})
	if c.messages[topic] != "ON" {
		t.Fatal("媒体恢复后未发布在线状态")
	}
	online = false
	p.OnEvent(events.Event{Type: events.TypeSource, DeviceID: "10000001", Payload: `{}`})
	if c.messages[topic] != "OFF" {
		t.Fatal("媒体断开后未发布离线状态")
	}
	if discovery["availability_topic"] != p.base+"/status" {
		t.Fatal("设备实体未绑定网关可用性")
	}
}
