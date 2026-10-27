// Package config loads and validates konx-gw configuration (DESIGN §13).
package config

import (
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

type App struct {
	GwID  string `yaml:"gw_id"`
	Cloud struct {
		User    string `yaml:"user"`
		PassMD5 string `yaml:"pass_md5"`
		APIHost string `yaml:"api_host"`
		// DeviceList 选择设备列表端点：auto（默认，LoadDeviceInfo 失败/空
		// 时降级 SearchBindDev）| loaddeviceinfo | searchbinddev。
		DeviceList string `yaml:"device_list"`
		// AppID/AppToken 可选覆盖云端登录的应用身份（LoginCheck/SearchBindDev/
		// LoadDeviceInfo 的 AppID/AppToken 参数）。缺省保持 GwellDemo 内置
		// 身份（向后兼容）。动机：云端对 demo 身份有严格呼叫频率门控
		// （15-30 分钟惩罚窗），生产 App 身份不受限；身份参数由部署方自备
		// （由部署方自行持有）。
		AppID    string `yaml:"app_id"`
		AppToken string `yaml:"app_token"`
		// AppVersion 可选覆盖登录参数 AppVersion（默认 "1"）。生产 App
		// 发送位压缩版本号（纯数字，由部署方自备）。
		AppVersion string `yaml:"app_version"`
		// AppPackage 可选覆盖登录参数 AppName/PackageName（默认 demo
		// 包 com.gwelldemo；生产 App 两者均为自身包名，身份参数由部署方
		// 自备（由部署方自行持有））。
		AppPackage string `yaml:"app_package"`
	} `yaml:"cloud"`
	P2P struct {
		Trace     bool          `yaml:"trace"`
		Heartbeat time.Duration `yaml:"heartbeat"`
		Reconnect struct {
			Min time.Duration `yaml:"min"`
			Max time.Duration `yaml:"max"`
		} `yaml:"reconnect"`
	} `yaml:"p2p"`
	Devices []Device `yaml:"devices"`
	Media   struct {
		Source                 string        `yaml:"source"` // external-rtsp | device-rtsp | p2p-av | off
		ExternalURL            string        `yaml:"external_url"`
		DeviceRTSPExclusiveACK bool          `yaml:"device_rtsp_exclusive_ack"`
		PullWithFFmpeg         bool          `yaml:"pull_with_ffmpeg"`
		EventStreamHold        time.Duration `yaml:"event_stream_hold"`
		IdleStop               time.Duration `yaml:"idle_stop"`
		FFmpegPath             string        `yaml:"ffmpeg_path"`
		P2P                    struct {
			// TreatedPassword 直配 treated 密码（u32；省缺时取流启动经
			// cloud LoadDeviceInfo→gwpass 密码链解析）。
			TreatedPassword int `yaml:"treated_password"`
			// HardIdle 是 p2p-av 槽位的硬空闲时长（默认 15m——P2P
			// 呼叫建立成本高且频繁挂断有节流黑名单风险，实测结论）。
			// session_hold=true 时到期动作由挂断改为暂停保持。
			HardIdle time.Duration `yaml:"hard_idle"`
			// SessionHold：cmd=1 会话保持模式（默认 false，v1 挂断语义，
			// 向后兼容）。true 时观众全部离开（HardIdle 到期）不挂断呼叫，
			// 改为暂停——KCP/meter 链路存活、描述缓存保留，新观众到来在
			// 同一条活链路上重发 AvStreamCtl cmd=1 复活媒体。动机（真机
			// 实证）：设备在会话存活期对重复 cmd=1 会再次 ACCEPT（实测
			// ground truth，ctl=[6,2,2]）；而完整拆链（CLOSE）后
			// 15-30 分钟内重呼会被服务器节流（登录后零推送）。
			SessionHold bool `yaml:"session_hold"`
		} `yaml:"p2p"`
	} `yaml:"media"`
	RTSP struct {
		Listen string `yaml:"listen"`
		// DescribeWait：DESCRIBE/SETUP 在源已发起但描述未就绪（p2p-av
		// 呼叫建立 ~5-8s）时的最长等待（默认 10s；仅已有源槽位时等待）。
		DescribeWait time.Duration `yaml:"describe_wait"`
	} `yaml:"rtsp"`
	ONVIF struct {
		Enable         bool     `yaml:"enable"`
		Listen         string   `yaml:"listen"`
		AllowAnonymous bool     `yaml:"allow_anonymous"`
		Topics         []string `yaml:"topics"`
	} `yaml:"onvif"`
	Advertise struct {
		Host     string `yaml:"host"`
		RTSPPort int    `yaml:"rtsp_port"`
		HTTPPort int    `yaml:"http_port"`
	} `yaml:"advertise"`
	MQTT struct {
		Enable    bool   `yaml:"enable"`
		Broker    string `yaml:"broker"`
		User      string `yaml:"user"`
		Pass      string `yaml:"pass"`
		Discovery bool   `yaml:"discovery"`
	} `yaml:"mqtt"`
	Web struct {
		Listen  string `yaml:"listen"`
		DataDir string `yaml:"data_dir"`
	} `yaml:"web"`
	Events struct {
		RetentionDays int  `yaml:"retention_days"`
		MaxRows       int  `yaml:"max_rows"`
		KeepRaw       bool `yaml:"keep_raw"`
	} `yaml:"events"`
	Log struct {
		Level  string `yaml:"level"`
		Format string `yaml:"format"`
	} `yaml:"log"`
}

type Device struct {
	ID   string `yaml:"id"`
	Name string `yaml:"name"`
	IP   string `yaml:"ip"`
	RTSP struct {
		Transport string `yaml:"transport"` // tcp | udp
		User      string `yaml:"user"`
		Pass      string `yaml:"pass"` // "auto" → MD5(id)
	} `yaml:"rtsp"`
}

// Defaults returns the configuration with design defaults applied.
func Defaults() *App {
	c := &App{GwID: "front-gate"}
	c.Cloud.APIHost = "api1.cloudlinks.cn"
	c.Cloud.DeviceList = "auto"
	c.P2P.Heartbeat = 5 * time.Second
	c.P2P.Reconnect.Min = 5 * time.Second
	c.P2P.Reconnect.Max = 60 * time.Second
	c.Media.Source = "external-rtsp"
	c.Media.EventStreamHold = 15 * time.Second
	c.Media.IdleStop = 60 * time.Second
	c.Media.P2P.HardIdle = 15 * time.Minute
	c.RTSP.DescribeWait = 10 * time.Second
	c.Media.FFmpegPath = "/usr/bin/ffmpeg"
	c.RTSP.Listen = ":8554"
	c.ONVIF.Enable = true
	c.ONVIF.Listen = ":8000"
	c.ONVIF.AllowAnonymous = true
	c.ONVIF.Topics = []string{"tns1:Doorbell/Stimulus"}
	c.Web.Listen = ":8080"
	c.Web.DataDir = "/data"
	c.Events.RetentionDays = 90
	c.Events.MaxRows = 100000
	c.Events.KeepRaw = true
	c.Log.Level = "info"
	c.Log.Format = "json"
	return c
}

// Load reads a YAML file, applies env overrides (KONX_* / KONX_CLOUD_*),
// validates and returns the config.
func Load(path string) (*App, error) {
	c := Defaults()
	if path != "" {
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, err
		}
		if err := yaml.Unmarshal(b, c); err != nil {
			return nil, fmt.Errorf("parse %s: %w", path, err)
		}
	}
	c.applyEnv()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *App) applyEnv() {
	if v := os.Getenv("KONX_CLOUD_USER"); v != "" {
		c.Cloud.User = v
	}
	if v := os.Getenv("KONX_CLOUD_PASS_MD5"); v != "" {
		c.Cloud.PassMD5 = v
	}
	if v := os.Getenv("KONX_CLOUD_PASS"); v != "" {
		// convenience: plaintext → md5 (never stored)
		c.Cloud.PassMD5 = md5hex(v)
	}
	if v := os.Getenv("KONX_CLOUD_DEVICE_LIST"); v != "" {
		c.Cloud.DeviceList = v
	}
	if v := os.Getenv("KONX_CLOUD_APP_ID"); v != "" {
		c.Cloud.AppID = v
	}
	if v := os.Getenv("KONX_CLOUD_APP_TOKEN"); v != "" {
		c.Cloud.AppToken = v
	}
	if v := os.Getenv("KONX_CLOUD_APP_VERSION"); v != "" {
		c.Cloud.AppVersion = v
	}
	if v := os.Getenv("KONX_CLOUD_APP_PACKAGE"); v != "" {
		c.Cloud.AppPackage = v
	}
	if v := os.Getenv("KONX_MEDIA_SOURCE"); v != "" {
		c.Media.Source = v
	}
	if v := os.Getenv("KONX_MEDIA_EXTERNAL_URL"); v != "" {
		c.Media.ExternalURL = v
	}
	if v := os.Getenv("KONX_MQTT_BROKER"); v != "" {
		c.MQTT.Broker = v
		c.MQTT.Enable = true
	}
	if v := os.Getenv("KONX_WEB_LISTEN"); v != "" {
		c.Web.Listen = v
	}
	if v := os.Getenv("KONX_DATA_DIR"); v != "" {
		c.Web.DataDir = v
	}
	if v := os.Getenv("KONX_LOG_LEVEL"); v != "" {
		c.Log.Level = v
	}
}

// Validate enforces the design invariants.
func (c *App) Validate() error {
	switch c.Media.Source {
	case "external-rtsp":
		if c.Media.ExternalURL == "" {
			return fmt.Errorf("media.source=external-rtsp requires media.external_url")
		}
	case "device-rtsp":
		if !c.Media.DeviceRTSPExclusiveACK {
			return fmt.Errorf("media.source=device-rtsp requires device_rtsp_exclusive_ack: true " +
				"(device RTSP concurrency is 1; NVR keeps the slot otherwise)")
		}
		if len(c.Devices) == 0 || c.Devices[0].IP == "" {
			return fmt.Errorf("media.source=device-rtsp requires a device with ip")
		}
	case "p2p-av":
		// 设备侧经 KCP/AvStreamCtl 取流（internal/p2p/av），需要数字设备
		// id 构建 CALLING；treated 密码可直配或运行时经 cloud 密码链解析。
		if len(c.Devices) == 0 {
			return fmt.Errorf("media.source=p2p-av requires at least one device")
		}
		for _, d := range c.Devices {
			if _, err := strconv.ParseUint(d.ID, 10, 32); err != nil {
				return fmt.Errorf("media.source=p2p-av requires numeric device ids, %q invalid", d.ID)
			}
		}
		if c.Media.P2P.TreatedPassword < 0 {
			return fmt.Errorf("media.p2p.treated_password must be >= 0")
		}
	case "off":
	default:
		return fmt.Errorf("unknown media.source %q", c.Media.Source)
	}
	if c.Cloud.User == "" {
		return fmt.Errorf("cloud.user (or KONX_CLOUD_USER) is required")
	}
	if c.Cloud.PassMD5 == "" {
		return fmt.Errorf("cloud.pass_md5 (or KONX_CLOUD_PASS_MD5) is required")
	}
	switch c.Cloud.DeviceList {
	case "", "auto", "loaddeviceinfo", "searchbinddev":
	default:
		return fmt.Errorf("cloud.device_list %q invalid (want auto|loaddeviceinfo|searchbinddev)", c.Cloud.DeviceList)
	}
	// AppID/AppToken 覆盖可单独提供（服务器各自校验），但格式必须符合
	// 官方 SDK 惯例（AppID=32 hex，AppToken=64 hex），写错立即报错胜过运行
	// 时云端 10901050 应用身份错。
	if err := validateHex("cloud.app_id", c.Cloud.AppID, 32); err != nil {
		return err
	}
	if err := validateHex("cloud.app_token", c.Cloud.AppToken, 64); err != nil {
		return err
	}
	// AppVersion 覆盖必须是纯数字（服务器端解析为整数）。
	if c.Cloud.AppVersion != "" {
		for _, r := range c.Cloud.AppVersion {
			if r < '0' || r > '9' {
				return fmt.Errorf("cloud.app_version %q must be pure numeric", c.Cloud.AppVersion)
			}
		}
	}
	// AppPackage 覆盖必须是 Android 包名形态（字母数字点下划线）。
	if c.Cloud.AppPackage != "" {
		for _, r := range c.Cloud.AppPackage {
			if !(r == '.' || r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9')) {
				return fmt.Errorf("cloud.app_package %q must look like an Android package name", c.Cloud.AppPackage)
			}
		}
	}
	for _, d := range c.Devices {
		if d.ID == "" {
			return fmt.Errorf("device entry missing id")
		}
		if d.RTSP.Pass == "" {
			d.RTSP.Pass = "auto"
		}
	}
	return nil
}

// CloudCreds builds the cloud credentials from config+env.
func (c *App) CloudCreds() (user, passMD5 string) {
	return c.Cloud.User, c.Cloud.PassMD5
}

// AdvertisedRTSPPort honors the port-mapping override.
func (c *App) AdvertisedRTSPPort() int {
	if c.Advertise.RTSPPort != 0 {
		return c.Advertise.RTSPPort
	}
	return portOf(c.RTSP.Listen, 8554)
}

// AdvertisedHTTPPort honors the port-mapping override.
func (c *App) AdvertisedHTTPPort() int {
	if c.Advertise.HTTPPort != 0 {
		return c.Advertise.HTTPPort
	}
	return portOf(c.Web.Listen, 8080)
}

// AdvertisedONVIFPort derives the ONVIF port from its listen address.
func (c *App) AdvertisedONVIFPort() int { return portOf(c.ONVIF.Listen, 8000) }

func portOf(listen string, def int) int {
	p := listen
	if i := strings.LastIndex(listen, ":"); i >= 0 {
		p = listen[i+1:]
	}
	if n, err := strconv.Atoi(p); err == nil && n > 0 {
		return n
	}
	return def
}

func md5hex(s string) string {
	h := md5.Sum([]byte(s))
	return hex.EncodeToString(h[:])
}

// validateHex 校验可选的十六进制身份字段：空=未配置（合法），否则必须
// 恰为 wantN 位小写/大写十六进制（统一小写存储）。
func validateHex(name, v string, wantN int) error {
	if v == "" {
		return nil
	}
	if len(v) != wantN {
		return fmt.Errorf("%s must be exactly %d hex chars, got %d", name, wantN, len(v))
	}
	for _, r := range v {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return fmt.Errorf("%s must be lowercase hex", name)
		}
	}
	return nil
}
