// Package app wires every module into the running gateway.
package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/fevenor/konx-gw/internal/api"
	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/cloud"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
	"github.com/fevenor/konx-gw/internal/media"
	"github.com/fevenor/konx-gw/internal/mqttx"
	"github.com/fevenor/konx-gw/internal/onvif"
	"github.com/fevenor/konx-gw/internal/p2p/gwpass"
	"github.com/fevenor/konx-gw/internal/p2p/session"
	"github.com/fevenor/konx-gw/internal/rtspout"
	"github.com/fevenor/konx-gw/internal/talk"
)

// App owns every subsystem lifecycle.
type App struct {
	Cfg   *config.App
	Log   *slog.Logger
	Store *events.Store
	Auth  *auth.Service
	Eng   *events.Engine
	Sess  *session.Session
	Mgr   *media.Manager
	Rtsp  *rtspout.Server
	Onvif *onvif.Server
	API   *api.Server
	MQTT  *mqttx.Publisher
	Talk  *talk.Manager
	HLS   *media.HLSManager
	FF    *media.FFmpeg

	httpSrv  *http.Server
	onvifSrv *http.Server
}

// Build constructs the app (no side effects beyond local stores).
func Build(cfg *config.App, log *slog.Logger) (*App, error) {
	if err := os.MkdirAll(cfg.Web.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("data dir: %w", err)
	}
	store, err := events.Open(filepath.Join(cfg.Web.DataDir, "konx-gw.db"))
	if err != nil {
		return nil, err
	}
	authSvc, err := auth.New(store, log)
	if err != nil {
		return nil, err
	}
	// RTSP/ONVIF users: reuse loopback + a default "nvr" user created at
	// first boot with a generated password (printed once).
	nvrPass, _ := store.KVGet("nvr_password")
	if nvrPass == "" {
		nvrPass = authSvc.CreateSecret()
		_ = store.KVSet("nvr_password", nvrPass)
		fmt.Printf("=== konx-gw RTSP/ONVIF user 'nvr' password (shown once): %s ===\n", nvrPass)
	}
	authSvc.SetUser("nvr", nvrPass)

	reg := events.NewRegistry()
	reg.Register("CheckLogin", &events.CheckLoginDecoder{})
	// Official SDK sensor alarm types: 2=motion detect,
	// 13=doorbell push, 14=button-triggered emergency.
	// 14 stays device.alarm (payload preserved raw either way).
	doorbellTypes := map[uint32]bool{13: true}
	reg.Register("ALARM_MSG_FROM_SRV", events.AlarmDecoder{DoorbellTypes: doorbellTypes})
	reg.Register("CALLING", events.AlarmDecoder{})
	reg.SetFallback(events.UnknownDecoder{})
	eng := events.NewEngine(reg, store)

	a := &App{Cfg: cfg, Log: log, Store: store, Auth: authSvc, Eng: eng}

	// cloud + p2p session
	creds := cloud.Credentials{User: cfg.Cloud.User, PassMD5: cfg.Cloud.PassMD5}
	cc := cloud.NewClient(cfg.Cloud.APIHost)
	// 应用身份覆盖（cloud.app_id / cloud.app_token / cloud.app_version；
	// 缺省 demo 值）。
	cc.AppID = cfg.Cloud.AppID
	cc.AppToken = cfg.Cloud.AppToken
	cc.AppVersion = cfg.Cloud.AppVersion
	cc.AppPackage = cfg.Cloud.AppPackage
	// 设备列表端点策略（cloud.device_list / KONX_CLOUD_DEVICE_LIST）：
	// 默认 auto —— 现行云端 LoadDeviceInfo，失败/空时降级旧端点 SearchBindDev。
	cc.ListMode = cloud.DeviceListMode(cfg.Cloud.DeviceList)
	cc.Log = log
	a.Sess = session.New(session.Options{
		Login: func(ctx context.Context) (*cloud.Session, error) {
			return cc.Login(ctx, creds)
		},
		Heartbeat:    cfg.P2P.Heartbeat,
		ReconnectMin: cfg.P2P.Reconnect.Min,
		ReconnectMax: cfg.P2P.Reconnect.Max,
		Log:          log,
		Trace:        traceHook(cfg),
	}, session.Events{
		OnNotify: eng.IngestPush,
		OnLogin: func(cs *cloud.Session) {
			eng.Submit(events.CloudEvent("login", cs))
		},
		OnState: func(from, to session.State) {
			if to == session.StateReady {
				eng.Submit(events.SessionEvent("ready", serverStr(a.Sess)))
			} else if to == session.StateReconnecting {
				eng.Submit(events.SessionEvent("down", serverStr(a.Sess)))
			}
		},
		OnError: func(err error) {
			eng.Submit(events.SystemEvent("p2p_error", err.Error()))
		},
	})

	// media
	a.FF = &media.FFmpeg{Path: cfg.Media.FFmpegPath}
	a.Mgr = media.NewManager(cfg, log, eng)
	// p2p-av 源接线：呼叫建立走 session.AttachAV；treated 密码经
	// cloud LoadDeviceInfo → secretKey → gwpass 密码链解析（真机验证）。
	a.Mgr.SetP2PDialer(func(devID uint32, treatedPwd int) (media.P2PCaller, error) {
		return a.Sess.AttachAV(devID, treatedPwd)
	})
	a.Mgr.SetP2PPwdResolver(func(devID uint32) (int, error) {
		cs := a.Sess.CloudSession()
		if cs == nil {
			return 0, fmt.Errorf("云会话未就绪（等待 gute 登录）")
		}
		cc := cloud.NewClient(cfg.Cloud.APIHost)
		// 与主客户端同一应用身份（cloud.app_id / cloud.app_token /
		// cloud.app_version / cloud.app_package 覆盖）。
		cc.AppID = cfg.Cloud.AppID
		cc.AppToken = cfg.Cloud.AppToken
		cc.AppVersion = cfg.Cloud.AppVersion
		cc.AppPackage = cfg.Cloud.AppPackage
		devs, err := cc.ListDevices(context.Background(), cs)
		if err != nil {
			return 0, fmt.Errorf("LoadDeviceInfo: %w", err)
		}
		for _, d := range devs {
			if d.DevID != fmt.Sprintf("%d", devID) {
				continue
			}
			if d.SecretKey == "" {
				return 0, fmt.Errorf("设备 %d 无 secretKey", devID)
			}
			// 服务器按请求者 uid 派生 kb 加密
			seed := uint32(cs.UID) | 0x80000000
			plain, err := gwpass.DecryptSecretKey(d.SecretKey, seed)
			if err != nil {
				return 0, fmt.Errorf("DecryptSecretKey: %w", err)
			}
			treated := gwpass.Treated(plain)
			log.Info("treated 密码已解析", "dev", devID, "treated", treated)
			return int(treated), nil
		}
		return 0, fmt.Errorf("设备 %d 不在绑定列表", devID)
	})
	a.HLS = media.NewHLSManager(a.FF, cfg.Web.DataDir)
	a.Mgr.OnState(func(ss media.SourceState) {
		if a.API != nil {
			a.API.BroadcastState()
		}
	})
	// doorbell prewarm
	eng.Subscribe(func(ev events.Event) {
		if ev.Type == events.TypeDoorbellPress && cfg.Media.EventStreamHold > 0 {
			a.Mgr.Prewarm(ev.DeviceID)
		}
	})

	// rtsp egress
	lu, lp := authSvc.Loopback()
	a.Rtsp = rtspout.New(cfg.RTSP.Listen, a.Mgr, authSvc, func(kind string, up bool) {
		if a.API != nil {
			a.API.BroadcastClientChange()
		}
	}, log, cfg.AdvertisedRTSPPort())
	a.Rtsp.DescribeWait = cfg.RTSP.DescribeWait
	_ = lu
	_ = lp

	// talk
	a.Talk = talk.NewManager()
	a.Talk.OnChange = func(dev string, active bool, by string) {
		if a.API != nil {
			a.API.BroadcastState()
		}
	}

	// api + web
	a.API = api.New(api.Deps{
		Cfg: cfg, Auth: authSvc, Store: store, Engine: eng, Sess: a.Sess,
		Mgr: a.Mgr, Rtsp: a.Rtsp, Talk: a.Talk, HLS: a.HLS, FF: a.FF, Log: log,
	})

	// onvif
	if cfg.ONVIF.Enable {
		host := cfg.Advertise.Host
		a.Onvif = onvif.New(onvif.Env{
			Cfg: cfg, Auth: authSvc, Engine: eng, Store: store, Mgr: a.Mgr, Log: log,
			StreamURL: func(dev string, p media.Profile) string {
				return fmt.Sprintf("rtsp://%s:%d/konx/%s/%s", onvifHost(host), cfg.AdvertisedRTSPPort(), dev, p)
			},
			SnapshotURL: func(dev string, p media.Profile) string {
				return fmt.Sprintf("http://%s:%d/api/devices/%s/snapshot", onvifHost(host), cfg.AdvertisedHTTPPort(), dev)
			},
		})
	}

	// mqtt
	if cfg.MQTT.Enable && cfg.MQTT.Broker != "" {
		a.MQTT = mqttx.New(cfg, log, mqttx.StateFuncs{
			GWUp:  func() bool { return true },
			P2PUp: func() bool { return a.Sess.State() == session.StateReady },
		})
		eng.Subscribe(a.MQTT.OnEvent)
	}
	return a, nil
}

func onvifHost(host string) string {
	if host == "" {
		return "<gateway-host>"
	}
	return host
}

func serverStr(s *session.Session) string {
	if srv, _, ok := s.CurrentServer(); ok {
		return srv.Addr()
	}
	return ""
}

func traceHook(cfg *config.App) func(dir string, ft byte, wire []byte) {
	if !cfg.P2P.Trace {
		return nil
	}
	dir := filepath.Join(cfg.Web.DataDir, "trace")
	_ = os.MkdirAll(dir, 0o755)
	f, err := os.OpenFile(filepath.Join(dir, "p2p.trace"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil
	}
	return func(dir string, ft byte, wire []byte) {
		ts := time.Now().Format("15:04:05.000")
		fmt.Fprintf(f, "%s %s type=0x%02x len=%d %x\n", ts, dir, ft, len(wire), wire)
	}
}

// Run starts every subsystem and blocks until ctx is done.
func (a *App) Run(ctx context.Context) error {
	a.Eng.Submit(events.SystemEvent("startup", map[string]any{
		"version": api.Version, "source": a.Cfg.Media.Source,
	}))

	// p2p session
	go a.Sess.Run(ctx)

	// rtsp egress
	if err := a.Rtsp.Start(); err != nil {
		return fmt.Errorf("rtsp egress: %w", err)
	}
	go func() { _ = a.Rtsp.Wait() }()

	// http (web+api)
	a.httpSrv = &http.Server{Addr: a.Cfg.Web.Listen, Handler: a.API.Handler()}
	go func() {
		a.Log.Info("web listening", "addr", a.Cfg.Web.Listen)
		if err := a.httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			a.Log.Error("web server", "err", err)
		}
	}()

	// onvif
	if a.Onvif != nil {
		a.onvifSrv = &http.Server{Addr: a.Cfg.ONVIF.Listen, Handler: a.Onvif.Handler()}
		go func() {
			a.Log.Info("onvif listening", "addr", a.Cfg.ONVIF.Listen)
			if err := a.onvifSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				a.Log.Error("onvif server", "err", err)
			}
		}()
	}

	// mqtt
	if a.MQTT != nil {
		if err := a.MQTT.Start(a.Cfg.MQTT.Broker, a.Cfg.MQTT.User, a.Cfg.MQTT.Pass); err != nil {
			a.Log.Warn("mqtt connect failed (will not retry until restart)", "err", err)
			a.MQTT = nil
		}
	}

	// retention loop
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(24 * time.Hour):
				_ = a.Store.Cleanup(a.Cfg.Events.RetentionDays, a.Cfg.Events.MaxRows)
			}
		}
	}()

	<-ctx.Done()
	a.Shutdown()
	return nil
}

// Shutdown stops subsystems gracefully.
func (a *App) Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if a.httpSrv != nil {
		_ = a.httpSrv.Shutdown(ctx)
	}
	if a.onvifSrv != nil {
		_ = a.onvifSrv.Shutdown(ctx)
	}
	a.Rtsp.Close()
	a.Mgr.StopAll()
	if a.MQTT != nil {
		a.MQTT.Stop()
	}
	a.Sess.Stop()
	_ = a.Store.Close()
}

var _ = strings.TrimSpace
