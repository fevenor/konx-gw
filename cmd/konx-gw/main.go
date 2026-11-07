// konx-gw：用于接入 Home Assistant 的 konx 门铃网关。
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/fevenor/konx-gw/internal/app"
	"github.com/fevenor/konx-gw/internal/config"
)

var version = "dev"

func main() {
	cfgPath := flag.String("config", "/etc/konx-gw/config.yaml", "config file path")
	flag.Parse()

	if flag.Arg(0) == "healthcheck" {
		// 容器健康检查：按与 server 主命令相同的配置解析取得实际
		// web.listen 地址再探测 /healthz（C3-5）——旧实现硬编码 8080，
		// web.listen 配成非默认端口后探测必然失败，容器会被判死而
		// 无限重启。无有效配置时保持 8080 默认。
		resp, err := httpGet(healthcheckURL(healthcheckListen(*cfgPath)))
		if err != nil || resp != "ok" {
			os.Exit(1)
		}
		os.Exit(0)
	}

	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: levelOf()}))
	slog.SetDefault(log)

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		if os.IsNotExist(err) {
			log.Warn("config file missing, using defaults + env", "path", *cfgPath)
			cfg, err = config.Load("")
			if err != nil {
				log.Error("config", "err", err)
				os.Exit(1)
			}
		} else {
			log.Error("config", "err", err)
			os.Exit(1)
		}
	}

	a, err := app.Build(cfg, log)
	if err != nil {
		log.Error("build", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := a.Run(ctx); err != nil {
		log.Error("run", "err", err)
		os.Exit(1)
	}
	log.Info("konx-gw stopped", "version", version)
}

func levelOf() slog.Level {
	switch os.Getenv("KONX_LOG_LEVEL") {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	default:
		return slog.LevelInfo
	}
}

// healthcheckListen 按与主命令一致的路径解析配置并返回 web.listen：
// 配置文件缺失时退回"默认值 + 环境变量"（与主命令的兜底一致）；仍拿
// 不到有效配置则返回空串（探测端口回退默认 8080）。
func healthcheckListen(cfgPath string) string {
	cfg, err := config.Load(cfgPath)
	if err == nil {
		return cfg.Web.Listen
	}
	if os.IsNotExist(err) {
		if cfg, err = config.Load(""); err == nil {
			return cfg.Web.Listen
		}
	}
	return ""
}

// healthcheckURL 把 web.listen 地址换成本机回环探测 URL：无论监听
// host 是通配（":9090"/"0.0.0.0:9090"）、IPv6（"[::]:9090"）还是具体
// IP/主机名，探测一律走 127.0.0.1；解析不出端口时用默认 8080。
func healthcheckURL(listen string) string {
	port := ""
	if i := strings.LastIndex(listen, ":"); i >= 0 {
		port = listen[i+1:]
	}
	if port == "" {
		port = "8080"
	}
	return "http://127.0.0.1:" + port + "/healthz"
}

func httpGet(url string) (string, error) {
	tr := &httpTransport{}
	return tr.get(url)
}
