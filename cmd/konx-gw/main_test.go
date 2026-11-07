package main

// healthcheck 地址推导测试（C3-5）：healthcheck 必须按与 server 相同的
// 配置解析取实际 web.listen，不能硬编码 8080。

import (
	"os"
	"path/filepath"
	"testing"
)

func TestHealthcheckURL(t *testing.T) {
	cases := []struct {
		listen string
		want   string
	}{
		{"", "http://127.0.0.1:8080/healthz"},             // 无配置保持默认 8080
		{":8080", "http://127.0.0.1:8080/healthz"},        // 默认监听
		{":9090", "http://127.0.0.1:9090/healthz"},        // 非默认端口
		{"0.0.0.0:9090", "http://127.0.0.1:9090/healthz"}, // 通配绑定
		{"127.0.0.1:9001", "http://127.0.0.1:9001/healthz"},
		{"[::]:9090", "http://127.0.0.1:9090/healthz"}, // IPv6 通配
		{"localhost:9090", "http://127.0.0.1:9090/healthz"},
	}
	for _, tc := range cases {
		if got := healthcheckURL(tc.listen); got != tc.want {
			t.Errorf("healthcheckURL(%q) = %q，期望 %q", tc.listen, got, tc.want)
		}
	}
}

func TestHealthcheckListenFromConfig(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	yaml := `
cloud:
  user: demo-user
  pass_md5: "0123456789abcdef0123456789abcdef"
media:
  source: off
web:
  listen: ":9091"
`
	if err := os.WriteFile(path, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}
	if got := healthcheckListen(path); got != ":9091" {
		t.Fatalf("healthcheckListen 应读出配置的 web.listen :9091，得到 %q", got)
	}
	// 环境变量路径与主命令一致（KONX_WEB_LISTEN 覆盖）
	t.Setenv("KONX_WEB_LISTEN", ":9092")
	if got := healthcheckListen(path); got != ":9092" {
		t.Fatalf("KONX_WEB_LISTEN 覆盖失效: %q", got)
	}
}
