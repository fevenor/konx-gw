package app

import (
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/fevenor/konx-gw/internal/config"
)

func TestGatewayPasswordEnvironment(t *testing.T) {
	// 显式清除宿主机覆盖，保证兼容性检查确实走旧数据库凭据。
	for _, key := range []string{"KONX_ADMIN_PASSWORD", "KONX_NVR_PASSWORD"} {
		t.Setenv(key, "")
		os.Unsetenv(key)
	}
	cfg := config.Defaults()
	cfg.Web.DataDir = t.TempDir()
	cfg.Media.Source = "off"
	cfg.ONVIF.Enable = false
	build := func() *App {
		t.Helper()
		a, err := Build(cfg, slog.Default())
		if err != nil {
			t.Fatal(err)
		}
		return a
	}
	a := build()
	if err := a.Auth.SetAdminPassword("旧管理密码"); err != nil {
		t.Fatal(err)
	}
	oldHash, _ := a.Store.KVGet("admin_password_hash")
	oldNVR, _ := a.Store.KVGet("nvr_password")
	a.Store.Close()
	for _, tc := range []struct{ name, password string }{
		{"单字符", "1"}, {"空串", ""}, {"空格与符号", " a$:'中 b "},
		{"超过旧哈希长度限制", strings.Repeat("长", 100)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KONX_ADMIN_PASSWORD", tc.password)
			t.Setenv("KONX_NVR_PASSWORD", tc.password)
			for i := 0; i < 2; i++ {
				a := build()
				if !a.Auth.CheckAdmin(tc.password) || !a.Auth.CheckRTSP("nvr", tc.password) {
					t.Error("环境变量密码应原样生效，重建后保持一致")
				}
				if a.Auth.CheckAdmin(tc.password+"x") || a.Auth.CheckRTSP("nvr", tc.password+"x") || a.Auth.CheckAdmin("旧管理密码") || a.Auth.CheckRTSP("nvr", oldNVR) {
					t.Error("不能接受错误密码或被覆盖的旧密码")
				}
				hash, _ := a.Store.KVGet("admin_password_hash")
				nvr, _ := a.Store.KVGet("nvr_password")
				if hash != oldHash || nvr != oldNVR {
					t.Error("环境变量不得改写旧数据库凭据")
				}
				a.Store.Close()
			}
		})
	}
	a = build()
	defer a.Store.Close()
	if !a.Auth.CheckAdmin("旧管理密码") || !a.Auth.CheckRTSP("nvr", oldNVR) {
		t.Fatal("移除环境变量后应继续使用已有数据库密码")
	}
	t.Run("空数据目录也沿用部署密码", func(t *testing.T) {
		t.Setenv("KONX_ADMIN_PASSWORD", "1")
		t.Setenv("KONX_NVR_PASSWORD", "2")
		cfg.Web.DataDir = t.TempDir()
		fresh := build()
		defer fresh.Store.Close()
		if !fresh.Auth.CheckAdmin("1") || !fresh.Auth.CheckRTSP("nvr", "2") {
			t.Fatal("空数据目录应直接使用部署密码")
		}
		if hash, _ := fresh.Store.KVGet("admin_password_hash"); hash != "" {
			t.Error("配置的管理密码不应写入数据库")
		}
		if nvr, _ := fresh.Store.KVGet("nvr_password"); nvr != "" {
			t.Error("配置的媒体密码不应写入数据库")
		}
	})
}
