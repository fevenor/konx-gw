// config 校验测试：p2p-av 合法分支与密码/设备约束。
package config

import (
	"strings"
	"testing"
	"time"
)

func p2pCfg() *App {
	c := Defaults()
	c.Cloud.User = "u"
	c.Cloud.PassMD5 = strings.Repeat("a", 32)
	c.Media.Source = "p2p-av"
	return c
}

// TestValidateP2PValid：p2p-av + 数字设备 id（密码直配或走云解析）合法。
func TestValidateP2PValid(t *testing.T) {
	c := p2pCfg()
	c.Devices = []Device{{ID: "10000001"}}
	if err := c.Validate(); err != nil {
		t.Fatalf("p2p-av 合法配置被拒: %v", err)
	}
	if c.Media.P2P.HardIdle != 15*time.Minute {
		t.Fatalf("HardIdle 默认 = %v", c.Media.P2P.HardIdle)
	}
	// 直配 treated 密码同样合法
	c.Media.P2P.TreatedPassword = 387648820
	if err := c.Validate(); err != nil {
		t.Fatalf("直配 treated 密码被拒: %v", err)
	}
}

// TestValidateP2PErrors：缺设备 / 非数字 id / 负密码 拒绝。
func TestValidateP2PErrors(t *testing.T) {
	c := p2pCfg()
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "device") {
		t.Fatalf("无设备必须报错: %v", err)
	}
	c.Devices = []Device{{ID: "front-bell"}}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "numeric") {
		t.Fatalf("非数字 id 必须报错: %v", err)
	}
	c.Devices = []Device{{ID: "10000001"}}
	c.Media.P2P.TreatedPassword = -1
	if err := c.Validate(); err == nil {
		t.Fatal("负 treated 密码必须报错")
	}
}

// TestCloudIdentityOverrideValid：cloud.app_id（32 hex）/ cloud.app_token
// （64 hex）可选覆盖，格式正确即通过；留空（默认 demo 身份）也通过。
func TestCloudIdentityOverrideValid(t *testing.T) {
	c := p2pCfg()
	c.Devices = []Device{{ID: "10000001"}}
	if err := c.Validate(); err != nil {
		t.Fatalf("默认（未配置身份覆盖）必须合法: %v", err)
	}
	c.Cloud.AppID = strings.Repeat("a", 32)
	c.Cloud.AppToken = strings.Repeat("0123456789abcdef", 4)
	if err := c.Validate(); err != nil {
		t.Fatalf("格式正确的覆盖必须合法: %v", err)
	}
}

// TestCloudIdentityOverrideErrors：长度不符 / 非小写十六进制 拒绝。
func TestCloudIdentityOverrideErrors(t *testing.T) {
	c := p2pCfg()
	c.Devices = []Device{{ID: "10000001"}}
	c.Cloud.AppID = strings.Repeat("a", 31)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_id") {
		t.Fatalf("AppID 长度错误必须报错: %v", err)
	}
	c.Cloud.AppID = strings.Repeat("A", 32)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_id") {
		t.Fatalf("AppID 大写必须报错: %v", err)
	}
	c.Cloud.AppID = strings.Repeat("g", 32)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_id") {
		t.Fatalf("AppID 非十六进制必须报错: %v", err)
	}
	c.Cloud.AppID = ""
	c.Cloud.AppToken = strings.Repeat("a", 63)
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_token") {
		t.Fatalf("AppToken 长度错误必须报错: %v", err)
	}
}

// TestCloudIdentityEnvOverride：KONX_CLOUD_APP_ID / KONX_CLOUD_APP_TOKEN
// 环境变量覆盖生效。
func TestCloudIdentityEnvOverride(t *testing.T) {
	t.Setenv("KONX_CLOUD_APP_ID", strings.Repeat("b", 32))
	t.Setenv("KONX_CLOUD_APP_TOKEN", strings.Repeat("c", 64))
	c := Defaults()
	c.applyEnv()
	if c.Cloud.AppID != strings.Repeat("b", 32) {
		t.Fatalf("AppID env 覆盖失效: %q", c.Cloud.AppID)
	}
	if c.Cloud.AppToken != strings.Repeat("c", 64) {
		t.Fatalf("AppToken env 覆盖失效: %q", c.Cloud.AppToken)
	}
}

// TestCloudAppVersionOverride：KONX_CLOUD_APP_VERSION 环境变量覆盖生效；
// 非数字 app_version 被拒。
func TestCloudAppVersionOverride(t *testing.T) {
	t.Setenv("KONX_CLOUD_APP_VERSION", "2000000")
	c := Defaults()
	c.applyEnv()
	if c.Cloud.AppVersion != "2000000" {
		t.Fatalf("AppVersion env 覆盖失效: %q", c.Cloud.AppVersion)
	}
	c2 := p2pCfg()
	c2.Devices = []Device{{ID: "10000001"}}
	c2.Cloud.AppVersion = "1a"
	if err := c2.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_version") {
		t.Fatalf("非数字 AppVersion 必须报错: %v", err)
	}
}

// TestCloudAppPackageOverride：KONX_CLOUD_APP_PACKAGE 环境变量覆盖生效；
// 非法包名字符被拒。
func TestCloudAppPackageOverride(t *testing.T) {
	t.Setenv("KONX_CLOUD_APP_PACKAGE", "com.konx.example")
	c := Defaults()
	c.applyEnv()
	if c.Cloud.AppPackage != "com.konx.example" {
		t.Fatalf("AppPackage env 覆盖失效: %q", c.Cloud.AppPackage)
	}
	c2 := p2pCfg()
	c2.Devices = []Device{{ID: "10000001"}}
	c2.Cloud.AppPackage = "bad package!"
	if err := c2.Validate(); err == nil || !strings.Contains(err.Error(), "cloud.app_package") {
		t.Fatalf("非法包名必须报错: %v", err)
	}
}
