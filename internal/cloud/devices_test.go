// 设备列表端点单测（假云，不访问真云）：
//   - LoadDeviceInfo 请求参数完整性（缺任何一个认证/分页参数回 10901021，
//     复刻真云行为）与管道串解析（真机实测值做 fixture）
//   - 容错：字段缺失/坏 devId 跳过、空列表、error_code、单字符串形态
//   - ListDevices 迁移策略：auto 自动降级 SearchBindDev / 端点固定不降级
package cloud

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// 真机实测 ground truth。
const (
	fixtureDoorbellRec = "x|y|z|10000001|501|Sz/4eUNmcc8VfWFd8tbBfV+MmVIxMfkB82U+BNT1UAo=|门铃|a|b"
	fixtureCameraRec   = "p|q|r|12345678|1|QUJD|客厅相机|c|d"
)

func testSess() *Session { return &Session{UID: 1234567, S1: 4242} }

func ldiJSON(devs ...string) string {
	b, _ := json.Marshal(map[string]any{"error_code": "0", "DeviceInfo": devs})
	return string(b)
}

func sbdJSON(devs []map[string]any) string {
	if devs == nil {
		devs = []map[string]any{{"dev_id": "999000", "dev_name": "fallback-cam", "online": true}}
	}
	b, _ := json.Marshal(map[string]any{"error_code": "0", "dev_list": devs})
	return string(b)
}

// requireLDIParams 校验 LoadDeviceInfo 的完整参数集（与 LoginCheck 同风格
// 的认证集 + PreDeviceID/RequestCount/MyUpgradeFlag/UpgCheckList/
// NotReturnDeleted）。缺/错任一 → false（调用方回 10901021，复刻真云）。
func requireLDIParams(t *testing.T, r *http.Request) bool {
	t.Helper()
	want := map[string]string{
		"UserID":           testSess().APIUserID(),
		"SessionID":        "4242",
		"AppID":            DemoAppID,
		"AppToken":         DemoAppToken,
		"Language":         "zh-cn",
		"AppOS":            "3",
		"AppName":          DemoPackage,
		"AppVersion":       "1",
		"PackageName":      DemoPackage,
		"ApiVersion":       "1",
		"PreDeviceID":      "0",
		"RequestCount":     "20",
		"MyUpgradeFlag":    "1",
		"UpgCheckList":     "0",
		"NotReturnDeleted": "1",
	}
	for k, v := range want {
		if got := r.Form.Get(k); got != v {
			t.Logf("param %s = %q, want %q", k, got, v)
			return false
		}
	}
	if r.Method != http.MethodPost {
		t.Errorf("method = %s, want POST", r.Method)
	}
	return true
}

// newFakeCloud 起一个 TLS 假云（两个端点都挂），返回客户端与两端点的命中
// 计数。ldi/sbd 为 nil 时用默认成功 fixture。
func newFakeCloud(t *testing.T, ldi, sbd func(r *http.Request) (int, string)) (*Client, *atomic.Int32, *atomic.Int32) {
	t.Helper()
	if ldi == nil {
		ldi = func(*http.Request) (int, string) { return 200, ldiJSON(fixtureDoorbellRec, fixtureCameraRec) }
	}
	if sbd == nil {
		sbd = func(*http.Request) (int, string) { return 200, sbdJSON(nil) }
	}
	var ldiHits, sbdHits atomic.Int32
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/Relationship/LoadDeviceInfo.ashx":
			ldiHits.Add(1)
			_ = r.ParseForm()
			if !requireLDIParams(t, r) {
				_, _ = w.Write([]byte(`{"error_code":"10901021","error":"传入参数格式错误"}`))
				return
			}
			code, body := ldi(r)
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
		case "/Account/Bind/SearchBindDev.ashx":
			sbdHits.Add(1)
			code, body := sbd(r)
			w.WriteHeader(code)
			_, _ = w.Write([]byte(body))
		default:
			t.Errorf("unexpected path %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)
	c := NewClient(strings.TrimPrefix(srv.URL, "https://"))
	c.HTTP = srv.Client()
	return c, &ldiHits, &sbdHits
}

// TestLoadDeviceInfoRequestAndParse：完整参数集下成功解析多设备管道串
// （请求若缺任何一个认证/分页参数，假云回 10901021 → 本测试即失败）。
func TestLoadDeviceInfoRequestAndParse(t *testing.T) {
	c, ldiHits, sbdHits := newFakeCloud(t, nil, nil)
	devs, err := c.LoadDeviceInfo(context.Background(), testSess())
	if err != nil {
		t.Fatalf("LoadDeviceInfo: %v", err)
	}
	if len(devs) != 2 {
		t.Fatalf("devices = %d, want 2", len(devs))
	}
	d := devs[0]
	if d.DevID != "10000001" || d.Name != "门铃" || d.DeviceCate != 501 || !d.IsDoorbell() {
		t.Fatalf("doorbell parsed wrong: %+v", d)
	}
	if d.SecretKey != "Sz/4eUNmcc8VfWFd8tbBfV+MmVIxMfkB82U+BNT1UAo=" {
		t.Fatalf("secretKey = %q", d.SecretKey)
	}
	if d.Online {
		t.Fatal("LoadDeviceInfo 不提供 online 字段，应默认 false")
	}
	if devs[1].DevID != "12345678" || devs[1].Name != "客厅相机" || devs[1].IsDoorbell() {
		t.Fatalf("camera parsed wrong: %+v", devs[1])
	}
	if ldiHits.Load() != 1 || sbdHits.Load() != 0 {
		t.Fatalf("hits ldi=%d sbd=%d, want 1/0（直接调用不触发回退）", ldiHits.Load(), sbdHits.Load())
	}
}

// TestLoadDeviceInfoTolerance：单条脏记录（字段不足 / devId 非 u32 / 零
// id）跳过不拖垮整表；deviceCate 非数字容错为 0；恰好 7 字段的记录保留。
func TestLoadDeviceInfoTolerance(t *testing.T) {
	recs := []string{
		"only|four|fields|x",                    // 字段不足 → 跳过
		"x|y|z|notanid|501|QUJD|坏ID|a|b",        // devId 非 u32 → 跳过
		"x|y|z|0|501|QUJD|零ID|a|b",              // devId=0 → 跳过
		"x|y|z|10000001|badcate|QUJD|分类非数字|a|b", // cate 容错为 0
		"x|y|z|12345678|501|QUJD|恰好七字段",         // 恰好 7 字段 → 保留
	}
	c, _, _ := newFakeCloud(t, func(*http.Request) (int, string) { return 200, ldiJSON(recs...) }, nil)
	devs, err := c.LoadDeviceInfo(context.Background(), testSess())
	if err != nil {
		t.Fatalf("LoadDeviceInfo: %v", err)
	}
	if len(devs) != 2 {
		t.Fatalf("devices = %d, want 2（坏记录跳过）: %+v", len(devs), devs)
	}
	if devs[0].DevID != "10000001" || devs[0].DeviceCate != 0 {
		t.Fatalf("cate-tolerant record wrong: %+v", devs[0])
	}
	if devs[1].DevID != "12345678" || devs[1].Name != "恰好七字段" {
		t.Fatalf("7-field record wrong: %+v", devs[1])
	}
}

// TestLoadDeviceInfoEmptyList：空列表 = 成功 + 零设备（触发 auto 降级是
// ListDevices 的职责，端点函数本身不报错）。
func TestLoadDeviceInfoEmptyList(t *testing.T) {
	c, _, _ := newFakeCloud(t, func(*http.Request) (int, string) { return 200, ldiJSON() }, nil)
	devs, err := c.LoadDeviceInfo(context.Background(), testSess())
	if err != nil {
		t.Fatalf("empty list must not error: %v", err)
	}
	if len(devs) != 0 {
		t.Fatalf("devices = %d, want 0", len(devs))
	}
}

// TestLoadDeviceInfoErrorShapes：error_code → *LoginError；非 JSON → 错误；
// DeviceInfo 单字符串（换行分隔）→ 逐条解析。
func TestLoadDeviceInfoErrorShapes(t *testing.T) {
	c, _, _ := newFakeCloud(t,
		func(*http.Request) (int, string) { return 200, `{"error_code":"10901025","error":"bad"}` }, nil)
	_, err := c.LoadDeviceInfo(context.Background(), testSess())
	le, ok := err.(*LoginError)
	if !ok || le.Code != 10901025 {
		t.Fatalf("err = %v, want LoginError 10901025", err)
	}

	c2, _, _ := newFakeCloud(t, func(*http.Request) (int, string) { return 200, "<html>404</html>" }, nil)
	if _, err := c2.LoadDeviceInfo(context.Background(), testSess()); err == nil {
		t.Fatal("non-JSON body must error")
	}

	c3, _, _ := newFakeCloud(t,
		func(*http.Request) (int, string) {
			return 200, `{"error_code":"0","DeviceInfo":"x|y|z|10000001|501|S1=|门铃|a|b\np|q|r|12345678|1|S2=|客厅|c|d"}`
		}, nil)
	devs, err := c3.LoadDeviceInfo(context.Background(), testSess())
	if err != nil {
		t.Fatalf("single-string DeviceInfo: %v", err)
	}
	if len(devs) != 2 || devs[0].DevID != "10000001" || devs[1].DevID != "12345678" {
		t.Fatalf("single-string parse wrong: %+v", devs)
	}
}

// TestListDevicesAutoFallbackOn404：现行云端旧端点 404 场景的镜像——
// LoadDeviceInfo 挂（404/非 JSON）时 auto 模式降级 SearchBindDev 成功。
func TestListDevicesAutoFallbackOn404(t *testing.T) {
	c, ldiHits, sbdHits := newFakeCloud(t, func(*http.Request) (int, string) { return 404, "not found" }, nil)
	c.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	devs, err := c.ListDevices(context.Background(), testSess())
	if err != nil {
		t.Fatalf("ListDevices: %v", err)
	}
	if len(devs) != 1 || devs[0].DevID != "999000" || devs[0].Name != "fallback-cam" {
		t.Fatalf("fallback result wrong: %+v", devs)
	}
	if ldiHits.Load() != 1 || sbdHits.Load() != 1 {
		t.Fatalf("hits ldi=%d sbd=%d, want 1/1", ldiHits.Load(), sbdHits.Load())
	}
}

// TestListDevicesAutoFallbackOnEmpty：LoadDeviceInfo 成功但空列表也降级
// （账号在旧端点才有数据的存量环境）。
func TestListDevicesAutoFallbackOnEmpty(t *testing.T) {
	c, ldiHits, sbdHits := newFakeCloud(t, func(*http.Request) (int, string) { return 200, ldiJSON() }, nil)
	devs, err := c.ListDevices(context.Background(), testSess())
	if err != nil {
		t.Fatalf("ListDevices: %v", err)
	}
	if len(devs) != 1 || devs[0].DevID != "999000" {
		t.Fatalf("fallback result wrong: %+v", devs)
	}
	if ldiHits.Load() != 1 || sbdHits.Load() != 1 {
		t.Fatalf("hits ldi=%d sbd=%d, want 1/1", ldiHits.Load(), sbdHits.Load())
	}
}

// TestListDevicesAutoBothFail：两端点都失败 → 错误以主端点为主、附带
// 回退结果说明。
func TestListDevicesAutoBothFail(t *testing.T) {
	c, _, _ := newFakeCloud(t,
		func(*http.Request) (int, string) { return 404, "not found" },
		func(*http.Request) (int, string) { return 500, "boom" })
	c.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	_, err := c.ListDevices(context.Background(), testSess())
	if err == nil || !strings.Contains(err.Error(), "LoadDeviceInfo") {
		t.Fatalf("err = %v, want primary LoadDeviceInfo error", err)
	}
}

// TestListDevicesAutoBothEmpty：两端点都成功且都空 → 空列表不报错
// （账号本就可能零设备）。
func TestListDevicesAutoBothEmpty(t *testing.T) {
	c, _, _ := newFakeCloud(t,
		func(*http.Request) (int, string) { return 200, ldiJSON() },
		func(*http.Request) (int, string) { return 200, sbdJSON([]map[string]any{}) })
	c.Log = slog.New(slog.NewTextHandler(io.Discard, nil))
	devs, err := c.ListDevices(context.Background(), testSess())
	if err != nil || len(devs) != 0 {
		t.Fatalf("both-empty: devs=%d err=%v, want 0/nil", len(devs), err)
	}
}

// TestListDevicesModePin：searchbinddev 固定旧端点（不请求 LoadDeviceInfo）；
// loaddeviceinfo 固定新端点（404 也不降级）；未知模式报错。
func TestListDevicesModePin(t *testing.T) {
	c, ldiHits, sbdHits := newFakeCloud(t, func(*http.Request) (int, string) { return 404, "not found" }, nil)
	c.ListMode = DeviceListSearchBindDev
	devs, err := c.ListDevices(context.Background(), testSess())
	if err != nil || len(devs) != 1 || devs[0].DevID != "999000" {
		t.Fatalf("searchbinddev mode: devs=%v err=%v", devs, err)
	}
	if ldiHits.Load() != 0 {
		t.Fatalf("searchbinddev mode must not hit LoadDeviceInfo (hits=%d)", ldiHits.Load())
	}
	if sbdHits.Load() != 1 {
		t.Fatalf("sbd hits = %d, want 1", sbdHits.Load())
	}

	c2, _, sbdHits2 := newFakeCloud(t, func(*http.Request) (int, string) { return 404, "not found" }, nil)
	c2.ListMode = DeviceListLoadDeviceInfo
	if _, err := c2.ListDevices(context.Background(), testSess()); err == nil {
		t.Fatal("loaddeviceinfo mode must surface the 404 error (no fallback)")
	}
	if sbdHits2.Load() != 0 {
		t.Fatalf("loaddeviceinfo mode must not hit SearchBindDev (hits=%d)", sbdHits2.Load())
	}

	c3, _, _ := newFakeCloud(t, nil, nil)
	c3.ListMode = DeviceListMode("bogus")
	if _, err := c3.ListDevices(context.Background(), testSess()); err == nil {
		t.Fatal("unknown mode must error")
	}
}
