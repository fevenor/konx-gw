// 设备列表端点（2026-10-26 迁移）：
//
//	现行云端使用 Relationship/LoadDeviceInfo.ashx（与 LoginCheck.ashx 同一
//	api 主机、同一认证参数风格）；旧的 Account/Bind/SearchBindDev.ashx 在
//	官方云旧域名已全部 404（真机验证），仅保留为可配置回退（私有/存量
//	环境可能仍在提供）。
package cloud

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
)

// DeviceListMode 选择设备列表端点（config cloud.device_list，env
// KONX_CLOUD_DEVICE_LIST；空值等同 auto）。
type DeviceListMode string

const (
	// DeviceListAuto 默认：LoadDeviceInfo，失败或空列表时自动降级
	// SearchBindDev（日志说明原因）。
	DeviceListAuto DeviceListMode = "auto"
	// DeviceListLoadDeviceInfo 仅 LoadDeviceInfo（现行云端端点，不降级）。
	DeviceListLoadDeviceInfo DeviceListMode = "loaddeviceinfo"
	// DeviceListSearchBindDev 仅旧端点 SearchBindDev（官方云已 404）。
	DeviceListSearchBindDev DeviceListMode = "searchbinddev"
)

// DeviceCateDoorbell：门铃类设备的 deviceCate（LoadDeviceInfo 管道串第 5
// 字段，真机实测门铃=501）。
const DeviceCateDoorbell = 501

// LoadDeviceInfo 拉取账号绑定设备列表（Relationship/LoadDeviceInfo.ashx）。
//
// 请求参数与真机实测通过的请求逐字一致：完整认证集
// （UserID/SessionID/AppID/AppToken/Language/AppOS/AppName/AppVersion/
// PackageName/ApiVersion，与 LoginCheck 同风格——缺 Language/AppOS/AppName/
// ApiVersion 任何一个云端回 10901021 参数错）+ PreDeviceID=0 +
// RequestCount=20 + MyUpgradeFlag=1 + UpgCheckList=0 + NotReturnDeleted=1。
//
// 响应 DeviceInfo 为"|"分隔的管道串（字段序来自官方客户端资料，
// 真机实测确认）：
//
//	{?, ?, ?, devId, deviceCate, secretKey, remarkName, ?, ?}
//
// 前三后二字段语义未知（保底跳过）。每设备提取：devID(u32)、名称=
// remarkName、deviceCate（501=门铃类，见 IsDoorbell）、secretKey（加密的
// 主人密码，base64，存下来供后续解密用）。
func (c *Client) LoadDeviceInfo(ctx context.Context, s *Session) ([]BoundDevice, error) {
	q := url.Values{}
	q.Set("UserID", s.APIUserID())
	q.Set("SessionID", strconv.FormatUint(uint64(s.S1), 10))
	q.Set("AppID", c.appID())
	q.Set("AppToken", c.appToken())
	q.Set("Language", "zh-cn")
	q.Set("AppOS", "3")
	q.Set("AppName", c.appPackage())
	q.Set("AppVersion", c.appVersion())
	q.Set("PackageName", c.appPackage())
	q.Set("ApiVersion", "1")
	q.Set("PreDeviceID", "0")
	q.Set("RequestCount", "20")
	q.Set("MyUpgradeFlag", "1")
	q.Set("UpgCheckList", "0")
	q.Set("NotReturnDeleted", "1")

	u := "https://" + c.Host + "/Relationship/LoadDeviceInfo.ashx?" + q.Encode()
	if c.LogReq != nil {
		c.LogReq("POST", "https://"+c.Host+"/Relationship/LoadDeviceInfo.ashx?...")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, nil)
	if err != nil {
		return nil, err
	}
	// 实测（2026-10-27）：端点不校验 User-Agent（Go 默认/okhttp 均成功），
	// 不再设置任何自定义 UA。
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}
	var top map[string]json.RawMessage
	if err := json.Unmarshal(body, &top); err != nil {
		return nil, fmt.Errorf("LoadDeviceInfo response not JSON (%d): %.120s", resp.StatusCode, body)
	}
	if ec, ok := top["error_code"]; ok {
		var codeStr string
		_ = json.Unmarshal(ec, &codeStr)
		if codeStr != "" && codeStr != "0" {
			code, _ := strconv.Atoi(codeStr)
			return nil, &LoginError{Code: code}
		}
	}
	records, err := deviceInfoRecords(top)
	if err != nil {
		return nil, err
	}
	var out []BoundDevice
	for _, rec := range records {
		if d, ok := parseDevicePipeRecord(rec); ok {
			out = append(out, d)
		}
	}
	return out, nil
}

// deviceInfoRecords 从响应 JSON 提取 DeviceInfo 管道串记录，兼容两种
// 形态：字符串数组（每元素一条设备记录）/ 单一字符串（按换行拆分）。
func deviceInfoRecords(top map[string]json.RawMessage) ([]string, error) {
	raw, ok := top["DeviceInfo"]
	if !ok {
		return nil, errors.New("no DeviceInfo field in LoadDeviceInfo response")
	}
	var arr []string
	if err := json.Unmarshal(raw, &arr); err == nil {
		return arr, nil
	}
	var one string
	if err := json.Unmarshal(raw, &one); err == nil {
		var recs []string
		for _, ln := range strings.Split(one, "\n") {
			if strings.TrimSpace(ln) != "" {
				recs = append(recs, ln)
			}
		}
		return recs, nil
	}
	return nil, errors.New("DeviceInfo field is neither a string array nor a string")
}

// parseDevicePipeRecord 解析一条设备管道串。字段序（真机实测）：
// {?,?,?,devId,deviceCate,secretKey,remarkName,?,?} —— 下标 3/4/5/6 为已知
// 字段，前三后二语义未知。字段不足 7 个或 devId 非 u32 的记录整体跳过
// （容错：单条脏数据不拖垮整张列表）。
func parseDevicePipeRecord(rec string) (BoundDevice, bool) {
	f := strings.Split(rec, "|")
	if len(f) < 7 {
		return BoundDevice{}, false
	}
	id, err := strconv.ParseUint(strings.TrimSpace(f[3]), 10, 32)
	if err != nil || id == 0 {
		return BoundDevice{}, false
	}
	cate, _ := strconv.ParseUint(strings.TrimSpace(f[4]), 10, 32)
	return BoundDevice{
		DevID:      strconv.FormatUint(id, 10),
		DeviceCate: uint32(cate),
		SecretKey:  strings.TrimSpace(f[5]), // 加密的主人密码（base64）
		Name:       strings.TrimSpace(f[6]), // remarkName
	}, true
}

// ListDevices 按 ListMode 拉取设备列表（迁移策略）：
//   - auto（默认）：LoadDeviceInfo；请求失败或列表为空时降级尝试
//     SearchBindDev，并经 Client.Log（可选）说明原因。
//   - loaddeviceinfo / searchbinddev：固定端点，不降级。
//
// 两端点都空且无错时返回空列表（账号本就可能零设备）。
func (c *Client) ListDevices(ctx context.Context, s *Session) ([]BoundDevice, error) {
	mode := c.ListMode
	if mode == "" {
		mode = DeviceListAuto
	}
	switch mode {
	case DeviceListSearchBindDev:
		return c.SearchBindDev(ctx, s)
	case DeviceListLoadDeviceInfo:
		return c.LoadDeviceInfo(ctx, s)
	case DeviceListAuto:
		devs, err := c.LoadDeviceInfo(ctx, s)
		if err == nil && len(devs) > 0 {
			return devs, nil
		}
		why := fmt.Sprintf("err=%v", err)
		if err == nil {
			why = "empty list"
		}
		c.logInfo("LoadDeviceInfo 不可用，自动降级 SearchBindDev（旧端点）", "reason", why)
		fb, fbErr := c.SearchBindDev(ctx, s)
		if fbErr == nil && len(fb) > 0 {
			c.logInfo("SearchBindDev 回退成功", "n", len(fb))
			return fb, nil
		}
		if err != nil {
			return nil, fmt.Errorf("LoadDeviceInfo: %w (SearchBindDev fallback: %v)", err, fbErr)
		}
		return devs, nil
	default:
		return nil, fmt.Errorf("unknown device list mode %q (want auto|loaddeviceinfo|searchbinddev)", string(mode))
	}
}

func (c *Client) logInfo(msg string, args ...any) {
	if c.Log != nil {
		c.Log.Info(msg, args...)
	}
}
