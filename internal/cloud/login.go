// Package cloud implements the konx cloud HTTP API client:
// LoginCheck.ashx (protocol §2) and the bound-device list —
// Relationship/LoadDeviceInfo.ashx on the current cloud (the legacy
// Account/Bind/SearchBindDev.ashx returns 404 on the legacy hosts and is
// kept only as a configurable fallback; see devices.go).
package cloud

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Fixed GwellDemo application identity.
const (
	DemoAppID     = "1e9a2c3ead108413e8218a639c540e44"
	DemoAppToken  = "7db7b2bff80a025a3dad546a4d5a6c3ee545568d4e0ce9609c0585c71c287d08"
	DemoPackage   = "com.gwelldemo"
	demoUserAgent = "okhttp/4.10.0"
)

// LoginError carries the server error_code for caller-side handling.
type LoginError struct {
	Code int
	Msg  string
}

func (e *LoginError) Error() string {
	return fmt.Sprintf("cloud error %d: %s", e.Code, e.Msg)
}

// Well-known error codes (protocol §2.3).
const (
	ErrParamFormat  = 10901021
	ErrAppIdentity  = 10901050
	ErrUserIDFormat = 10901026
	ErrMissingParam = 10901020
	ErrBadCreds     = 10901025 // account exists, credential rejected
	ErrNoAccount    = 10902011 // account does not exist
)

// Credentials is the login identity. PassMD5 takes precedence; when only
// Pass is set the MD5 is computed here and never logged.
type Credentials struct {
	User    string
	Pass    string
	PassMD5 string
}

// MD5 returns the lowercase hex MD5 of the password.
func (c Credentials) MD5() string {
	if c.PassMD5 != "" {
		return c.PassMD5
	}
	h := md5.Sum([]byte(c.Pass))
	return hex.EncodeToString(h[:])
}

// Session is the result of a successful login.
type Session struct {
	UID     uint64 // u32 form of the signed UserID
	C1      uint32 // P2PVerifyCode1
	C2      uint32 // P2PVerifyCode2
	S1      uint32 // SessionID
	S2      uint32 // SessionID2
	Raw     map[string]string
	LoginAt time.Time
}

// APIUserID returns the "0"+low-31-bits decimal form required by later
// .ashx calls (live-verified).
func (s *Session) APIUserID() string {
	return "0" + strconv.FormatUint(uint64(uint32(s.UID))&0x7FFFFFFF, 10)
}

// Client talks to the cloud API.
type Client struct {
	Host   string // e.g. api1.cloudlinks.cn
	HTTP   *http.Client
	LogReq func(method, url string) // optional request trace hook (sanitized)

	// AppID/AppToken 可选覆盖应用身份（默认 GwellDemo 值，向后兼容）。
	// 生产 App 持有自己的身份：云端对 demo 身份有严格呼叫频率门控
	// （真机实测：demo 15-30 分钟惩罚窗，生产身份无间隔限制）。
	// 身份参数由部署方自备（由部署方自行持有）。
	AppID string
	// AppToken 同上。
	AppToken string
	// AppVersion 可选覆盖登录参数 AppVersion（默认 "1" 纯数字）。生产
	// App 发送位压缩版本号（纯数字，由部署方自备）。
	AppVersion string
	// AppPackage 可选覆盖登录参数 AppName/PackageName（默认 demo 包
	// com.gwelldemo）。生产 App 两者均为自身包名（身份参数由部署方自备，
	// 官方客户端资料）。
	AppPackage string

	// ListMode 选择设备列表端点（cloud.DeviceListAuto 等；空=auto），
	// 见 devices.go。
	ListMode DeviceListMode
	// Log 可选日志器：设备列表自动降级等运维决策经它说明原因。
	Log *slog.Logger
}

func NewClient(host string) *Client {
	return &Client{Host: host, HTTP: &http.Client{Timeout: 10 * time.Second}}
}

// appID 返回生效的 AppID（覆盖优先，缺省回落 demo 常量）。
func (c *Client) appID() string {
	if c.AppID != "" {
		return c.AppID
	}
	return DemoAppID
}

// appToken 返回生效的 AppToken（覆盖优先，缺省回落 demo 常量）。
func (c *Client) appToken() string {
	if c.AppToken != "" {
		return c.AppToken
	}
	return DemoAppToken
}

// appVersion 返回生效的 AppVersion（覆盖优先，缺省 "1"）。
func (c *Client) appVersion() string {
	if c.AppVersion != "" {
		return c.AppVersion
	}
	return "1"
}

// appPackage 返回生效的 AppName/PackageName（覆盖优先，缺省 demo 包）。
func (c *Client) appPackage() string {
	if c.AppPackage != "" {
		return c.AppPackage
	}
	return DemoPackage
}

// Login performs LoginCheck.ashx. AppVersion must stay pure-numeric ("1").
func (c *Client) Login(ctx context.Context, cred Credentials) (*Session, error) {
	q := url.Values{}
	q.Set("User", cred.User)
	q.Set("Pwd", cred.MD5())
	q.Set("VersionFlag", "1")
	q.Set("SessionID2", "1")
	q.Set("AnonymousId", "")
	q.Set("AppID", c.appID())
	q.Set("AppToken", c.appToken())
	q.Set("Language", "zh-cn")
	q.Set("AppOS", "3")
	q.Set("AppName", c.appPackage())
	q.Set("AppVersion", c.appVersion())
	q.Set("PackageName", c.appPackage())
	q.Set("ApiVersion", "1")

	u := "https://" + c.Host + "/Users/LoginCheck.ashx?" + q.Encode()
	if c.LogReq != nil {
		c.LogReq("POST", "https://"+c.Host+"/Users/LoginCheck.ashx?...")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", demoUserAgent)
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	var raw map[string]string
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("login response not JSON (%d): %.120s", resp.StatusCode, body)
	}
	code, _ := strconv.Atoi(raw["error_code"])
	if code != 0 {
		return nil, &LoginError{Code: code, Msg: raw["error"]}
	}
	signed, err := strconv.ParseInt(raw["UserID"], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("bad UserID %q", raw["UserID"])
	}
	s := &Session{
		UID:     uint64(uint32(signed)),
		Raw:     raw,
		LoginAt: time.Now(),
	}
	s.C1 = parseU32(raw["P2PVerifyCode1"])
	s.C2 = parseU32(raw["P2PVerifyCode2"])
	s.S1 = parseU32(raw["SessionID"])
	s.S2 = parseU32(raw["SessionID2"])
	return s, nil
}

func parseU32(v string) uint32 {
	n, _ := strconv.ParseInt(v, 10, 64)
	return uint32(n)
}

// BoundDevice is one entry of the account's bound device list.
// LoadDeviceInfo additionally fills DeviceCate/SecretKey（SearchBindDev
// 回退路径两者为零值）。
type BoundDevice struct {
	DevID      string         `json:"dev_id"`
	Name       string         `json:"name"`
	Online     bool           `json:"online"`
	DeviceCate uint32         `json:"device_cate,omitempty"` // LoadDeviceInfo 第 5 字段（501=门铃类）
	SecretKey  string         `json:"secret_key,omitempty"`  // 加密的主人密码（base64），供后续解密用
	Raw        map[string]any `json:"raw,omitempty"`
}

// IsDoorbell 报告该设备是否门铃类（deviceCate=501，真机实测）。
func (d BoundDevice) IsDoorbell() bool { return d.DeviceCate == DeviceCateDoorbell }

// SearchBindDev lists devices bound to the account via the LEGACY endpoint
// (recovered in T0 from libhttp HttpService.java; 404 on the current
// official cloud — kept as the configurable fallback of ListDevices).
// Field names tolerate server variants.
func (c *Client) SearchBindDev(ctx context.Context, s *Session) ([]BoundDevice, error) {
	q := url.Values{}
	q.Set("UserID", s.APIUserID())
	q.Set("SessionID", strconv.FormatUint(uint64(s.S1), 10))
	q.Set("AppID", c.appID())
	q.Set("AppToken", c.appToken())
	q.Set("PackageName", c.appPackage())
	q.Set("AppVersion", c.appVersion())

	u := "https://" + c.Host + "/Account/Bind/SearchBindDev.ashx?" + q.Encode()
	if c.LogReq != nil {
		c.LogReq("POST", "https://"+c.Host+"/Account/Bind/SearchBindDev.ashx?...")
	}
	req, err := http.NewRequestWithContext(ctx, "POST", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", demoUserAgent)
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
		return nil, fmt.Errorf("SearchBindDev response not JSON: %.120s", body)
	}
	if ec, ok := top["error_code"]; ok {
		var codeStr string
		_ = json.Unmarshal(ec, &codeStr)
		if codeStr != "" && codeStr != "0" {
			code, _ := strconv.Atoi(codeStr)
			return nil, &LoginError{Code: code}
		}
	}
	// The list may sit under several keys depending on server generation.
	var arr []map[string]any
	for _, key := range []string{"dev_list", "DevList", "data", "list"} {
		if raw, ok := top[key]; ok {
			if err := json.Unmarshal(raw, &arr); err == nil && arr != nil {
				break
			}
		}
	}
	if arr == nil {
		return nil, errors.New("no device array found in SearchBindDev response")
	}
	var out []BoundDevice
	for _, m := range arr {
		d := BoundDevice{Raw: m}
		d.DevID = firstString(m, "dev_id", "DevID", "Did", "device_id")
		d.Name = firstString(m, "dev_name", "DevName", "name", "Name")
		if v, ok := m["online"]; ok {
			switch tv := v.(type) {
			case bool:
				d.Online = tv
			case float64:
				d.Online = tv != 0
			case string:
				d.Online = tv == "1" || strings.EqualFold(tv, "true")
			}
		}
		if d.DevID != "" {
			out = append(out, d)
		}
	}
	return out, nil
}

func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch tv := v.(type) {
			case string:
				return tv
			case float64:
				return strconv.FormatFloat(tv, 'f', -1, 64)
			}
		}
	}
	return ""
}
