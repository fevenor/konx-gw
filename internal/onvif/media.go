package onvif

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/fevenor/konx-gw/internal/media"
)

var reProfileToken = regexp.MustCompile(`ProfileToken="([^"]*)"`)
var reProfileTokenElem = regexp.MustCompile(`<(?:[A-Za-z0-9]+:)?ProfileToken[^>]*>\s*([^<]*?)\s*</(?:[A-Za-z0-9]+:)?ProfileToken>`)

// profileTokenOf 提取 ProfileToken：优先 XML 元素式
// <trt:ProfileToken>token</trt:ProfileToken>（规范形态，命名空间
// 前缀任意或缺省），回退属性式 ProfileToken="token"（兼容旧客户端）。
func profileTokenOf(body string) (string, bool) {
	if m := reProfileTokenElem.FindStringSubmatch(body); m != nil {
		return m[1], true
	}
	if m := reProfileToken.FindStringSubmatch(body); m != nil {
		return m[1], true
	}
	return "", false
}

// mediaService handles /onvif/media_service: GetProfiles / GetProfile /
// GetStreamUri / GetSnapshotUri / GetVideoEncoderConfigurations.
func (s *Server) mediaService(w http.ResponseWriter, r *http.Request, action, body string) {
	if !s.checkUserToken(body) {
		soapFault(w, "s:Sender", "ter:NotAuthorized", "invalid user token")
		return
	}
	op := actionOf(action, body, "")
	switch op {
	case "GetProfiles":
		s.getProfiles(w, r)
	case "GetProfile":
		dev, prof, ok := s.requireProfile(w, body)
		if !ok {
			return
		}
		reply(w, `<s:Body><tmv:GetProfileResponse>`+
			profileBody("Profile", dev, prof, s.videoEncoderXML(dev, prof))+
			`</tmv:GetProfileResponse></s:Body>`)
	case "GetStreamUri":
		dev, prof, ok := s.requireProfile(w, body)
		if !ok {
			return
		}
		uri := s.env.StreamURL(dev, prof)
		reply(w, `<s:Body><tmv:GetStreamUriResponse><tt:MediaUri><tt:Uri>`+
			xmlEscape(uri)+`</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect>`+
			`<tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT60S</tt:Timeout>`+
			`</tt:MediaUri></tmv:GetStreamUriResponse></s:Body>`)
	case "GetSnapshotUri":
		dev, prof, ok := s.requireProfile(w, body)
		if !ok {
			return
		}
		uri := s.env.SnapshotURL(dev, prof)
		reply(w, `<s:Body><tmv:GetSnapshotUriResponse><tt:MediaUri><tt:Uri>`+
			xmlEscape(uri)+`</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect>`+
			`<tt:InvalidAfterReboot>true</tt:InvalidAfterReboot><tt:Timeout>PT5S</tt:Timeout>`+
			`</tt:MediaUri></tmv:GetSnapshotUriResponse></s:Body>`)
	case "GetVideoEncoderConfigurations":
		// 该操作规范上不携带 ProfileToken：有 token 且可解析则按
		// 其定位，否则回退第一台配置设备主码流。
		dev, prof := s.firstDeviceProfile()
		if tok, found := profileTokenOf(body); found {
			if d, p, ok := lookupProfileToken(tok); ok {
				dev, prof = d, p
			}
		}
		reply(w, `<s:Body><tmv:GetVideoEncoderConfigurationsResponse>`+
			s.videoEncoderXML(dev, prof)+`</tmv:GetVideoEncoderConfigurationsResponse></s:Body>`)
	case "GetServiceCapabilities":
		s.mediaCapabilities(w)
	case "GetVideoSources", "GetVideoSourceConfigurations", "GetAudioEncoderConfigurations",
		"GetMetadataConfigurations":
		reply(w, `<s:Body><`+opLocal(op)+`Response/></s:Body>`)
	default:
		if op == "" {
			soapFault(w, "s:Sender", "ter:ActionNotSupported", "missing operation")
			return
		}
		reply(w, `<s:Body><`+opLocal(op)+`Response/></s:Body>`)
	}
}

// token layout: profile_<device>_<main|sub>
// lookupProfileToken 纯解析 profile token，不做任何设备回退；
// 解析失败返回 ok=false，由调用方决定是否报错。
func lookupProfileToken(tok string) (string, media.Profile, bool) {
	if i := strings.Index(tok, "profile_"); i >= 0 {
		rest := tok[i+len("profile_"):]
		if j := strings.LastIndex(rest, "_"); j > 0 {
			dev := rest[:j]
			p := rest[j+1:]
			if p == "main" || p == "sub" {
				return dev, media.Profile(p), true
			}
		}
	}
	return "", "", false
}

// requireProfile 解析请求中的 ProfileToken；元素式/属性式均可。未
// 提供或无法解析时返回明确 SOAP 错误（多设备环境下静默回退第一台
// 会导致客户端拉错流），解析成功才返回设备与档位。
func (s *Server) requireProfile(w http.ResponseWriter, body string) (string, media.Profile, bool) {
	tok, found := profileTokenOf(body)
	if !found || tok == "" {
		soapFault(w, "s:Sender", "ter:InvalidArgVal", "missing profile token")
		return "", "", false
	}
	dev, prof, ok := lookupProfileToken(tok)
	if !ok {
		soapFault(w, "s:Sender", "ter:InvalidArgVal", "unknown profile token: "+tok)
		return "", "", false
	}
	return dev, prof, true
}

// firstDeviceProfile 回退到第一台配置设备的主码流（用于规范上不携
// 带 ProfileToken 的操作）。
func (s *Server) firstDeviceProfile() (string, media.Profile) {
	for _, d := range s.env.Cfg.Devices {
		return d.ID, media.ProfileMain
	}
	return "", media.ProfileMain
}

func (s *Server) getProfiles(w http.ResponseWriter, r *http.Request) {
	var bodies string
	for _, d := range s.env.Cfg.Devices {
		for _, p := range []media.Profile{media.ProfileMain, media.ProfileSub} {
			bodies += profileBody("Profiles", d.ID, p, s.videoEncoderXML(d.ID, p))
		}
	}
	reply(w, `<s:Body><tmv:GetProfilesResponse>`+bodies+`</tmv:GetProfilesResponse></s:Body>`)
}

func profileBody(element, dev string, p media.Profile, encoder string) string {
	name := dev + "-" + string(p)
	tok := "profile_" + dev + "_" + string(p)
	return `<tmv:` + element + ` token="` + tok + `" fixed="true">` +
		`<tt:Name>` + xmlEscape(name) + `</tt:Name>` +
		`<tt:VideoSourceConfiguration token="vsc_` + tok + `">` +
		`<tt:Name>` + xmlEscape(name) + `</tt:Name><tt:SourceToken>vsrc_0</tt:SourceToken>` +
		`<tt:Bounds x="0" y="0" width="1920" height="1080"/></tt:VideoSourceConfiguration>` +
		`<tt:VideoEncoderConfiguration token="vec_` + tok + `">` + encoder + `</tt:VideoEncoderConfiguration>` +
		`<tt:AudioEncoderConfiguration token="aec_` + tok + `">` +
		`<tt:Name>` + xmlEscape(name) + `</tt:Name><tt:Encoding>G711</tt:Encoding>` +
		`<tt:Bitrate>64000</tt:Bitrate><tt:SampleRate>8000</tt:SampleRate>` +
		`</tt:AudioEncoderConfiguration>` +
		`<tt:PTZConfiguration token="ptz_` + tok + `"><tt:Name>fixed</tt:Name></tt:PTZConfiguration>` +
		`</tmv:` + element + `>`
}

func (s *Server) videoEncoderXML(dev string, p media.Profile) string {
	res := "1920x1080"
	if p == media.ProfileSub {
		res = "320x180"
	}
	w, h := res, "0"
	if i := strings.Index(res, "x"); i > 0 {
		w, h = res[:i], res[i+1:]
	}
	return `<tt:Name>` + xmlEscape(dev+"-"+string(p)) + `</tt:Name><tt:Encoding>H264</tt:Encoding>` +
		`<tt:Resolution><tt:Width>` + w + `</tt:Width><tt:Height>` + h + `</tt:Height></tt:Resolution>` +
		`<tt:Quality>5</tt:Quality><tt:RateControl><tt:FrameRateLimit>15</tt:FrameRateLimit>` +
		`<tt:BitrateLimit>2048</tt:BitrateLimit></tt:RateControl>`
}

func (s *Server) mediaCapabilities(w http.ResponseWriter) {
	reply(w, `<s:Body><tmv:GetServiceCapabilitiesResponse><tmv:Capabilities`+
		` SnapshotUri="true" RTP_RTSP_TCP="true" ProfileChange="false"/></tmv:GetServiceCapabilitiesResponse></s:Body>`)
}
