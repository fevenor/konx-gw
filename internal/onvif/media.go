package onvif

import (
	"net/http"
	"regexp"
	"strings"

	"github.com/fevenor/konx-gw/internal/media"
)

var reProfileToken = regexp.MustCompile(`ProfileToken="([^"]*)"`)

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
		tok := firstGroup(reProfileToken, body)
		dev, prof, ok := s.parseToken(tok)
		if !ok {
			soapFault(w, "s:Sender", "ter:InvalidArgVal", "unknown profile token")
			return
		}
		reply(w, profileBody(dev, prof, s.videoEncoderXML(dev, prof)))
	case "GetStreamUri":
		dev, prof, ok := s.parseToken(firstGroup(reProfileToken, body))
		if !ok {
			soapFault(w, "s:Sender", "ter:InvalidArgVal", "unknown profile token")
			return
		}
		uri := s.env.StreamURL(dev, prof)
		reply(w, `<s:Body><tmv:GetStreamUriResponse><tt:MediaUri><tt:Uri>`+
			xmlEscape(uri)+`</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect>`+
			`<tt:InvalidAfterReboot>false</tt:InvalidAfterReboot><tt:Timeout>PT60S</tt:Timeout>`+
			`</tt:MediaUri></tmv:GetStreamUriResponse></s:Body>`)
	case "GetSnapshotUri":
		dev, prof, ok := s.parseToken(firstGroup(reProfileToken, body))
		if !ok {
			soapFault(w, "s:Sender", "ter:InvalidArgVal", "unknown profile token")
			return
		}
		uri := s.env.SnapshotURL(dev, prof)
		reply(w, `<s:Body><tmv:GetSnapshotUriResponse><tt:MediaUri><tt:Uri>`+
			xmlEscape(uri)+`</tt:Uri><tt:InvalidAfterConnect>false</tt:InvalidAfterConnect>`+
			`<tt:InvalidAfterReboot>true</tt:InvalidAfterReboot><tt:Timeout>PT5S</tt:Timeout>`+
			`</tt:MediaUri></tmv:GetSnapshotUriResponse></s:Body>`)
	case "GetVideoEncoderConfigurations":
		dev, prof, _ := s.parseToken(firstGroup(reProfileToken, body))
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

func firstGroup(re *regexp.Regexp, s string) string {
	if m := re.FindStringSubmatch(s); m != nil {
		return m[1]
	}
	return ""
}

// token layout: profile_<device>_<main|sub>
func (s *Server) parseToken(tok string) (string, media.Profile, bool) {
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
	// default: first configured device, main
	for _, d := range s.env.Cfg.Devices {
		return d.ID, media.ProfileMain, true
	}
	return "", "", false
}

func (s *Server) getProfiles(w http.ResponseWriter, r *http.Request) {
	var bodies string
	for _, d := range s.env.Cfg.Devices {
		for _, p := range []media.Profile{media.ProfileMain, media.ProfileSub} {
			bodies += profileBody(d.ID, p, s.videoEncoderXML(d.ID, p))
		}
	}
	reply(w, `<s:Body><tmv:GetProfilesResponse>`+bodies+`</tmv:GetProfilesResponse></s:Body>`)
}

func profileBody(dev string, p media.Profile, encoder string) string {
	name := dev + "-" + string(p)
	tok := "profile_" + dev + "_" + string(p)
	return `<tmv:Profiles token="` + tok + `" fixed="true">` +
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
		`</tmv:Profiles>`
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
