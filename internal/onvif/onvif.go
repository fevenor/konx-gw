// Package onvif implements the gateway's ONVIF server face (DESIGN §7):
// Device + Media + Events (PullPoint) over hand-rolled SOAP. The gateway
// presents itself as a Konx virtual NVR and re-exports the doorbell event
// stream the device itself lacks (live-probed: no Events service).
package onvif

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/xml"
	"log/slog"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/events"
	"github.com/fevenor/konx-gw/internal/media"
)

// Env provides the pieces ONVIF needs from the app.
type Env struct {
	Cfg    *config.App
	Auth   *auth.Service
	Engine *events.Engine
	Store  *events.Store
	Mgr    *media.Manager
	Log    *slog.Logger
	// StreamURL builds the advertised RTSP URL for device/profile.
	StreamURL func(deviceID string, p media.Profile) string
	// SnapshotURL builds the advertised snapshot URL.
	SnapshotURL func(deviceID string, p media.Profile) string
}

type Server struct {
	env Env
	mux *http.ServeMux

	mu   sync.Mutex
	subs map[string]*pullSub
}

func New(env Env) *Server {
	s := &Server{env: env, mux: http.NewServeMux(), subs: map[string]*pullSub{}}
	s.mux.HandleFunc("/onvif/device_service", s.soap(s.deviceService))
	s.mux.HandleFunc("/onvif/media_service", s.soap(s.mediaService))
	s.mux.HandleFunc("/onvif/events_service", s.soap(s.eventsService))
	return s
}

// Handler returns the HTTP handler.
func (s *Server) Handler() http.Handler { return s.mux }

// ---------------------------------------------------------------------------
// SOAP plumbing
// ---------------------------------------------------------------------------

type soapHandler func(w http.ResponseWriter, r *http.Request, action, bodyXML string)

func (s *Server) soap(fn soapHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !s.env.Cfg.ONVIF.AllowAnonymous {
			if u, p, ok := r.BasicAuth(); ok {
				if !s.env.Auth.CheckRTSP(u, p) {
					w.Header().Set("WWW-Authenticate", `Basic realm="konx-gw-onvif"`)
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
			}
			// WS-UsernameToken is validated per-operation inside handlers
		}
		buf := make([]byte, 0, 64*1024)
		tmp := make([]byte, 8192)
		for {
			n, err := r.Body.Read(tmp)
			buf = append(buf, tmp[:n]...)
			if err != nil {
				break
			}
		}
		action := strings.Trim(r.Header.Get("SOAPAction"), `"' `)
		fn(w, r, action, string(buf))
	}
}

// userToken is the extracted wsse:UsernameToken.
type userToken struct {
	Username    string
	NonceB64    string
	Created     string
	Password    string
	PasswordTyp string
}

var (
	reUser  = regexp.MustCompile(`<wsse:Username[^>]*>([^<]*)</wsse:Username>`)
	reNonce = regexp.MustCompile(`<wsse:Nonce[^>]*>([^<]*)</wsse:Nonce>`)
	reCreat = regexp.MustCompile(`<wsu:Created[^>]*>([^<]*)</wsu:Created>`)
	rePass  = regexp.MustCompile(`Type="([^"]*)"[^>]*>([^<]*)</wsse:Password>`)
	rePass2 = regexp.MustCompile(`<wsse:Password[^>]*>([^<]*)</wsse:Password>`)
)

func extractUserToken(body string) userToken {
	var t userToken
	if m := reUser.FindStringSubmatch(body); m != nil {
		t.Username = m[1]
	}
	if m := reNonce.FindStringSubmatch(body); m != nil {
		t.NonceB64 = m[1]
	}
	if m := reCreat.FindStringSubmatch(body); m != nil {
		t.Created = m[1]
	}
	if m := rePass.FindStringSubmatch(body); m != nil {
		t.PasswordTyp = m[1]
		t.Password = m[2]
	} else if m := rePass2.FindStringSubmatch(body); m != nil {
		t.Password = m[1]
	}
	return t
}

// checkUserToken validates WS-UsernameToken (digest or text) or Basic.
func (s *Server) checkUserToken(bodyXML string) bool {
	if s.env.Cfg.ONVIF.AllowAnonymous {
		return true
	}
	t := extractUserToken(bodyXML)
	if t.Username == "" {
		return false
	}
	pass, ok := s.env.Auth.RTSPPassword(t.Username)
	if !ok {
		return false
	}
	if strings.Contains(t.PasswordTyp, "PasswordDigest") && t.NonceB64 != "" {
		nonce, err := base64.StdEncoding.DecodeString(strings.TrimSpace(t.NonceB64))
		if err != nil {
			return false
		}
		sum := sha1.Sum(append(append(nonce, []byte(t.Created)...), []byte(pass)...))
		return base64.StdEncoding.EncodeToString(sum[:]) == strings.TrimSpace(t.Password)
	}
	return t.Password == pass
}

// reply writes a SOAP 1.2 response body.
func reply(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/soap+xml; charset=utf-8")
	_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>` +
		`<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" ` +
		`xmlns:tds="http://www.onvif.org/ver10/device/wsdl" ` +
		`xmlns:tt="http://www.onvif.org/ver10/schema" ` +
		`xmlns:tmv="http://www.onvif.org/ver10/media/wsdl" ` +
		`xmlns:tev="http://www.onvif.org/ver10/events/wsdl" ` +
		`xmlns:wsnt="http://docs.oasis-open.org/wsn/b-2" ` +
		`xmlns:wsa="http://www.w3.org/2005/08/addressing">` + body + `</s:Envelope>`))
}

func soapFault(w http.ResponseWriter, code, subcode, text string) {
	w.WriteHeader(http.StatusBadRequest)
	reply(w, `<s:Body><s:Fault><s:Code><s:Value>`+code+
		`</s:Value><s:Subcode><s:Value>`+subcode+`</s:Value></s:Subcode></s:Code>`+
		`<s:Reason><s:Text xml:lang="en">`+xmlEscape(text)+`</s:Text></s:Reason></s:Fault></s:Body>`)
}

func xmlEscape(v string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(v))
	return b.String()
}

func randID() string {
	b := make([]byte, 10)
	_, _ = rand.Read(b)
	return "konx-" + hex.EncodeToString(b)
}

// advertiseHost resolves the host clients should address.
func (s *Server) advertiseHost(r *http.Request) string {
	if s.env.Cfg.Advertise.Host != "" {
		return s.env.Cfg.Advertise.Host
	}
	host := r.Host
	if i := strings.LastIndex(host, ":"); i > 0 {
		host = host[:i]
	}
	return host
}

// actionOf normalizes an operation name from SOAPAction or the first
// body child element (any namespace prefix or default namespace).
var reBodyOp = regexp.MustCompile(`<(?:[A-Za-z0-9]+:)?([A-Za-z0-9]+)(?:\s[^>]*)?>`)

var knownOps = map[string]bool{
	"GetServices": true, "GetServiceCapabilities": true, "GetDeviceInformation": true,
	"GetCapabilities": true, "GetSystemDateAndTime": true, "GetHostname": true,
	"GetUsers": true, "GetSystemLog": true, "GetRelayOutputs": true,
	"GetProfiles": true, "GetProfile": true, "GetStreamUri": true, "GetSnapshotUri": true,
	"GetVideoEncoderConfigurations": true, "GetVideoSources": true,
	"GetVideoSourceConfigurations": true, "GetAudioEncoderConfigurations": true,
	"GetMetadataConfigurations": true,
	"GetEventProperties":        true, "CreatePullPointSubscription": true,
	"PullMessages": true, "Unsubscribe": true, "Subscribe": true,
}

func actionOf(action, bodyXML, fallback string) string {
	if action != "" {
		return action
	}
	// skip the Envelope/Body/Header wrappers: scan candidates, first hit
	// inside the body region wins; wrappers are excluded via knownOps.
	if i := strings.Index(bodyXML, "<s:Body"); i >= 0 {
		bodyXML = bodyXML[i:]
	}
	for _, m := range reBodyOp.FindAllStringSubmatch(bodyXML, -1) {
		if knownOps[m[1]] {
			return m[1]
		}
	}
	return fallback
}
