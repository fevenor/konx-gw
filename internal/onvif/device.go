package onvif

import (
	"net/http"
	"strings"
)

// deviceService handles /onvif/device_service (GetServices,
// GetServiceCapabilities, GetDeviceInformation, GetCapabilities,
// GetSystemDateAndTime, GetHostname, GetUsers, GetSystemLog…).
func (s *Server) deviceService(w http.ResponseWriter, r *http.Request, action, body string) {
	if !s.checkUserToken(body) {
		soapFault(w, "s:Sender", "ter:NotAuthorized", "invalid user token")
		return
	}
	op := actionOf(action, body, "")
	host := s.advertiseHost(r)
	port := s.env.Cfg.AdvertisedONVIFPort()
	switch op {
	case "GetServices":
		reply(w, `<s:Body><tds:GetServicesResponse>`+
			s.serviceEntry("tds", "Device", "/onvif/device_service", "Device")+
			s.serviceEntry("tmv", "Media", "/onvif/media_service", "Media")+
			s.serviceEntry("tev", "Events", "/onvif/events_service", "Events")+
			`</tds:GetServicesResponse></s:Body>`)
	case "GetServiceCapabilities":
		if strings.Contains(body, "Category=\"Media\"") || strings.Contains(action, "Media") {
			s.mediaCapabilities(w)
			return
		}
		if strings.Contains(body, "Category=\"Events\"") || strings.Contains(action, "Events") {
			s.eventsCapabilities(w)
			return
		}
		reply(w, `<s:Body><tds:GetServiceCapabilitiesResponse><tds:Capabilities>`+
			`<tds:Network IPFilter="false" ZeroConfiguration="false" IPv6="false" DynDNS="false"/>`+
			`<tds:Security TLS1.0="false" TLS1.1="false" TLS1.2="false" OnvifAccess="true"/>`+
			`<tds:System DiscoveryResolve="false" SupportedVersions=""/>`+
			`</tds:Capabilities></tds:GetServiceCapabilitiesResponse></s:Body>`)
	case "GetDeviceInformation":
		reply(w, `<s:Body><tds:GetDeviceInformationResponse>`+
			`<tds:Manufacturer>Konx</tds:Manufacturer>`+
			`<tds:Model>konx-gw</tds:Model>`+
			`<tds:FirmwareVersion>`+s.env.Cfg.GwID+`</tds:FirmwareVersion>`+
			`<tds:SerialNumber>`+s.env.Cfg.GwID+`</tds:SerialNumber>`+
			`<tds:HardwareId>konx-gw</tds:HardwareId>`+
			`</tds:GetDeviceInformationResponse></s:Body>`)
	case "GetCapabilities":
		reply(w, `<s:Body><tds:GetCapabilitiesResponse><tds:Capabilities>`+
			`<tt:Media><tt:XAddr>http://`+host+`:`+itoa(port)+`/onvif/media_service</tt:XAddr>`+
			`<tt:StreamingCapabilities><tt:RTP_TCP="true" RTP_RTSP_TCP="true"/></tt:StreamingCapabilities></tt:Media>`+
			`<tt:Events><tt:XAddr>http://`+host+`:`+itoa(port)+`/onvif/events_service</tt:XAddr>`+
			`<tt:WSSubscriptionPolicySupport="false"/></tt:Events>`+
			`<tt:Device><tt:XAddr>http://`+host+`:`+itoa(port)+`/onvif/device_service</tt:XAddr></tt:Device>`+
			`</tds:Capabilities></tds:GetCapabilitiesResponse></s:Body>`)
	case "GetSystemDateAndTime":
		reply(w, systemDateAndTimeBody())
	case "GetHostname":
		reply(w, `<s:Body><tds:GetHostnameResponse><tds:HostnameInformation>`+
			`<tt:FromDHCP>false</tt:FromDHCP><tt:Name>`+xmlEscape(host)+`</tt:Name>`+
			`</tds:HostnameInformation></tds:GetHostnameResponse></s:Body>`)
	case "GetUsers":
		reply(w, `<s:Body><tds:GetUsersResponse>`+
			`<tds:User><tt:Username>admin</tt:Username><tt:UserLevel>Administrator</tt:UserLevel></tds:User>`+
			`</tds:GetUsersResponse></s:Body>`)
	case "GetSystemLog":
		reply(w, `<s:Body><tds:GetSystemLogResponse><tds:SystemLog>ok</tds:SystemLog></tds:GetSystemLogResponse></s:Body>`)
	default:
		// GetRelayOutputs etc: empty success keeps clients happy
		if op == "" {
			soapFault(w, "s:Sender", "ter:ActionNotSupported", "missing operation")
			return
		}
		reply(w, `<s:Body><`+opLocal(op)+`Response/></s:Body>`)
	}
}

func opLocal(op string) string {
	if i := strings.LastIndex(op, ":"); i >= 0 {
		op = op[i+1:]
	}
	if i := strings.LastIndex(op, "/"); i >= 0 {
		op = op[i+1:]
	}
	return op
}

func (s *Server) serviceEntry(ns, name, path, nsName string) string {
	host := s.env.Cfg.Advertise.Host
	port := s.env.Cfg.AdvertisedONVIFPort()
	if host == "" {
		host = "127.0.0.1"
	}
	return `<tds:Service><tds:Namespace>http://www.onvif.org/ver10/` + strings.ToLower(name) +
		`/wsdl</tds:Namespace><tds:XAddr>http://` + host + `:` + itoa(port) + path +
		`</tds:XAddr><tds:Version><tt:Major>2</tt:Major><tt:Minor>0</tt:Minor></tds:Version></tds:Service>`
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func systemDateAndTimeBody() string {
	// filled by time helpers in events.go file
	return systemDateAndTime()
}
