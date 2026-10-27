package api

import (
	"fmt"
	"net/http"
	"strings"
)

// metrics renders a minimal Prometheus text exposition (DESIGN §15).
func (s *Server) metrics(w http.ResponseWriter, r *http.Request) {
	snap := s.snapshot()
	var b strings.Builder
	p := func(name, help, typ string, v any) {
		b.WriteString(fmt.Sprintf("# HELP %s %s\n# TYPE %s %s\n%s %v\n", name, help, name, typ, name, v))
	}
	up := 0
	if snap.P2P == "ready" {
		up = 1
	}
	p("konx_p2p_up", "P2P push session state", "gauge", up)
	p("konx_uptime_seconds", "Gateway uptime", "gauge", snap.Uptime)
	var evTotal float64
	if evs, err := s.d.Store.Query("", "", 0, 1000); err == nil {
		evTotal = float64(len(evs))
	}
	p("konx_events_recent", "Events stored (last query window)", "gauge", evTotal)
	for kind, n := range snap.Clients {
		p("konx_clients{kind=\""+kind+"\"}", "Connected clients", "gauge", n)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
