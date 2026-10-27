package api

import (
	"fmt"
	"net/http"
	"strings"
)

// metrics renders a minimal Prometheus text exposition (DESIGN §15).
// 鉴权在 routes() 按配置包裹（web.metrics_auth：默认与 API 同源的
// Bearer 令牌校验，可信内网抓取可显式关闭）。
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
	// 事件总量：改为 SQLite COUNT(*)（O(1)）。旧实现 Query 拉最多 1000
	// 行再数长度，语义是"最近窗口条数"而非总量，且每次抓取全表捞行；
	// 指标名同时对齐 DESIGN §15 的 konx_events_total。
	var evTotal float64
	if n, err := s.d.Store.CountEvents(); err == nil {
		evTotal = float64(n)
	}
	p("konx_events_total", "Total events stored", "gauge", evTotal)
	for kind, n := range snap.Clients {
		p("konx_clients{kind=\""+kind+"\"}", "Connected clients", "gauge", n)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String()))
}
