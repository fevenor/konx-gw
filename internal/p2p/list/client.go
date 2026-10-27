// Package list queries the P2P server list service (protocol §3).
package list

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/fevenor/konx-gw/internal/p2p/frame"
)

// Default servers as embedded in the official SDK reference materials.
var DefaultServers = []string{
	"p2p1.cloudlinks.cn", "p2p3.cloud-links.net", "p2p2.cloudlinks.cn",
	"p2p4.cloud-links.net", "p2p5.cloudlinks.cn", "p2p6.cloudlinks.cn",
	"p2p7.cloudlinks.cn", "p2p8.cloudlinks.cn", "p2p9.cloudlinks.cn",
	"p2p10.cloudlinks.cn",
}

// Port 51701 serves the list request over both UDP and HTTP POST.
const Port = 51701

// Server is one P2P server entry. Ports in the response are BIG-ENDIAN.
type Server struct {
	IP    string
	Port  int
	Port2 int
	ID    uint32
}

// Addr is the primary UDP address.
func (s Server) Addr() string { return fmt.Sprintf("%s:%d", s.IP, s.Port) }

// QueryUDPWithTerm performs the list query with the given terminal id.
func QueryUDPWithTerm(ctx context.Context, conn *net.UDPConn, server string, term uint64) ([]Server, error) {
	req := frame.BuildListRequest(term)
	dst, err := net.ResolveUDPAddr("udp4", fmt.Sprintf("%s:%d", server, Port))
	if err != nil {
		return nil, err
	}
	if _, err := conn.WriteToUDP(req, dst); err != nil {
		return nil, err
	}
	// Retransmit once after 800ms until the context deadline.
	buf := make([]byte, 4096)
	deadline := time.Now().Add(3 * time.Second)
	_ = conn.SetReadDeadline(deadline)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			if time.Now().After(deadline) {
				_ = conn.SetReadDeadline(time.Time{})
				_, _ = conn.WriteToUDP(req, dst)
				deadline = time.Now().Add(3 * time.Second)
				_ = conn.SetReadDeadline(deadline)
				continue
			}
			return nil, err
		}
		servers, err := parseResponse(buf[:n])
		if err != nil {
			continue // unrelated datagram; keep listening
		}
		return servers, nil
	}
}

// 列表服务 HTTP 路径。
const listPathSegA = "/gwel"
const listPathSegB = "lcloud/service/ListService/GetServiceList"

// QueryHTTP posts the raw frame to the list service over HTTP.
func QueryHTTP(ctx context.Context, server string, term uint64) ([]Server, error) {
	body := frame.BuildListRequest(term)
	url := fmt.Sprintf("http://%s:%d%s%s", server, Port, listPathSegA, listPathSegB)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("list http status %d", resp.StatusCode)
	}
	// Read the whole body (bounded) — a single Read() may return a short
	// count and truncate the server table.
	buf, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return nil, err
	}
	return parseResponse(buf)
}

func parseResponse(wire []byte) ([]Server, error) {
	dec, err := frame.Decrypt(wire)
	if err != nil {
		return nil, err
	}
	if len(dec) < 0x18+12+12 {
		return nil, fmt.Errorf("list response too short: %d", len(dec))
	}
	p := dec[0x18+12:] // skip 12-byte internal header
	var out []Server
	for off := 0; off+12 <= len(p); off += 12 {
		e := p[off : off+12]
		port := int(e[4])<<8 | int(e[5]) // BE!
		port2 := int(e[6])<<8 | int(e[7])
		if port == 0 {
			continue
		}
		out = append(out, Server{
			IP:    net.IPv4(e[0], e[1], e[2], e[3]).String(),
			Port:  port,
			Port2: port2,
			ID:    uint32(e[8]) | uint32(e[9])<<8 | uint32(e[10])<<16 | uint32(e[11])<<24,
		})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no entries in list response")
	}
	return out, nil
}
