package api

import (
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fevenor/konx-gw/internal/auth"
	"github.com/fevenor/konx-gw/internal/config"
	"github.com/fevenor/konx-gw/internal/media"
	"github.com/fevenor/konx-gw/internal/rtspout"
)

// 转码进程已退出时，再次开始观看应该真正重启管线。
func TestRegressionHLSRestartAfterExit(t *testing.T) {
	dir := t.TempDir()
	ff := filepath.Join(dir, "ffmpeg-demo")
	count := filepath.Join(dir, "启动次数")
	if err := os.WriteFile(ff, []byte("#!/bin/sh\nprintf '启动\\n' >> '"+count+"'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Media.Source = "external-rtsp"
	cfg.Media.ExternalURL = "invalid-url"
	cfg.Devices = []config.Device{{ID: "10000001"}}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	mgr := media.NewManager(cfg, log, nil)
	defer mgr.StopAll()
	hls := media.NewHLSManager(&media.FFmpeg{Path: ff}, dir)
	s := New(Deps{Cfg: cfg, Auth: &auth.Service{}, Mgr: mgr,
		HLS: hls, Rtsp: rtspout.New(":8554", mgr, nil, nil, log), Log: log})
	start := func() {
		r := httptest.NewRequest("POST", "/api/devices/10000001/hls-start", nil)
		r.SetPathValue("id", "10000001")
		w := httptest.NewRecorder()
		s.handleHLSStart(w, r)
		if w.Code != 200 {
			t.Fatalf("启动失败：%s", w.Body.String())
		}
	}
	start()
	time.Sleep(100 * time.Millisecond)
	start()
	time.Sleep(100 * time.Millisecond)
	hls.Stop("10000001|main")
	b, err := os.ReadFile(count)
	if err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(string(b), "启动"); n != 2 {
		t.Fatalf("两次观看请求之间进程已退出，但启动次数=%d", n)
	}
}

func TestHLSReadDoesNotStartProcess(t *testing.T) {
	hls := media.NewHLSManager(&media.FFmpeg{Path: "/does/not/exist"}, t.TempDir())
	s := New(Deps{Cfg: config.Defaults(), Auth: &auth.Service{}, HLS: hls})
	r := httptest.NewRequest("GET", "/api/devices/10000001/hls/main/index.m3u8", nil)
	r.SetPathValue("id", "10000001")
	r.SetPathValue("profile", "main")
	r.SetPathValue("file", "index.m3u8")
	w := httptest.NewRecorder()
	s.handleHLSFile(w, r)
	if w.Code != 404 {
		t.Fatalf("未启动的流读取应返回 404，得到 %d", w.Code)
	}
}
