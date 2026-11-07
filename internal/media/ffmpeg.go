// ffmpeg pipelines (DESIGN §5.3): snapshot (JPEG), HLS (web live) and
// MJPEG (lightweight web view). All read from the egress loopback so the
// device sees exactly one consumer.
package media

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"
)

// FFmpeg wraps the ffmpeg binary.
type FFmpeg struct {
	Path string
}

// Available reports whether the binary runs.
func (f *FFmpeg) Available() bool {
	if f.Path == "" {
		return false
	}
	cmd := exec.Command(f.Path, "-version")
	return cmd.Run() == nil
}

// Snapshot grabs one JPEG frame from src (an rtsp:// URL).
func (f *FFmpeg) Snapshot(ctx context.Context, src string, timeout time.Duration) ([]byte, error) {
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	cmd := exec.CommandContext(cctx, f.Path,
		"-rtsp_transport", "tcp",
		"-loglevel", "error",
		"-i", src,
		"-frames:v", "1",
		"-f", "image2", "-c:v", "mjpeg", "-q:v", "3",
		"pipe:1",
	)
	out, err := cmd.Output()
	if err != nil {
		if cctx.Err() != nil {
			return nil, fmt.Errorf("snapshot timeout: %w", cctx.Err())
		}
		return nil, fmt.Errorf("ffmpeg snapshot: %w", err)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("snapshot produced no data (source warming up?)")
	}
	return out, nil
}

// hlsSession keeps one HLS transcode running per stream.
type hlsSession struct {
	cancel context.CancelFunc
	done   chan struct{}
}

type HLSManager struct {
	ff      *FFmpeg
	outRoot string

	mu       sync.Mutex
	sessions map[string]*hlsSession
}

func NewHLSManager(ff *FFmpeg, outRoot string) *HLSManager {
	return &HLSManager{ff: ff, outRoot: outRoot, sessions: map[string]*hlsSession{}}
}

// Ensure starts (or keeps) an HLS pipeline for key (device|profile) if at
// least one viewer is expected; Stop drops it.
func (h *HLSManager) Ensure(key, src string) (string, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if _, ok := h.sessions[key]; ok {
		return h.playlistPath(key), nil
	}
	if src == "" {
		return "", fmt.Errorf("HLS 缺少媒体源地址")
	}
	dir := filepath.Join(h.outRoot, "hls", key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	ctx, cancel := context.WithCancel(context.Background())
	ss := &hlsSession{cancel: cancel, done: make(chan struct{})}
	h.sessions[key] = ss

	cmd := exec.CommandContext(ctx, h.ff.Path,
		"-rtsp_transport", "tcp",
		"-loglevel", "error",
		"-i", src,
		"-c:v", "copy",
		"-c:a", "aac", "-ar", "16000", "-b:a", "24k", // G.711 → AAC (AD6)
		"-f", "hls",
		"-hls_time", "2",
		"-hls_list_size", "4",
		"-hls_flags", "delete_segments+append_list+independent_segments",
		filepath.Join(dir, "index.m3u8"),
	)
	if err := cmd.Start(); err != nil {
		delete(h.sessions, key)
		cancel()
		return "", err
	}
	go func() {
		defer close(ss.done)
		defer cancel()
		_ = cmd.Wait()
		h.mu.Lock()
		if cur, ok := h.sessions[key]; ok && cur == ss {
			delete(h.sessions, key)
		}
		h.mu.Unlock()
	}()
	return h.playlistPath(key), nil
}

// Playlist 只查询已启动的管线；读取文件不会创建进程。
func (h *HLSManager) Playlist(key string) (string, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	_, ok := h.sessions[key]
	return h.playlistPath(key), ok
}

// Stop terminates the HLS pipeline for key.
func (h *HLSManager) Stop(key string) {
	h.mu.Lock()
	ss, ok := h.sessions[key]
	delete(h.sessions, key)
	h.mu.Unlock()
	if ok {
		ss.cancel()
		<-ss.done
	}
}

func (h *HLSManager) playlistPath(key string) string {
	return filepath.Join(h.outRoot, "hls", key, "index.m3u8")
}

// MJPEGHandler streams multipart JPEG from src until the client breaks.
func (f *FFmpeg) MJPEGHandler(w io.Writer, src string, stop <-chan struct{}) error {
	cmd := exec.Command(f.ffpath(),
		"-rtsp_transport", "tcp",
		"-loglevel", "error",
		"-i", src,
		"-vf", "fps=5,scale=640:-1",
		"-c:v", "mjpeg", "-q:v", "5",
		"-f", "mpjpeg", "-boundary_tag", "konxgw",
		"pipe:1",
	)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() {
		_, err := io.Copy(w, stdout)
		done <- err
	}()
	select {
	case <-stop:
		_ = cmd.Process.Kill()
		<-done
		// Kill 后必须 Wait 回收子进程，否则僵尸残留（进程表/fd 泄漏）；
		// Wait 加时限保护——进程已 Kill 仅缺 reap，卡死超限则放弃并
		// 告警（不影响返回语义）。
		waited := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(waited)
		}()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			slog.Warn("ffmpeg MJPEG 进程 Wait 超时，放弃回收（进程已 Kill）",
				"path", f.ffpath())
		}
		return nil
	case err := <-done:
		_ = cmd.Wait()
		return err
	}
}

func (f *FFmpeg) ffpath() string {
	if f.Path != "" {
		return f.Path
	}
	return "ffmpeg"
}

var _ = time.Second
