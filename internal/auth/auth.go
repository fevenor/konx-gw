// Package auth implements the gateway credential model (DESIGN §12):
// bcrypt admin password, scoped API tokens, RTSP/ONVIF users shared with
// the token table, and a fixed loopback identity for internal pipelines.
package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/bcrypt"

	"github.com/fevenor/konx-gw/internal/events"
)

// Scopes
const (
	ScopeRead  = "read"
	ScopeTalk  = "talk"
	ScopeAdmin = "admin"
)

// Service is the auth facade.
type Service struct {
	store *events.Store
	log   *slog.Logger

	mu           sync.Mutex
	adminHash    []byte
	users        map[string]string // rtsp/onvif basic users → pass
	loopbackUser string
	loopbackPass string

	loginMu   sync.Mutex
	loginLast map[string][]time.Time // ip → attempts
}

func New(store *events.Store, log *slog.Logger) (*Service, error) {
	s := &Service{store: store, log: log, users: map[string]string{}, loginLast: map[string][]time.Time{}}
	// loopback identity for internal ffmpeg pipelines
	s.loopbackUser = "loopback"
	s.loopbackPass = randHex(16)
	// restore or create admin password
	hash, _ := store.KVGet("admin_password_hash")
	if hash == "" {
		pw := randHex(8)
		h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
		if err != nil {
			return nil, err
		}
		if err := store.KVSet("admin_password_hash", string(h)); err != nil {
			return nil, err
		}
		fmt.Printf("=== konx-gw generated admin password (shown once): %s ===\n", pw)
		hash = string(h)
	}
	s.adminHash = []byte(hash)
	return s, nil
}

// SetAdminPassword replaces the admin password (admin scope required).
func (s *Service) SetAdminPassword(pw string) error {
	h, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	if err != nil {
		return err
	}
	if err := s.store.KVSet("admin_password_hash", string(h)); err != nil {
		return err
	}
	s.mu.Lock()
	s.adminHash = h
	s.mu.Unlock()
	return nil
}

// CheckAdmin validates the web login password.
func (s *Service) CheckAdmin(pw string) bool {
	s.mu.Lock()
	h := s.adminHash
	s.mu.Unlock()
	return bcrypt.CompareHashAndPassword(h, []byte(pw)) == nil
}

// SetUser sets an RTSP/ONVIF basic user.
func (s *Service) SetUser(user, pass string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.users[user] = pass
}

// CheckRTSP validates RTSP basic credentials (loopback identity allowed).
func (s *Service) CheckRTSP(user, pass string) bool {
	s.mu.Lock()
	want, ok := s.users[user]
	lu, lp := s.loopbackUser, s.loopbackPass
	s.mu.Unlock()
	if user == lu && pass == lp {
		return true
	}
	_ = want
	return ok && want == pass
}

// Loopback returns the internal pipeline identity.
func (s *Service) Loopback() (string, string) { return s.loopbackUser, s.loopbackPass }

// --- tokens ---

// Token is an API token record.
type Token struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Scopes []string `json:"scopes"`
	Secret string   `json:"secret"` // returned only at creation
}

// CreateToken mints a token; the secret is shown once.
func (s *Service) CreateToken(name string, scopes []string) (*Token, error) {
	for _, sc := range scopes {
		switch sc {
		case ScopeRead, ScopeTalk, ScopeAdmin:
		default:
			return nil, fmt.Errorf("unknown scope %q", sc)
		}
	}
	secret := randHex(24)
	id := randHex(6)
	hash := hashSecret(secret)
	if err := s.store.InsertToken(id, hash, strings.Join(scopes, ","), name); err != nil {
		return nil, err
	}
	s.log.Info("token created", "id", id, "name", name, "scopes", scopes)
	return &Token{ID: id, Name: name, Scopes: scopes, Secret: secret}, nil
}

// ListTokens lists tokens without secrets.
func (s *Service) ListTokens() ([]map[string]any, error) { return s.store.ListTokens() }

// DeleteToken revokes a token.
func (s *Service) DeleteToken(id string) error { return s.store.DeleteToken(id) }

func hashSecret(s string) string {
	h := sha256.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

// CheckToken validates "Bearer <secret>" and returns its scopes.
func (s *Service) CheckToken(secret string) (scopes []string, ok bool) {
	id, sc, ok := s.store.LookupToken(hashSecret(secret))
	if !ok {
		return nil, false
	}
	_ = id
	return strings.Split(sc, ","), true
}

// --- HTTP middleware ---

type ctxKey int

const scopesKey ctxKey = 1

// Require returns a handler wrapper demanding the given scope.
func (s *Service) Require(scope string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		h := r.Header.Get("Authorization")
		// 浏览器 <img>/<video> 等标签无法携带 Authorization 头：与 ws.go
		// 一致，允许经 query 参数传递会话令牌（stream.mjpeg/snapshot 等
		// 流端点依赖此路径；token 与 tok 两种参数名都接受）。
		if h == "" {
			if tok := r.URL.Query().Get("token"); tok != "" {
				h = "Bearer " + tok
			} else if tok := r.URL.Query().Get("tok"); tok != "" {
				h = "Bearer " + tok
			}
		}
		const prefix = "Bearer "
		if !strings.HasPrefix(h, prefix) {
			writeErr(w, 401, "token_missing", "missing bearer token")
			return
		}
		scopes, ok := s.CheckToken(h[len(prefix):])
		if !ok {
			writeErr(w, 401, "token_invalid", "unknown token")
			return
		}
		if !hasScope(scopes, scope) {
			writeErr(w, 403, "scope_denied", "token lacks scope "+scope)
			return
		}
		next(w, r.WithContext(r.Context()))
	}
}

func hasScope(scopes []string, want string) bool {
	for _, s := range scopes {
		if s == ScopeAdmin || s == want {
			return true
		}
	}
	return false
}

func writeErr(w http.ResponseWriter, code int, ecode, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error": map[string]string{"code": ecode, "message": msg},
	})
}

// LoginRateLimited enforces 5 attempts/min/IP (DESIGN §12).
func (s *Service) LoginRateLimited(ip string) bool {
	s.loginMu.Lock()
	defer s.loginMu.Unlock()
	now := time.Now()
	var keep []time.Time
	for _, t := range s.loginLast[ip] {
		if now.Sub(t) < time.Minute {
			keep = append(keep, t)
		}
	}
	if len(keep) >= 5 {
		s.loginLast[ip] = keep
		return false
	}
	keep = append(keep, now)
	s.loginLast[ip] = keep
	return true
}

func randHex(n int) string {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// RTSPPassword returns the stored clear password for an RTSP/ONVIF user
// (needed for WS-UsernameToken digest recomputation).
func (s *Service) RTSPPassword(user string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.users[user]
	return p, ok
}

// CreateSecret generates a random secret (for first-boot credentials).
func (s *Service) CreateSecret() string { return randHex(10) }
