package events

import (
	"database/sql"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"time"

	_ "modernc.org/sqlite"
)

// Store persists events and the client registry in SQLite (DESIGN §8.2).
type Store struct {
	db *sql.DB

	// bus is referenced for insert notifications via Engine; kept here to
	// avoid an import cycle by wiring in NewEngine.
	mu sync.Mutex

	// skipRaw 为 true 时按配置 events.keep_raw=false 跳过 raw 字段存储。
	// 零值 false 即默认存 raw，保持既有行为不变。
	skipRaw atomic.Bool
}

// Open creates/opens the database at path (WAL mode) and migrates.
func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // sqlite: serialize writers
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS events(
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT,
  device_id TEXT NOT NULL,
  type TEXT NOT NULL,
  ts_event INTEGER,
  ts_recv INTEGER NOT NULL,
  payload TEXT,
  raw BLOB
);
CREATE INDEX IF NOT EXISTS idx_events_ts ON events(ts_recv);
CREATE INDEX IF NOT EXISTS idx_events_dev ON events(device_id, ts_recv);
CREATE TABLE IF NOT EXISTS clients(
  id TEXT PRIMARY KEY,
  kind TEXT NOT NULL,
  peer TEXT, user TEXT,
  connected_at INTEGER NOT NULL,
  last_active INTEGER NOT NULL,
  active INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS tokens(
  id TEXT PRIMARY KEY,
  hash TEXT NOT NULL,
  scopes TEXT NOT NULL,
  name TEXT,
  created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS kv(key TEXT PRIMARY KEY, value TEXT);
`)
	return err
}

// SetKeepRaw 应用配置 events.keep_raw：false 时跳过 raw 字段存储以省磁盘。
// 默认（未调用）保持存 raw 的现状语义。
func (s *Store) SetKeepRaw(keep bool) { s.skipRaw.Store(!keep) }

// Insert stores an event; on success sets ev.ID.
func (s *Store) Insert(ev *Event) error {
	var raw any
	// keep_raw=false 时跳过原始报文存储（读取侧 raw 允许为空）
	if len(ev.Raw) > 0 && !s.skipRaw.Load() {
		raw = ev.Raw
	}
	res, err := s.db.Exec(
		`INSERT INTO events(msg_id, device_id, type, ts_event, ts_recv, payload, raw)
		 VALUES(?,?,?,?,?,?,?)`,
		ev.MsgID, ev.DeviceID, ev.Type, ev.TSEvent, ev.TSRecv, ev.Payload, raw)
	if err != nil {
		return err
	}
	ev.ID, _ = res.LastInsertId()
	return nil
}

// Query filters events. Empty filters match all.
func (s *Store) Query(device, typ string, sinceID int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	q := `SELECT id, COALESCE(msg_id,''), device_id, type, COALESCE(ts_event,0), ts_recv, COALESCE(payload,'')
	      FROM events WHERE id > ?`
	args := []any{sinceID}
	if device != "" {
		q += ` AND device_id = ?`
		args = append(args, device)
	}
	if typ != "" {
		q += ` AND type = ?`
		args = append(args, typ)
	}
	q += ` ORDER BY id DESC LIMIT ?`
	args = append(args, limit)
	rows, err := s.db.Query(q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.MsgID, &ev.DeviceID, &ev.Type, &ev.TSEvent, &ev.TSRecv, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	return out, rows.Err()
}

// QuerySince 按事件 id 升序返回 sinceID 之后最近 limit 条事件。
// 供 WS 断线补拉使用：取「最近」limit 条而非最早，超出回放窗口的更早
// 事件由客户端转 REST /api/events 查询（快照语义兜底）。
func (s *Store) QuerySince(sinceID int64, limit int) ([]Event, error) {
	if limit <= 0 || limit > 1000 {
		limit = 100
	}
	rows, err := s.db.Query(`SELECT id, COALESCE(msg_id,''), device_id, type, COALESCE(ts_event,0), ts_recv, COALESCE(payload,'')
	      FROM events WHERE id > ? ORDER BY id DESC LIMIT ?`, sinceID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var ev Event
		if err := rows.Scan(&ev.ID, &ev.MsgID, &ev.DeviceID, &ev.Type, &ev.TSEvent, &ev.TSRecv, &ev.Payload); err != nil {
			return nil, err
		}
		out = append(out, ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// SQL 取 DESC（最近优先），反转成升序返回，便于按时间顺序回放
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// RawHex returns the stored raw payload of one event (for the UI's
// "unknown" inspector).
func (s *Store) RawHex(id int64) (string, error) {
	var raw []byte
	err := s.db.QueryRow(`SELECT raw FROM events WHERE id=?`, id).Scan(&raw)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(raw), nil
}

// CountEvents 返回 events 表总行数（O(1) 计数查询，供 /metrics 使用）。
// 替代旧路径"Query 拉取最多 1000 行再数长度"——旧路径既非总量语义，
// 又让每次指标抓取全表捞 1000 行。
func (s *Store) CountEvents() (int64, error) {
	var n int64
	err := s.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&n)
	return n, err
}

// Cleanup enforces retention_days and max_rows.
func (s *Store) Cleanup(retentionDays, maxRows int) error {
	if retentionDays > 0 {
		cut := time.Now().AddDate(0, 0, -retentionDays).Unix()
		if _, err := s.db.Exec(`DELETE FROM events WHERE ts_recv < ?`, cut); err != nil {
			return err
		}
	}
	if maxRows > 0 {
		_, err := s.db.Exec(`DELETE FROM events WHERE id <=
			(SELECT MAX(id) FROM events) - ?`, maxRows)
		return err
	}
	return nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

// UpsertClient registers/refreshes a connected client.
func (s *Store) UpsertClient(id, kind, peer, user string) error {
	_, err := s.db.Exec(`INSERT INTO clients(id,kind,peer,user,connected_at,last_active,active)
		VALUES(?,?,?,?,?,?,1)
		ON CONFLICT(id) DO UPDATE SET last_active=excluded.last_active, active=1`,
		id, kind, peer, user, time.Now().Unix(), time.Now().Unix())
	return err
}

// TouchClient updates last_active.
func (s *Store) TouchClient(id string) error {
	_, err := s.db.Exec(`UPDATE clients SET last_active=? WHERE id=?`, time.Now().Unix(), id)
	return err
}

// CloseClient marks a client disconnected.
func (s *Store) CloseClient(id string) error {
	_, err := s.db.Exec(`UPDATE clients SET active=0, last_active=? WHERE id=?`, time.Now().Unix(), id)
	return err
}

// SweepStaleClients 把 last_active 超过 ttl 仍标记活跃的 web 客户端置为
// 断开（兜底清理：活跃 WS 连接由 ping 周期刷新 last_active，不会被误清；
// 只清 web 类是因为 rtsp/onvif 长会话期间不刷新 last_active，无法区分
// 活跃观看与僵尸行——那类残留由启动时的 SweepAllClientsInactive 处理）。
func (s *Store) SweepStaleClients(ttl time.Duration) error {
	_, err := s.db.Exec(`UPDATE clients SET active=0
		WHERE active=1 AND kind='web' AND last_active < ?`, time.Now().Add(-ttl).Unix())
	return err
}

// SweepAllClientsInactive 把所有仍标记活跃的客户端行置为断开。
// 进程启动时调用：上次运行（如崩溃）来不及 CloseClient 的残留行在此清零。
func (s *Store) SweepAllClientsInactive() error {
	_, err := s.db.Exec(`UPDATE clients SET active=0 WHERE active=1`)
	return err
}

// ActiveClients lists connected clients.
func (s *Store) ActiveClients() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT id,kind,COALESCE(peer,''),COALESCE(user,''),connected_at,last_active
		FROM clients WHERE active=1 ORDER BY last_active DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, kind, peer, user string
		var conn, last int64
		if err := rows.Scan(&id, &kind, &peer, &user, &conn, &last); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{
			"id": id, "kind": kind, "peer": peer, "user": user,
			"connected_at": conn, "last_active": last,
		})
	}
	return out, rows.Err()
}

// ClientCounts returns active counts per kind.
func (s *Store) ClientCounts() (map[string]int, error) {
	rows, err := s.db.Query(`SELECT kind, COUNT(*) FROM clients WHERE active=1 GROUP BY kind`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var k string
		var n int
		if err := rows.Scan(&k, &n); err != nil {
			return nil, err
		}
		out[k] = n
	}
	return out, rows.Err()
}

// Token management (auth module persists here).
func (s *Store) InsertToken(id, hash, scopes, name string) error {
	_, err := s.db.Exec(`INSERT INTO tokens(id,hash,scopes,name,created_at) VALUES(?,?,?,?,?)`,
		id, hash, scopes, name, time.Now().Unix())
	return err
}

func (s *Store) LookupToken(hash string) (id, scopes string, ok bool) {
	err := s.db.QueryRow(`SELECT id, scopes FROM tokens WHERE hash=?`, hash).Scan(&id, &scopes)
	return id, scopes, err == nil
}

func (s *Store) ListTokens() ([]map[string]any, error) {
	rows, err := s.db.Query(`SELECT id,name,scopes,created_at FROM tokens`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id, name, scopes string
		var created int64
		if err := rows.Scan(&id, &name, &scopes, &created); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "name": name, "scopes": scopes, "created_at": created})
	}
	return out, rows.Err()
}

func (s *Store) DeleteToken(id string) error {
	_, err := s.db.Exec(`DELETE FROM tokens WHERE id=?`, id)
	return err
}

// KV helpers.
func (s *Store) KVSet(key, value string) error {
	_, err := s.db.Exec(`INSERT INTO kv(key,value) VALUES(?,?)
		ON CONFLICT(key) DO UPDATE SET value=excluded.value`, key, value)
	return err
}

func (s *Store) KVGet(key string) (string, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM kv WHERE key=?`, key).Scan(&v)
	return v, err
}
