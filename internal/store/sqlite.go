// Package store implements bus.Store on SQLite (pure Go, no cgo), so the
// binary cross-compiles to every platform Go supports.
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"birdie/internal/bus"
)

const schema = `
CREATE TABLE IF NOT EXISTS agents (
	name       TEXT PRIMARY KEY,
	token_hash TEXT NOT NULL UNIQUE,
	delivery   TEXT NOT NULL DEFAULT 'pull',
	config     TEXT NOT NULL DEFAULT '{}',
	created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS messages (
	seq             INTEGER PRIMARY KEY AUTOINCREMENT,
	id              TEXT NOT NULL UNIQUE,
	thread_id       TEXT NOT NULL,
	in_reply_to     TEXT NOT NULL DEFAULT '',
	sender          TEXT NOT NULL,
	recipient       TEXT NOT NULL,
	content         TEXT NOT NULL,
	meta            TEXT NOT NULL DEFAULT '{}',
	status          TEXT NOT NULL,
	attempts        INTEGER NOT NULL DEFAULT 0,
	last_error      TEXT NOT NULL DEFAULT '',
	next_attempt_at INTEGER NOT NULL DEFAULT 0,
	created_at      INTEGER NOT NULL,
	delivered_at    INTEGER
);
CREATE INDEX IF NOT EXISTS messages_inbox ON messages(recipient, status, seq);
CREATE INDEX IF NOT EXISTS messages_thread ON messages(thread_id, seq);
CREATE TABLE IF NOT EXISTS inbox_releases (
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	recipient   TEXT NOT NULL,
	session     TEXT NOT NULL,
	digest      BOOLEAN NOT NULL,
	released_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS inbox_releases_session ON inbox_releases(recipient, session);
CREATE TABLE IF NOT EXISTS state (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sessions (
	agent     TEXT NOT NULL,
	id        TEXT NOT NULL,
	name      TEXT NOT NULL DEFAULT '',
	cwd       TEXT NOT NULL DEFAULT '',
	tier      TEXT NOT NULL DEFAULT 'pull',
	active_at INTEGER NOT NULL,
	last_seen INTEGER NOT NULL,
	PRIMARY KEY (agent, id)
);
`

// columns added to existing databases, oldest first.
var addedColumns = []struct{ table, column, def string }{
	{"messages", "from_session", "TEXT NOT NULL DEFAULT ''"},
	{"messages", "to_session", "TEXT NOT NULL DEFAULT ''"},
	{"messages", "lease_until", "INTEGER NOT NULL DEFAULT 0"},
	// taken_by is the session that took the message; error_at is when
	// last_error happened.
	{"messages", "taken_by", "TEXT NOT NULL DEFAULT ''"},
	{"messages", "error_at", "INTEGER NOT NULL DEFAULT 0"},
	{"sessions", "branch", "TEXT NOT NULL DEFAULT ''"},
	{"sessions", "state", "TEXT NOT NULL DEFAULT ''"},
	{"sessions", "ended_at", "INTEGER NOT NULL DEFAULT 0"},
	// held_at is when the sender was told the message is held for a session
	// that isn't running.
	{"messages", "held_at", "INTEGER NOT NULL DEFAULT 0"},
	// pull_only: the session gets messages only when it checks.
	{"sessions", "pull_only", "INTEGER NOT NULL DEFAULT 0"},
	{"messages", "priority", "INTEGER NOT NULL DEFAULT -1"},
	{"messages", "release_id", "INTEGER NOT NULL DEFAULT 0"},
}

const (
	msgCols     = `m.id, m.thread_id, m.in_reply_to, m.sender, m.recipient, m.from_session, m.to_session, m.content, m.meta, m.status, m.attempts, m.created_at, m.priority`
	agentCols   = `a.name, a.token_hash, a.delivery, a.config, a.created_at`
	sessionCols = `agent, id, name, cwd, branch, tier, state, active_at, last_seen, ended_at, pull_only`
	// trackCols follow msgCols for the delivery details birdie's own commands show.
	trackCols = `COALESCE(m.delivered_at, 0), m.taken_by, m.last_error, m.error_at, m.next_attempt_at`
)

type SQLite struct{ db *sql.DB }

var _ bus.Store = (*SQLite)(nil)

func Open(path string) (*SQLite, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", "file:"+path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	// One connection serializes writers in-process; WAL plus busy_timeout
	// handles the CLI touching the same file concurrently.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	if err := addColumns(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate %s: %w", path, err)
	}
	return &SQLite{db: db}, nil
}

func addColumns(db *sql.DB) error {
	for _, c := range addedColumns {
		var n int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info(?) WHERE name = ?`, c.table, c.column).Scan(&n); err != nil {
			return err
		}
		if n == 0 {
			if _, err := db.Exec(`ALTER TABLE ` + c.table + ` ADD COLUMN ` + c.column + ` ` + c.def); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *SQLite) Close() error { return s.db.Close() }

type scanner interface{ Scan(...any) error }

func scanAgent(r scanner) (bus.Agent, error) {
	var a bus.Agent
	var cfg string
	var created int64
	if err := r.Scan(&a.Name, &a.TokenHash, &a.Delivery, &cfg, &created); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return a, bus.ErrNotFound
		}
		return a, err
	}
	a.CreatedAt = time.UnixMilli(created).UTC()
	if err := json.Unmarshal([]byte(cfg), &a.Config); err != nil {
		return a, err
	}
	a.Description = a.Config["description"]
	a.Label = bus.LabelOf(a.Name, a.Config)
	return a, nil
}

func scanMessage(r scanner) (bus.Message, error) {
	var m bus.Message
	var meta string
	var created int64
	var priority int
	if err := r.Scan(&m.ID, &m.ThreadID, &m.InReplyTo, &m.From, &m.To, &m.FromSession, &m.ToSession, &m.Content, &meta, &m.Status, &m.Attempts, &created, &priority); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return m, bus.ErrNotFound
		}
		return m, err
	}
	m.CreatedAt = time.UnixMilli(created).UTC()
	m.Priority = priorityName(priority)
	if err := json.Unmarshal([]byte(meta), &m.Meta); err != nil {
		return m, err
	}
	if len(m.Meta) == 0 {
		m.Meta = nil
	}
	return m, nil
}

func priorityName(priority int) string {
	switch priority {
	case 0:
		return "p0"
	case 1:
		return "p1"
	case 3:
		return "p3"
	default:
		return ""
	}
}

func collectMessages(rows *sql.Rows, err error) ([]bus.Message, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	msgs := []bus.Message{}
	for rows.Next() {
		m, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, m)
	}
	return msgs, rows.Err()
}

func (s *SQLite) CreateAgent(ctx context.Context, a bus.Agent) error {
	cfg, err := json.Marshal(a.Config)
	if err != nil {
		return err
	}
	if a.Config == nil {
		cfg = []byte("{}")
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO agents (name, token_hash, delivery, config, created_at) VALUES (?, ?, ?, ?, ?)`,
		a.Name, a.TokenHash, a.Delivery, string(cfg), a.CreatedAt.UnixMilli())
	return err
}

func (s *SQLite) DeleteAgent(ctx context.Context, name string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM agents WHERE name = ?`, name)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n == 0 {
		return bus.ErrNotFound
	}
	return nil
}

func (s *SQLite) GetAgent(ctx context.Context, name string) (bus.Agent, error) {
	return scanAgent(s.db.QueryRowContext(ctx, `SELECT `+agentCols+` FROM agents a WHERE a.name = ?`, name))
}

func (s *SQLite) AgentByTokenHash(ctx context.Context, hash string) (bus.Agent, error) {
	return scanAgent(s.db.QueryRowContext(ctx, `SELECT `+agentCols+` FROM agents a WHERE a.token_hash = ?`, hash))
}

func (s *SQLite) ListAgents(ctx context.Context) ([]bus.Agent, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT `+agentCols+` FROM agents a ORDER BY a.name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	agents := []bus.Agent{}
	for rows.Next() {
		a, err := scanAgent(rows)
		if err != nil {
			return nil, err
		}
		agents = append(agents, a)
	}
	return agents, rows.Err()
}

func (s *SQLite) InsertMessage(ctx context.Context, m bus.Message) error {
	return insertMessage(ctx, s.db, m)
}

type execer interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func insertMessage(ctx context.Context, db execer, m bus.Message) error {
	meta := []byte("{}")
	if len(m.Meta) > 0 {
		var err error
		if meta, err = json.Marshal(m.Meta); err != nil {
			return err
		}
	}
	var held int64 // the sender, told at send that the message is held, needs no notice
	if m.Held != "" {
		held = m.CreatedAt.UnixMilli()
	}
	priority := -1
	switch m.Priority {
	case "p0":
		priority = 0
	case "p1":
		priority = 1
	case "p3":
		priority = 3
	}
	_, err := db.ExecContext(ctx,
		`INSERT INTO messages (id, thread_id, in_reply_to, sender, recipient, from_session, to_session, content, meta, status, created_at, held_at, priority)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		m.ID, m.ThreadID, m.InReplyTo, m.From, m.To, m.FromSession, m.ToSession, m.Content, string(meta), m.Status, m.CreatedAt.UnixMilli(), held, priority)
	return err
}

func (s *SQLite) GetMessage(ctx context.Context, id string) (bus.Message, error) {
	return scanMessage(s.db.QueryRowContext(ctx, `SELECT `+msgCols+` FROM messages m WHERE m.id = ?`, id))
}

func (s *SQLite) ThreadAccess(ctx context.Context, threadID, agent string) (bool, bool, error) {
	var total, mine int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*), COALESCE(SUM(sender = ? OR recipient = ?), 0) FROM messages WHERE thread_id = ?`,
		agent, agent, threadID).Scan(&total, &mine)
	return total > 0, mine > 0, err
}

func (s *SQLite) Thread(ctx context.Context, threadID, agent string) ([]bus.Message, error) {
	return collectMessages(s.db.QueryContext(ctx,
		`SELECT `+msgCols+` FROM messages m WHERE m.thread_id = ? AND (m.sender = ? OR m.recipient = ?) ORDER BY m.seq`,
		threadID, agent, agent))
}

func (s *SQLite) HasNotice(ctx context.Context, threadID, agent, kind string) (bool, error) {
	var exists bool
	err := s.db.QueryRowContext(ctx, `SELECT EXISTS (
		SELECT 1 FROM messages WHERE thread_id = ? AND recipient = ?
		AND sender IN (?, ?) AND COALESCE(json_extract(meta, '$.notice'), '') = ?
	)`, threadID, agent, bus.SystemSender, bus.LegacySystemSender, kind).Scan(&exists)
	return exists, err
}

func (s *SQLite) ThreadSession(ctx context.Context, threadID, from, agent string) (string, error) {
	var id string
	// The pair comes first, as it always did: the session of agent that last
	// took part with from, or no session if they only exchanged messages
	// without one. Only when the two never exchanged a message here does a
	// follow-up go where agent last took part in the thread with anyone: the
	// session it sent from or took a message with, or no session if it did
	// so without one, so a reader that isn't a session can take it. A
	// session another sender merely addressed doesn't count there, since it
	// may never have seen the thread, and neither does a message given back
	// (a nack or expiry keeps taken_by). An agent following up to itself
	// gets no fallback, so a session can't hand mail back to itself. ?1 is
	// from, ?2 agent, ?3 the thread.
	const (
		pairSession = `((m.sender = ?1 AND m.recipient = ?2 AND ((m.taken_by != '' AND m.status != 'queued') OR m.to_session != ''))
		     OR (m.sender = ?2 AND m.recipient = ?1 AND m.from_session != ''))`
		pair = `((m.sender = ?1 AND m.recipient = ?2) OR (m.sender = ?2 AND m.recipient = ?1))`
		took = `m.status IN ('claimed', 'delivered', 'replied')`
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT CASE WHEN `+pairSession+` THEN
		          CASE WHEN m.sender = ?2 THEN m.from_session WHEN m.taken_by != '' AND m.status != 'queued' THEN m.taken_by ELSE m.to_session END
		        WHEN `+pair+` THEN ''
		        WHEN m.sender = ?2 AND m.from_session != '' THEN m.from_session
		        WHEN m.recipient = ?2 AND `+took+` THEN m.taken_by
		        ELSE '' END
		 FROM messages m WHERE m.thread_id = ?3
		   AND (`+pair+` OR (?1 != ?2 AND (m.sender = ?2 OR (m.recipient = ?2 AND `+took+`))))
		 ORDER BY CASE WHEN `+pairSession+` THEN 2 WHEN `+pair+` THEN 1 ELSE 0 END DESC, m.seq DESC LIMIT 1`,
		from, agent, threadID).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return id, err
}

// inboxWhere is the condition for messages a ClaimFilter may take. It uses the
// messages alias m.
func inboxWhere(f bus.ClaimFilter, now time.Time) (string, []any) {
	q := ` m.recipient = ? AND (m.status = 'queued' OR (m.status = 'claimed' AND m.lease_until > 0 AND m.lease_until <= ?))`
	args := []any{f.Agent, now.UnixMilli()}
	if f.Thread != "" {
		q += ` AND m.thread_id = ?`
		args = append(args, f.Thread)
	}
	if f.Session == "" {
		// A receiver that isn't a session takes what isn't for a running
		// one: never mail pinned to a session that is open.
		if f.Orphans {
			q += ` AND (m.to_session = '' OR m.to_session NOT IN
				(SELECT id FROM sessions WHERE agent = ? AND last_seen >= ? AND ended_at = 0))`
			args = append(args, f.Agent, f.LiveSince.UnixMilli())
		} else {
			q += ` AND m.to_session = ''`
		}
	} else {
		switch {
		case f.Unpinned && f.Orphans:
			// Mail for a session that has gone falls to this one too.
			q += ` AND (m.to_session = ? OR m.to_session = '' OR m.to_session NOT IN
				(SELECT id FROM sessions WHERE agent = ? AND last_seen >= ? AND ended_at = 0))`
			args = append(args, f.Session, f.Agent, f.LiveSince.UnixMilli())
		case f.Unpinned:
			q += ` AND (m.to_session = ? OR m.to_session = '')`
			args = append(args, f.Session)
		default:
			q += ` AND m.to_session = ?`
			args = append(args, f.Session)
		}
	}
	return q, args
}

// positionGate runs in the claim transaction so overlapping polls cannot
// both reserve the same digest window. The age checks use the same inbox
// predicate as the claim, including leases that have run out.
func positionGate(ctx context.Context, tx *sql.Tx, f bus.ClaimFilter, where string, args []any, now time.Time) (gate string, carry bool, err error) {
	var oldestHeld, oldestP3 sql.NullInt64
	var ready, p0 bool
	err = tx.QueryRowContext(ctx, `SELECT MIN(CASE WHEN m.priority IN (1, 3) THEN m.created_at END),
		MIN(CASE WHEN m.priority = 3 THEN m.created_at END),
		COALESCE(MAX(m.priority IN (-1, 0, 1)), 0),
		COALESCE(MAX(m.priority = 0), 0) FROM messages m WHERE`+where, args...).Scan(&oldestHeld, &oldestP3, &ready, &p0)
	if err != nil {
		return "", false, err
	}

	// P0 brings held mail at either turn position. P3 that fits counts as
	// a digest release, including when it rides with a midturn P0.
	if p0 && f.Thread == "" {
		return "", true, nil
	}
	if f.At == bus.Boundary && (ready || !oldestP3.Valid) {
		return "", false, nil
	}
	if f.At == bus.Midturn && (!oldestHeld.Valid || oldestHeld.Int64 > now.Add(-bus.Valve).UnixMilli()) {
		return " AND m.priority IN (-1, 0)", false, nil
	}

	var lastRelease, lastDigest int64
	err = tx.QueryRowContext(ctx, `SELECT COALESCE(MAX(released_at), 0),
		COALESCE(MAX(CASE WHEN digest THEN released_at END), 0)
		FROM inbox_releases WHERE recipient = ? AND session = ?`, f.Agent, f.Session).Scan(&lastRelease, &lastDigest)
	if err != nil {
		return "", false, err
	}
	if f.At == bus.Midturn {
		if lastRelease == 0 || lastRelease <= now.Add(-bus.Valve).UnixMilli() {
			return "", false, nil
		}
		return " AND m.priority IN (-1, 0)", false, nil
	}
	cutoff := now.Add(-bus.DigestEvery).UnixMilli()
	// Cache age can release fresh mail, but overflow still waits for the
	// next digest window or an immediate message it can ride with.
	if lastDigest == 0 || lastDigest <= cutoff {
		if oldestP3.Int64 <= cutoff {
			return "", false, nil
		}
		var idle bool
		err := tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM sessions
			WHERE agent = ? AND id = ? AND active_at <= ?)`, f.Agent, f.Session, now.Add(-bus.CacheIdle).UnixMilli()).Scan(&idle)
		if err != nil || idle {
			return "", false, err
		}
	}
	return " AND m.priority != 3", false, nil
}

// settleDelay checks both ages after applying the position gate. A fresh
// message that would stay held cannot delay a claim. Using the same
// transaction keeps new arrivals from slipping into a settled batch.
func settleDelay(ctx context.Context, tx *sql.Tx, where string, args []any, now time.Time) (time.Duration, error) {
	var oldest, newest sql.NullInt64
	err := tx.QueryRowContext(ctx, `SELECT MIN(m.created_at), MAX(m.created_at) FROM messages m WHERE`+where, args...).Scan(&oldest, &newest)
	if err != nil || !oldest.Valid {
		return 0, err
	}
	quiet := time.UnixMilli(newest.Int64).Add(bus.Coalesce)
	latest := time.UnixMilli(oldest.Int64).Add(bus.CoalesceMax)
	return max(0, min(quiet.Sub(now), latest.Sub(now))), nil
}

func (s *SQLite) ClaimInbox(ctx context.Context, f bus.ClaimFilter, now time.Time) ([]bus.Message, time.Duration, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	where, args := inboxWhere(f, now)
	order, cols := "m.seq", msgCols
	positioned := f.At != ""
	limit := f.Limit
	if positioned {
		gate, carry, err := positionGate(ctx, tx, f, where, args, now)
		if err != nil {
			return nil, 0, err
		}
		where += gate
		if f.Thread == "" {
			delay, err := settleDelay(ctx, tx, where, args, now)
			if err != nil || delay > 0 {
				return nil, delay, err
			}
		}
		if carry {
			// A push frame can fit more than the normal row limit. Each
			// member takes at least its opening and closing tags, so the
			// bridge cannot fit a prefix longer than this.
			limit = bus.MaxPushFrameBytes / len("<birdie-message></birdie-message>")
		}
		order = "CASE m.priority WHEN 0 THEN 0 WHEN 3 THEN 2 ELSE 1 END, m.seq"
		cols = strings.Replace(msgCols, "m.content", digestContent, 1)
	}
	// Sizes first, so only what fits in one batch is read.
	rows, err := tx.QueryContext(ctx, `SELECT m.id, m.priority, `+contentSize+` FROM messages m WHERE`+where+` ORDER BY `+order+` LIMIT ?`, append(args, limit)...)
	if err != nil {
		return nil, 0, err
	}
	var ids []any
	pointers := make(map[string]int)
	total, digest := 0, 0
	for rows.Next() {
		var id string
		var priority, size int
		if err := rows.Scan(&id, &priority, &size); err != nil {
			rows.Close()
			return nil, 0, err
		}
		if positioned && priority == 3 {
			if size > bus.MaxDigestBytes {
				// An individually oversized message is shown as a pointer so
				// it can leave the queue and the full text stays in its thread.
				pointers[id], size = size, 0
			}
			if digest+size > bus.MaxDigestBytes {
				break
			}
			digest += size
		}
		if len(ids) > 0 && total+size > bus.MaxBatchBytes {
			break
		}
		ids, total = append(ids, id), total+size
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(ids) == 0 {
		return []bus.Message{}, 0, err
	}
	msgs, err := collectMessages(tx.QueryContext(ctx,
		`SELECT `+cols+` FROM messages m WHERE m.id IN (?`+strings.Repeat(", ?", len(ids)-1)+`) ORDER BY `+order, ids...))
	if err != nil {
		return nil, 0, err
	}
	reserved := false
	for i := range msgs {
		m := &msgs[i]
		m.ContentBytes = pointers[m.ID]
		m.Status = bus.StatusClaimed
		var delivered any
		if f.LeaseUntil.IsZero() {
			m.Status, delivered = bus.StatusDelivered, now.UnixMilli()
		} else {
			m.LeaseUntil = f.LeaseUntil.UnixMilli()
		}
		var releaseID int64
		if positioned && (m.Priority == "p3" || (f.At == bus.Midturn && m.Priority == "p1")) {
			// Keep earlier reservations even when an expired member rides
			// with another claim. Returning that claim cancels only its own.
			result, err := tx.ExecContext(ctx, `INSERT INTO inbox_releases (recipient, session, digest, released_at)
				VALUES (?, ?, ?, ?)`, f.Agent, f.Session, m.Priority == "p3", now.UnixMilli())
			if err != nil {
				return nil, 0, err
			}
			releaseID, err = result.LastInsertId()
			if err != nil {
				return nil, 0, err
			}
			reserved = true
		}
		_, err = tx.ExecContext(ctx, `UPDATE messages SET status = ?, delivered_at = ?, lease_until = ?, taken_by = ?,
			release_id = ? WHERE id = ?`, m.Status, delivered, m.LeaseUntil, f.Session, releaseID, m.ID)
		if err != nil {
			return nil, 0, err
		}
	}
	if reserved {
		if _, err := tx.ExecContext(ctx, `DELETE FROM inbox_releases WHERE released_at <= ?`, now.Add(-bus.Valve).UnixMilli()); err != nil {
			return nil, 0, err
		}
	}
	return msgs, 0, tx.Commit()
}

const contentSize = `length(CAST(m.content AS BLOB))`

var digestContent = fmt.Sprintf(`CASE WHEN m.priority = 3 AND %[1]s > %[2]d
	THEN printf('(%%d bytes; read it in the thread)', %[1]s) ELSE m.content END`, contentSize, bus.MaxDigestBytes)

// waiting is the condition for a message still waiting to be taken: queued,
// or claimed with the lease run out.
const waiting = `(m.status = 'queued' OR (m.status = 'claimed' AND m.lease_until > 0 AND m.lease_until <= ?))`

func (s *SQLite) SettleHeld(ctx context.Context, q bus.HoldQuery, build func([]bus.HeldMessage) []bus.Message) ([]bus.HeldMessage, []bus.Message, error) {
	where := `m.sender NOT IN (?, ?) AND ` + waiting + `
		AND m.recipient IN (SELECT name FROM agents WHERE delivery = 'pull')`
	args := []any{bus.SystemSender, bus.LegacySystemSender, q.Now.UnixMilli()}
	session := `m.to_session != '' AND s.ended_at > 0`
	if q.Lapsed {
		session = `m.to_session != '' AND (s.id IS NULL OR s.ended_at > 0 OR s.last_seen < ?)`
		args = append(args, q.LiveSince.UnixMilli())
		if len(q.Unrouted) > 0 {
			// Before sessions were required, a new conversation could be
			// queued for the agent alone, and no session takes it now. Mail
			// in a thread the recipient spoke in, or took a message in
			// without a session, is left alone: it knows that thread, and
			// whoever waits on it or reads without a session takes it (see
			// Bus.Send). A take by a session doesn't count: that session was
			// handed the message, and nothing says it waits on the thread. The
			// route is checked again here, in the same transaction, so a
			// default chosen or fallback turned on since Unrouted was made
			// keeps the mail for the session that can now take it.
			session = `(` + session + `) OR (m.to_session = '' AND m.recipient IN (?` + strings.Repeat(", ?", len(q.Unrouted)-1) + `)
				AND NOT EXISTS (SELECT 1 FROM messages p WHERE p.thread_id = m.thread_id AND p.seq < m.seq
					AND (p.sender = m.recipient OR (p.recipient = m.recipient AND p.status IN ('claimed', 'delivered', 'replied') AND p.taken_by = '')))
				AND NOT EXISTS (SELECT 1 FROM agents a LEFT JOIN sessions d ON d.agent = a.name AND d.ended_at = 0
					AND d.last_seen >= ? AND d.name = json_extract(a.config, '$.default_session')
					WHERE a.name = m.recipient AND (json_extract(a.config, '$.fallback') = 'on' OR d.id IS NOT NULL))
				AND m.created_at < ?)`
			for _, agent := range q.Unrouted {
				args = append(args, agent)
			}
			before := int64(math.MaxInt64)
			if !q.UnpinnedBefore.IsZero() {
				before = q.UnpinnedBefore.UnixMilli()
			}
			args = append(args, q.LiveSince.UnixMilli(), before)
		}
	}
	where += ` AND (` + session + `)`
	if q.Expire {
		// Mail for no session counts from when its sender was told it is
		// held, so it never expires untold, and an upgrade leaves time to
		// choose a default before mail sent long ago expires.
		where += ` AND CASE WHEN m.to_session = '' THEN m.held_at > 0 AND m.held_at < ?
			ELSE MAX(m.created_at, COALESCE(CASE WHEN s.ended_at > 0 THEN s.ended_at ELSE s.last_seen END, 0)) < ? END`
		args = append(args, q.Before.UnixMilli(), q.Before.UnixMilli())
	} else {
		where += ` AND m.held_at = 0`
	}
	args = append(args, q.Limit)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	// A byte prefix covers every preview rune without stopping at embedded NULs.
	// Notices need routing details, not full content or caller-supplied metadata.
	rows, err := tx.QueryContext(ctx, `SELECT m.id, m.thread_id, m.in_reply_to, m.sender, m.recipient,
		m.from_session, m.to_session, substr(CAST(m.content AS BLOB), 1, ?), '{}',
		m.status, m.attempts, m.created_at, m.priority, COALESCE(s.name, '') FROM messages m
		LEFT JOIN sessions s ON s.agent = m.recipient AND s.id = m.to_session
		WHERE `+where+` ORDER BY m.seq LIMIT ?`, append([]any{4 * bus.PreviewRunes}, args...)...)
	if err != nil {
		return nil, nil, err
	}
	var held []bus.HeldMessage
	for rows.Next() {
		var h bus.HeldMessage
		var name string
		m, err := scanMessage(withName{rows, &name})
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		h.Message, h.SessionName = m, name
		held = append(held, h)
	}
	rows.Close()
	if err := rows.Err(); err != nil || len(held) == 0 {
		return nil, nil, err
	}
	for i := range held {
		var err error
		if q.Expire {
			_, err = tx.ExecContext(ctx, `UPDATE messages SET status = 'expired', lease_until = 0 WHERE id = ?`, held[i].ID)
			held[i].Status = bus.StatusExpired
		} else {
			_, err = tx.ExecContext(ctx, `UPDATE messages SET held_at = ? WHERE id = ?`, q.Now.UnixMilli(), held[i].ID)
		}
		if err != nil {
			return nil, nil, err
		}
	}
	notices := build(held)
	for _, n := range notices {
		if err := insertMessage(ctx, tx, n); err != nil {
			return nil, nil, err
		}
	}
	return held, notices, tx.Commit()
}

// withName scans msgCols followed by one more column into name.
type withName struct {
	r    scanner
	name *string
}

func (w withName) Scan(dest ...any) error { return w.r.Scan(append(dest, w.name)...) }

func (s *SQLite) CountInbox(ctx context.Context, f bus.ClaimFilter, now time.Time) (int, error) {
	where, args := inboxWhere(f, now)
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages m WHERE`+where, args...).Scan(&n)
	return n, err
}

func (s *SQLite) CountSent(ctx context.Context, from, to, fromSession, toSession string, since time.Time) (int, error) {
	q := `SELECT COUNT(*) FROM messages WHERE sender = ? AND recipient = ? AND from_session = ? AND created_at >= ?`
	args := []any{from, to, fromSession, since.UnixMilli()}
	if toSession != "" {
		q += ` AND to_session = ?`
		args = append(args, toSession)
	}
	var n int
	err := s.db.QueryRowContext(ctx, q, args...).Scan(&n)
	return n, err
}

func (s *SQLite) DueOutbound(ctx context.Context, now time.Time, limit int) ([]bus.Outbound, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+msgCols+`, `+agentCols+` FROM messages m JOIN agents a ON a.name = m.recipient
		 WHERE m.status = 'queued' AND a.delivery != 'pull' AND m.next_attempt_at <= ?
		 ORDER BY m.seq LIMIT ?`, now.UnixMilli(), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bus.Outbound
	for rows.Next() {
		var o bus.Outbound
		var meta, cfg string
		var mCreated, aCreated int64
		var priority int
		m, a := &o.Message, &o.Agent
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.InReplyTo, &m.From, &m.To, &m.FromSession, &m.ToSession, &m.Content, &meta, &m.Status, &m.Attempts, &mCreated, &priority,
			&a.Name, &a.TokenHash, &a.Delivery, &cfg, &aCreated); err != nil {
			return nil, err
		}
		m.CreatedAt, a.CreatedAt = time.UnixMilli(mCreated).UTC(), time.UnixMilli(aCreated).UTC()
		m.Priority = priorityName(priority)
		if err := json.Unmarshal([]byte(meta), &m.Meta); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(cfg), &a.Config); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *SQLite) MarkDelivered(ctx context.Context, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages SET status = 'delivered', delivered_at = ?, last_error = '' WHERE id = ?`, at.UnixMilli(), id)
	return err
}

func (s *SQLite) MarkClaimed(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE messages SET status = 'claimed', lease_until = 0, last_error = '' WHERE id = ?`, id)
	return err
}

func (s *SQLite) Settle(ctx context.Context, id, agent string, ok bool, at time.Time, leaseUntil int64) (bool, error) {
	if !ok {
		return s.returnClaim(ctx, id, agent, leaseUntil)
	}
	res, err := s.db.ExecContext(ctx, `UPDATE messages SET status = 'delivered', delivered_at = ?, lease_until = 0
		WHERE id = ? AND recipient = ? AND status = 'claimed' AND (?4 = 0 OR lease_until = ?4)`, at.UnixMilli(), id, agent, leaseUntil)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *SQLite) returnClaim(ctx context.Context, id, agent string, leaseUntil int64) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var releaseID int64
	err = tx.QueryRowContext(ctx, `UPDATE messages SET status = 'queued', lease_until = 0
		WHERE id = ? AND recipient = ? AND status = 'claimed' AND (?3 = 0 OR lease_until = ?3) RETURNING release_id`, id, agent, leaseUntil).Scan(&releaseID)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if releaseID != 0 {
		if _, err := tx.ExecContext(ctx, `DELETE FROM inbox_releases WHERE id = ?`, releaseID); err != nil {
			return false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (s *SQLite) MarkReplied(ctx context.Context, id, agent, session string, sameAgent bool) (bool, error) {
	res, err := s.db.ExecContext(ctx, `UPDATE messages SET status = 'replied', lease_until = 0,
		delivered_at = COALESCE(delivered_at, ?1)
		WHERE id = ?2 AND recipient = ?3 AND status != 'failed'
		  AND (status != 'replied' OR lease_until != 0 OR delivered_at IS NULL)
		  AND (status IN ('delivered', 'replied', 'expired')
		       OR (?5 = '' AND NOT ?4)
		       OR (?5 != '' AND (
		           (status = 'claimed' AND taken_by = ?5)
		           OR (status = 'queued' AND (to_session = ?5 OR (to_session = '' AND NOT ?4))))))`,
		time.Now().UnixMilli(), id, agent, sameAgent, session)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func (s *SQLite) ReleaseOutbound(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `UPDATE messages SET status = 'queued', next_attempt_at = 0 WHERE status = 'claimed'
		AND recipient IN (SELECT name FROM agents WHERE delivery != 'pull')`)
	return err
}

func (s *SQLite) UpsertSession(ctx context.Context, ses bus.Session) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM sessions WHERE last_seen < ?`, ses.LastSeen.Add(-bus.SessionKeep).UnixMilli()); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO sessions (agent, id, name, cwd, branch, tier, state, active_at, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(agent, id) DO UPDATE SET name = excluded.name, cwd = excluded.cwd, branch = excluded.branch, tier = excluded.tier,
			state = excluded.state, active_at = MAX(active_at, excluded.active_at), last_seen = excluded.last_seen, ended_at = 0`,
		ses.Agent, ses.ID, ses.Name, ses.Cwd, ses.Branch, ses.Tier, ses.State, ses.ActiveAt.UnixMilli(), ses.LastSeen.UnixMilli())
	return err
}

func (s *SQLite) EndSession(ctx context.Context, agent, id string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `UPDATE sessions SET ended_at = ? WHERE agent = ? AND id = ?`, at.UnixMilli(), agent, id)
	return err
}

func (s *SQLite) MoveSession(ctx context.Context, agent, from, to string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, q := range []string{
		`UPDATE messages SET to_session = ? WHERE recipient = ? AND to_session = ?`,
		`UPDATE messages SET taken_by = ? WHERE recipient = ? AND taken_by = ?`,
		`UPDATE inbox_releases SET session = ? WHERE recipient = ? AND session = ?`,
		`UPDATE messages SET from_session = ? WHERE sender = ? AND from_session = ?`,
	} {
		if _, err := tx.ExecContext(ctx, q, to, agent, from); err != nil {
			return err
		}
	}
	// The same conversation: it stays pull-only if its stand-in was.
	if _, err := tx.ExecContext(ctx, `UPDATE sessions SET pull_only = 1 WHERE agent = ? AND id = ?
		AND EXISTS (SELECT 1 FROM sessions WHERE agent = ? AND id = ? AND pull_only = 1)`, agent, to, agent, from); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *SQLite) SetSessionPullOnly(ctx context.Context, agent, id string, on bool) error {
	res, err := s.db.ExecContext(ctx, `UPDATE sessions SET pull_only = ? WHERE agent = ? AND id = ?`, on, agent, id)
	if err != nil {
		return err
	}
	if n, err := res.RowsAffected(); err != nil {
		return err
	} else if n == 0 {
		return fmt.Errorf("%w: session %s of %s", bus.ErrNotFound, id, agent)
	}
	return nil
}

func (s *SQLite) SessionPullOnly(ctx context.Context, agent, id string) (bool, error) {
	var on bool
	err := s.db.QueryRowContext(ctx, `SELECT pull_only FROM sessions WHERE agent = ? AND id = ?`, agent, id).Scan(&on)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return on, err
}

func (s *SQLite) LastSeenBefore(ctx context.Context, t time.Time) (time.Time, error) {
	var ms int64
	err := s.db.QueryRowContext(ctx, `SELECT COALESCE(MAX(last_seen), 0) FROM sessions WHERE last_seen < ?`, t.UnixMilli()).Scan(&ms)
	return unixMilli(ms), err
}

func (s *SQLite) LiveSessions(ctx context.Context, agent string, since time.Time) ([]bus.Session, error) {
	return collectSessions(s.db.QueryContext(ctx, `SELECT `+sessionCols+` FROM sessions
		WHERE agent = ? AND last_seen >= ? AND ended_at = 0 ORDER BY active_at DESC, id`, agent, since.UnixMilli()))
}

func (s *SQLite) RecentSessions(ctx context.Context, agent string, since time.Time) ([]bus.Session, error) {
	return collectSessions(s.db.QueryContext(ctx, `SELECT `+sessionCols+` FROM sessions
		WHERE agent = ? AND last_seen >= ? ORDER BY active_at DESC, id`, agent, since.UnixMilli()))
}

func collectSessions(rows *sql.Rows, err error) ([]bus.Session, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []bus.Session
	for rows.Next() {
		var ses bus.Session
		var active, seen, ended int64
		if err := rows.Scan(&ses.Agent, &ses.ID, &ses.Name, &ses.Cwd, &ses.Branch, &ses.Tier, &ses.State, &active, &seen, &ended, &ses.PullOnly); err != nil {
			return nil, err
		}
		if ses.PullOnly {
			ses.Tier = bus.TierPull
		}
		ses.ActiveAt, ses.LastSeen = time.UnixMilli(active).UTC(), time.UnixMilli(seen).UTC()
		ses.EndedAt = unixMilli(ended)
		out = append(out, ses)
	}
	return out, rows.Err()
}

// unixMilli reads a time stored as milliseconds, where 0 means none.
func unixMilli(ms int64) time.Time {
	if ms == 0 {
		return time.Time{}
	}
	return time.UnixMilli(ms).UTC()
}

func (s *SQLite) RecordFailure(ctx context.Context, id, errMsg string, next time.Time, failed bool) error {
	status := bus.StatusQueued
	if failed {
		status = bus.StatusFailed
	}
	_, err := s.db.ExecContext(ctx,
		`UPDATE messages SET attempts = attempts + 1, last_error = ?, error_at = ?, next_attempt_at = ?, status = ? WHERE id = ?`,
		errMsg, time.Now().UnixMilli(), next.UnixMilli(), status, id)
	return err
}

// Delivery sums up how messages to one agent are getting through.
type Delivery struct {
	// Waiting counts messages the agent hasn't got yet. Oldest is when the
	// oldest of them was sent.
	Waiting int
	Oldest  time.Time
	// Delivered is when a message last reached the agent.
	Delivered time.Time
	// Error is why the latest try failed, at ErrorAt, when no message has got
	// through since. ErrorID is the message tried. Retry is when the next try
	// is due; it is zero when birdie gave up on that message.
	Error   string
	ErrorID string
	ErrorAt time.Time
	Retry   time.Time
	// GaveUp counts messages birdie stopped trying to deliver since the time
	// asked about.
	GaveUp int
}

// Deliveries sums up delivery to each agent that has been sent a message,
// counting messages given up on since a time.
func (s *SQLite) Deliveries(ctx context.Context, since time.Time) (map[string]Delivery, error) {
	out, err := s.deliveryCounts(ctx, since)
	if err != nil {
		return nil, err
	}
	return out, s.addFailures(ctx, out)
}

func (s *SQLite) deliveryCounts(ctx context.Context, since time.Time) (map[string]Delivery, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT recipient,
			COALESCE(SUM(status IN ('queued', 'claimed')), 0),
			COALESCE(MIN(CASE WHEN status IN ('queued', 'claimed') THEN created_at END), 0),
			COALESCE(MAX(delivered_at), 0),
			COALESCE(SUM(status = 'failed' AND error_at >= ?), 0)
		FROM messages GROUP BY recipient`, since.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]Delivery{}
	for rows.Next() {
		var name string
		var d Delivery
		var oldest, delivered int64
		if err := rows.Scan(&name, &d.Waiting, &oldest, &delivered, &d.GaveUp); err != nil {
			return nil, err
		}
		d.Oldest, d.Delivered = unixMilli(oldest), unixMilli(delivered)
		out[name] = d
	}
	return out, rows.Err()
}

// addFailures records each agent's latest failure, if nothing got through
// after it.
func (s *SQLite) addFailures(ctx context.Context, out map[string]Delivery) error {
	rows, err := s.db.QueryContext(ctx, `SELECT recipient, id, last_error, error_at, status, next_attempt_at FROM messages
		WHERE last_error != '' AND error_at > 0 AND status IN ('queued', 'failed') ORDER BY error_at`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var name, id, msg, status string
		var at, next int64
		if err := rows.Scan(&name, &id, &msg, &at, &status, &next); err != nil {
			return err
		}
		d := out[name]
		if t := unixMilli(at); t.After(d.Delivered) {
			d.Error, d.ErrorID, d.ErrorAt, d.Retry = msg, id, t, time.Time{}
			if status == bus.StatusQueued {
				d.Retry = unixMilli(next)
			}
			out[name] = d
		}
	}
	return rows.Err()
}

// Tracked is a message with how its delivery went.
type Tracked struct {
	bus.Message
	// Delivered is when the recipient got it, and TakenBy the session that
	// took it, if one did.
	Delivered time.Time
	TakenBy   string
	// Error is why the latest try failed, at ErrorAt. Retry is when the next
	// try is due while it is queued.
	Error   string
	ErrorAt time.Time
	Retry   time.Time
}

func collectTracked(rows *sql.Rows, err error) ([]Tracked, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Tracked{}
	for rows.Next() {
		var t Tracked
		var meta string
		var created, delivered, errorAt, next int64
		var priority int
		m := &t.Message
		if err := rows.Scan(&m.ID, &m.ThreadID, &m.InReplyTo, &m.From, &m.To, &m.FromSession, &m.ToSession, &m.Content, &meta, &m.Status, &m.Attempts, &created, &priority,
			&delivered, &t.TakenBy, &t.Error, &errorAt, &next); err != nil {
			return nil, err
		}
		if err := json.Unmarshal([]byte(meta), &m.Meta); err != nil {
			return nil, err
		}
		m.CreatedAt = time.UnixMilli(created).UTC()
		m.Priority = priorityName(priority)
		t.Delivered, t.ErrorAt = unixMilli(delivered), unixMilli(errorAt)
		if m.Status == bus.StatusQueued && t.Error != "" {
			t.Retry = unixMilli(next)
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// TrackThread returns every message in a thread, whoever sent it, oldest first.
func (s *SQLite) TrackThread(ctx context.Context, threadID string) ([]Tracked, error) {
	return collectTracked(s.db.QueryContext(ctx,
		`SELECT `+msgCols+`, `+trackCols+` FROM messages m WHERE m.thread_id = ? ORDER BY m.seq`, threadID))
}

// Waiting returns up to limit messages that haven't reached their recipient
// yet, oldest first, and how many there are in all.
func (s *SQLite) Waiting(ctx context.Context, limit int) ([]Tracked, int, error) {
	var n int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM messages WHERE status IN ('queued', 'claimed')`).Scan(&n); err != nil {
		return nil, 0, err
	}
	msgs, err := collectTracked(s.db.QueryContext(ctx,
		`SELECT `+msgCols+`, `+trackCols+` FROM messages m WHERE m.status IN ('queued', 'claimed') ORDER BY m.seq LIMIT ?`, limit))
	return msgs, n, err
}

// GetState returns adapter state saved under key, or bus.ErrNotFound.
func (s *SQLite) GetState(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM state WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", bus.ErrNotFound
	}
	return v, err
}

func (s *SQLite) SetState(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO state (key, value) VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

// SetAgentConfig sets one config key for an agent; an empty value removes it.
// SetAgentDelivery changes how an agent gets its messages. Moving a pull agent
// to a pushed delivery sends whatever is still queued for it.
func (s *SQLite) SetAgentDelivery(ctx context.Context, name, delivery string) error {
	if _, err := s.GetAgent(ctx, name); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `UPDATE agents SET delivery = ? WHERE name = ?`, delivery, name)
	return err
}

func (s *SQLite) SetAgentConfig(ctx context.Context, name, key, value string) error {
	a, err := s.GetAgent(ctx, name)
	if err != nil {
		return err
	}
	if a.Config == nil {
		a.Config = map[string]string{}
	}
	if value == "" {
		delete(a.Config, key)
	} else {
		a.Config[key] = value
	}
	cfg, err := json.Marshal(a.Config)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx, `UPDATE agents SET config = ? WHERE name = ?`, string(cfg), name)
	return err
}
