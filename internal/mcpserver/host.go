package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"birdie/internal/bus"
)

// SessionInfo describes the conversation a bridge serves right now.
type SessionInfo struct {
	ID, Name, Cwd string
	// State is one of the bus.State values, or empty when the host can't tell.
	State    string
	ActiveAt time.Time
}

// Host is the agent program a bridge runs under. It knows which conversation
// the bridge serves and how to put a message in front of the model.
type Host interface {
	Session() SessionInfo
	// Tier says how soon the session sees a message right now.
	Tier() string
	// Push shows the message to the model of conversation session, the one
	// it was taken for. If the host now serves another conversation (after
	// /clear), it returns ErrOtherConversation and shows nothing. It is only
	// called at TierPush.
	Push(ctx context.Context, session string, m bus.Message) error
	// Observe sees the _meta of each tool call, which is the only place some
	// agents name their conversation.
	Observe(meta json.RawMessage)
}

// ErrOtherConversation is Push refusing a message taken for a conversation
// the host no longer serves.
var ErrOtherConversation = errors.New("the session has moved to another conversation")

// errUnsure is a host that can't tell which conversation it serves right now.
var errUnsure = errors.New("can't tell which conversation the session serves right now")

// HostOptions configure DetectHost.
type HostOptions struct {
	// NoPush keeps messages from interrupting the session; they wait for
	// check_inbox.
	NoPush bool
}

// DetectHost picks the Host for the MCP client that started this bridge. The
// client's own name decides first, because environment variables are inherited
// when one agent starts another from its shell.
func DetectHost(client string, opts HostOptions) Host {
	name := strings.ToLower(client)
	switch {
	case strings.Contains(name, "codex"):
		return &codexHost{noPush: opts.NoPush, fallback: procSession()}
	case strings.Contains(name, "grok"):
		return &grokHost{fallback: procSession()}
	case strings.Contains(name, "claude"):
		return &claudeHost{noPush: opts.NoPush, fallback: procSession()}
	case os.Getenv("GROK_SESSION_ID") != "":
		return &grokHost{fallback: procSession()}
	case os.Getenv("CLAUDECODE") != "" || os.Getenv("CLAUDE_CODE_SESSION_ID") != "":
		return &claudeHost{noPush: opts.NoPush, fallback: procSession()}
	}
	return &plainHost{info: procSession()}
}

func procSession() SessionInfo {
	cwd, _ := os.Getwd()
	return SessionInfo{ID: procID(), Name: slug(filepath.Base(cwd)), Cwd: cwd}
}

// procID is the stand-in session id a bridge uses until its host names the
// conversation.
func procID() string { return fmt.Sprintf("proc-%d", os.Getpid()) }

// slugBad leaves dashes out of the kept characters, so a run of them and of
// anything else becomes one dash: "Plugin - Birdie" is plugin-birdie, not
// plugin---birdie.
var slugBad = regexp.MustCompile(`[^a-z0-9._]+`)

// slug turns a title or folder name into something usable in an address.
func slug(s string) string {
	s = strings.Trim(slugBad.ReplaceAllString(strings.ToLower(s), "-"), "-._")
	if len(s) > 48 {
		s = strings.TrimRight(s[:48], "-._")
	}
	return s
}

// plainHost is any other MCP client: tools work, and messages wait for check_inbox.
type plainHost struct{ info SessionInfo }

func (h *plainHost) Session() SessionInfo    { return h.info }
func (h *plainHost) Tier() string            { return bus.TierPull }
func (h *plainHost) Observe(json.RawMessage) {}
func (h *plainHost) Push(context.Context, string, bus.Message) error {
	return errors.New("this agent can't be pushed to")
}

// claudeHost is Claude Code. It keeps a registry entry for each running
// process, ~/.claude/sessions/<pid>.json, naming its conversation and inbox
// socket. The entry follows the conversation through /clear.
type claudeHost struct {
	noPush   bool
	fallback SessionInfo

	once sync.Once
	pid  int

	mu   sync.Mutex
	last *claudeEntry // the last entry read whole
	// failingSince is when reads started failing one after another, each
	// soon after the last (lastFailure), since the last whole read.
	failingSince, lastFailure time.Time
}

// registryGrace is how long whole reads of the registry may fail before the
// session stops counting as push. A read cut short by a rewrite fails for a
// moment; one failing for longer means the registry can't be read, and then
// nothing says which conversation the process serves, so messages wait, for
// check_inbox too, until a whole read does (see conversation).
const registryGrace = 10 * time.Second

// failureGap is how close failed reads must come to count as one run. It is
// longer than the pump's slowest retry (5 s) while it can't tell, so an outage
// stays one run, and shorter than the 30 s between an idle session's reports,
// so two reads cut short by unrelated rewrites don't make one.
const failureGap = 6 * time.Second

type claudeEntry struct {
	SessionID       string `json:"sessionId"`
	Name            string `json:"name"`
	NameSource      string `json:"nameSource"`
	Cwd             string `json:"cwd"`
	Status          string `json:"status"`
	StatusUpdatedAt int64  `json:"statusUpdatedAt"`
	Socket          string `json:"messagingSocketPath"`
}

func claudeDir() string {
	if d := os.Getenv("CLAUDE_CONFIG_DIR"); d != "" {
		return d
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".claude")
}

// entry reads the process's registry entry. When the read fails or is cut
// short, as while Claude Code rewrites the entry at /clear, e is the last
// whole entry, if there was one. That keeps the session's identity steady:
// the environment would name the conversation the process started with. But
// a remembered entry can't say the conversation hasn't changed since, so sure
// is false then (see conversation).
func (h *claudeHost) entry() (e claudeEntry, ok, sure bool) {
	h.once.Do(func() { h.pid = claudeAncestor() })
	if h.pid == 0 {
		return claudeEntry{}, false, true
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	b, err := os.ReadFile(filepath.Join(claudeDir(), "sessions", fmt.Sprintf("%d.json", h.pid)))
	if err != nil || json.Unmarshal(b, &e) != nil {
		if h.last == nil {
			return claudeEntry{}, false, true
		}
		// A failure long after the last one starts a new run: a read cut
		// short a while ago says nothing about the registry now.
		now := time.Now()
		if h.failingSince.IsZero() || now.Sub(h.lastFailure) > failureGap {
			h.failingSince = now
		}
		h.lastFailure = now
		return *h.last, true, false
	}
	h.last, h.failingSince, h.lastFailure = &e, time.Time{}, time.Time{}
	return e, true, true
}

// unreadable reports whether whole reads of the registry have failed for
// longer than registryGrace.
func (h *claudeHost) unreadable() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return !h.failingSince.IsZero() && time.Since(h.failingSince) >= registryGrace
}

// conversation is the session's conversation as the registry says right now,
// or errUnsure when only a remembered entry says (see entry). With no
// registry entry at all, it is the conversation in the environment.
func (h *claudeHost) conversation() (string, error) {
	e, ok, sure := h.entry()
	if !sure {
		return "", errUnsure
	}
	return h.info(e, ok).ID, nil
}

func (h *claudeHost) Session() SessionInfo {
	e, ok, _ := h.entry()
	return h.info(e, ok)
}

// info is the session as registry entry e describes it.
func (h *claudeHost) info(e claudeEntry, ok bool) SessionInfo {
	info := h.fallback
	if id := os.Getenv("CLAUDE_CODE_SESSION_ID"); id != "" {
		info.ID = id
	}
	if !ok {
		return info
	}
	if e.SessionID != "" {
		info.ID = e.SessionID
	}
	if e.Cwd != "" {
		info.Cwd = e.Cwd
		info.Name = slug(filepath.Base(e.Cwd))
	}
	// A name someone chose wins. One Claude made up from the conversation is
	// long, so it only stands in when the folder gives nothing.
	if e.Name != "" && (e.NameSource != "auto" && e.NameSource != "derived" || info.Name == "") {
		info.Name = slug(e.Name)
	}
	info.State = claudeStates[e.Status]
	switch {
	case e.Status == "busy":
		info.ActiveAt = time.Now()
	case e.StatusUpdatedAt > 0:
		info.ActiveAt = time.UnixMilli(e.StatusUpdatedAt)
	}
	return info
}

// claudeStates maps the status in Claude Code's registry entry to the bus's.
var claudeStates = map[string]string{"busy": bus.StateBusy, "idle": bus.StateIdle, "waiting": bus.StateWaiting}

func (h *claudeHost) socket() string {
	e, _, _ := h.entry()
	return socketOf(e)
}

func socketOf(e claudeEntry) string {
	if s := os.Getenv("CLAUDE_CODE_MESSAGING_SOCKET"); s != "" {
		return s
	}
	return e.Socket
}

func (h *claudeHost) Tier() string {
	if h.noPush || runtime.GOOS == "windows" || h.socket() == "" || h.unreadable() {
		return bus.TierPull
	}
	return bus.TierPush
}

// Push writes a user frame to the session's inbox socket: Claude Code shows it
// between tool calls, or starts a turn if the session is idle, and marks it as
// coming from a peer. No token is sent on purpose, so a session that skips
// permission prompts still holds the message for its user to review.
func (h *claudeHost) Push(ctx context.Context, session string, m bus.Message) error {
	// The socket belongs to the process, not the conversation, and stays the
	// same through /clear. So the conversation is checked against the same
	// read of the registry the socket comes from, just before writing.
	e, ok, sure := h.entry()
	if !sure {
		return errUnsure
	}
	if h.info(e, ok).ID != session {
		return ErrOtherConversation
	}
	path := socketOf(e)
	if path == "" {
		return errors.New("no inbox socket")
	}
	var d net.Dialer
	conn, err := d.DialContext(ctx, "unix", path)
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	frame, err := json.Marshal(map[string]any{
		"type":    "user",
		"message": map[string]string{"role": "user", "content": MessageText(m)},
	})
	if err != nil {
		return err
	}
	_, err = conn.Write(append(frame, '\n'))
	return err
}

func (h *claudeHost) Observe(json.RawMessage) {}

func claudeAncestor() int {
	pid := os.Getppid()
	for depth := 0; depth < 8 && pid > 1; depth++ {
		ppid, argv, err := procInfo(pid)
		if err != nil {
			return 0
		}
		if isClaudeCode(argv) {
			return pid
		}
		pid = ppid
	}
	return 0
}

// maxArgBytes keeps a message inside what one command-line argument can hold.
const maxArgBytes = 100 << 10

// codexHost is Codex. Its bridge isn't told the conversation until a tool call
// names it, so a session becomes reachable by push after its first tool call.
// `codex queue` then adds to the thread and starts a turn if it is idle.
type codexHost struct {
	noPush   bool
	fallback SessionInfo

	mu     sync.Mutex
	thread string
}

func (h *codexHost) current() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.thread != "" {
		return h.thread
	}
	return os.Getenv("CODEX_THREAD_ID")
}

func (h *codexHost) Session() SessionInfo {
	info := h.fallback
	if t := h.current(); t != "" {
		info.ID = t
	}
	return info
}

func (h *codexHost) Tier() string {
	if h.noPush || h.current() == "" {
		return bus.TierPull
	}
	if _, err := exec.LookPath("codex"); err != nil {
		return bus.TierPull
	}
	return bus.TierPush
}

func (h *codexHost) Push(ctx context.Context, session string, m bus.Message) error {
	thread := h.current()
	if thread == "" || thread != session {
		return ErrOtherConversation
	}
	text := MessageText(m)
	if len(text) > maxArgBytes {
		text = fmt.Sprintf("<birdie-message from=%q message_id=%q thread_id=%q>\nThis message is too long to show here. Read it with the birdie get_thread tool, and answer with send_message, in_reply_to %q.\n</birdie-message>", m.From, m.ID, m.ThreadID, m.ID)
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "codex", "queue", "--thread", thread, "--message", text)
	cmd.Dir = h.fallback.Cwd
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("codex queue: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

func (h *codexHost) Observe(meta json.RawMessage) {
	var m struct {
		Turn struct {
			SessionID string `json:"session_id"`
			ThreadID  string `json:"thread_id"`
		} `json:"x-codex-turn-metadata"`
		SessionID string `json:"sessionId"`
		ThreadID  string `json:"threadId"`
	}
	if json.Unmarshal(meta, &m) != nil {
		return
	}
	// The session id is the root conversation; a subagent's thread id differs.
	for _, id := range []string{m.Turn.SessionID, m.SessionID, m.Turn.ThreadID, m.ThreadID} {
		if id != "" {
			h.mu.Lock()
			h.thread = id
			h.mu.Unlock()
			return
		}
	}
}

// GrokHooksPath is where `birdie setup grok` puts its hooks.
func GrokHooksPath() string {
	dir := os.Getenv("GROK_HOME")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".grok")
	}
	return filepath.Join(dir, "hooks", "birdie.json")
}

// grokHost is the Grok CLI. With the hooks from `birdie setup grok`, it sees
// messages at its next tool call or end of turn. Nothing wakes an idle session.
type grokHost struct{ fallback SessionInfo }

func (h *grokHost) Session() SessionInfo {
	info := h.fallback
	if id := os.Getenv("GROK_SESSION_ID"); id != "" {
		info.ID = id
	}
	return info
}

func (h *grokHost) Tier() string {
	if _, err := os.Stat(GrokHooksPath()); err == nil {
		return bus.TierTurn
	}
	return bus.TierPull
}

func (h *grokHost) Observe(json.RawMessage) {}
func (h *grokHost) Push(context.Context, string, bus.Message) error {
	return errors.New("grok sessions get messages through hooks")
}
