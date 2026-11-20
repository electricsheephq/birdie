package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

type observedClaim struct {
	at, wait string
	resume   chan func()
}

type claudeInbox struct {
	srv    *Server
	st     *store.SQLite
	sender map[string]*client.Client
	polls  chan observedClaim
	frames chan string
	now    atomic.Int64
	status func(string)
}

func newClaudeInbox(t *testing.T, initialStatus string) *claudeInbox {
	t.Helper()
	f := &claudeInbox{sender: make(map[string]*client.Client), polls: make(chan observedClaim), frames: make(chan string, 16)}
	f.now.Store(time.Now().UnixMilli())
	dir := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", dir)
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "")
	t.Setenv("CLAUDE_CODE_SESSION_ID", "desk")
	if err := os.Mkdir(filepath.Join(dir, "sessions"), 0o700); err != nil {
		t.Fatal(err)
	}
	sockDir, err := os.MkdirTemp("", "bd")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.Remove(sockDir) })
	socket := filepath.Join(sockDir, "s.sock")
	l, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			var frame struct {
				Message struct{ Content string }
			}
			err = json.NewDecoder(c).Decode(&frame)
			c.Close()
			if err == nil {
				f.frames <- frame.Message.Content
			}
		}
	}()
	f.status = func(status string) {
		t.Helper()
		file := filepath.Join(dir, "sessions", "4242.json")
		data, _ := json.Marshal(map[string]any{"sessionId": "desk", "status": status, "statusUpdatedAt": f.now.Load(), "messagingSocketPath": socket})
		if err := os.WriteFile(file+".new", data, 0o600); err != nil {
			t.Error(err)
			return
		}
		if err := os.Rename(file+".new", file); err != nil {
			t.Error(err)
		}
	}
	f.status(initialStatus)

	ctx := context.Background()
	f.st, err = store.Open(filepath.Join(dir, "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { f.st.Close() })
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(f.st, quiet)
	b.Now = func() time.Time { return time.UnixMilli(f.now.Load()) }
	for _, name := range []string{"claude", "alice", "bob", "carol"} {
		if err := f.st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: b.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	handler := httpapi.New(b, quiet)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if r.URL.Path != "/v1/inbox" || q.Get("push") != "1" {
			handler.ServeHTTP(w, r)
			return
		}
		poll := observedClaim{q.Get("at"), q.Get("wait"), make(chan func(), 1)}
		select {
		case f.polls <- poll:
		case <-r.Context().Done():
			return
		}
		select {
		case afterClaim := <-poll.resume:
			if afterClaim == nil {
				handler.ServeHTTP(w, r)
				return
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, r)
			afterClaim()
			w.WriteHeader(response.Code)
			response.Body.WriteTo(w)
		case <-r.Context().Done():
			return
		}
	}))
	t.Cleanup(api.Close)
	for _, name := range []string{"alice", "bob", "carol"} {
		f.sender[name] = client.New(api.URL, name+"-tok")
	}
	h := &claudeHost{pid: 4242}
	h.once.Do(func() {})
	f.srv = New(client.New(api.URL, "claude-tok"), "claude", "test", false, log.New(io.Discard, "", 0))
	f.srv.host = h
	f.srv.syncSession(ctx, h)
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		f.srv.pump(ctx)
	}()
	t.Cleanup(func() { cancel(); <-done })
	return f
}

func (f *claudeInbox) poll(t *testing.T, at string) observedClaim {
	t.Helper()
	select {
	case p := <-f.polls:
		if p.at != at || (at == "boundary" && p.wait != "5") {
			t.Fatalf("poll at=%q wait=%q, want at=%q and wait=5 at boundaries", p.at, p.wait, at)
		}
		return p
	case <-time.After(7 * time.Second):
		t.Fatal("no next push poll")
		return observedClaim{}
	}
}

func (f *claudeInbox) send(t *testing.T, from, priority, content string) string {
	t.Helper()
	m, err := f.sender[from].Send(context.Background(), bus.SendInput{To: "claude/desk", Priority: priority, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	return m.ID
}

var frameID = regexp.MustCompile(`<birdie-message [^>]*message_id="([^"]+)"`)

func (f *claudeInbox) frame(t *testing.T, ids ...string) string {
	t.Helper()
	select {
	case content := <-f.frames:
		var got []string
		for _, match := range frameID.FindAllStringSubmatch(content, -1) {
			got = append(got, match[1])
		}
		if !slices.Equal(got, ids) {
			t.Fatalf("socket frame ids = %v, want %v", got, ids)
		}
		return content
	case <-time.After(7 * time.Second):
		t.Fatal("no socket frame")
		return ""
	}
}

func (f *claudeInbox) statusIs(t *testing.T, id, want string) {
	t.Helper()
	m, err := f.st.GetMessage(context.Background(), id)
	if err != nil || m.Status != want {
		t.Fatalf("%s status = %q, %v, want %q", id, m.Status, err, want)
	}
}

func TestAClaudeStreamReachesOneSocketFramePerDigest(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	for range 3 {
		f.status("idle")
		f.srv.syncSession(context.Background(), f.srv.host)
		var ids []string
		for _, from := range []string{"alice", "bob", "carol"} {
			ids = append(ids, f.send(t, from, "p3", "progress"))
		}
		f.now.Add(bus.DigestEvery.Milliseconds())
		poll.resume <- nil
		f.frame(t, ids...)
		poll = f.poll(t, "boundary")
		for _, id := range ids {
			f.statusIs(t, id, bus.StatusDelivered)
		}
	}
	select {
	case frame := <-f.frames:
		t.Fatalf("unexpected repeat frame: %q", frame)
	default:
	}
}

func TestABusyClaudeShowsOnlyP0UntilItsTurnEnds(t *testing.T) {
	f := newClaudeInbox(t, "busy")
	poll := f.poll(t, "midturn")
	p1 := f.send(t, "alice", "p1", "answer")
	p3 := f.send(t, "bob", "p3", "progress")
	p0 := f.send(t, "carol", "p0", "stop")
	f.status("idle")
	poll.resume <- nil
	f.frame(t, p0)
	poll = f.poll(t, "boundary")
	f.statusIs(t, p1, bus.StatusQueued)
	f.statusIs(t, p3, bus.StatusQueued)
	poll.resume <- nil
	f.frame(t, p1, p3)
}

func TestAnIdleClaudePollTakesNewP1AndThenP3ComingDue(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	poll.resume <- nil
	p1 := f.send(t, "alice", "p1", "answer")
	f.frame(t, p1)

	poll = f.poll(t, "boundary")
	p3 := f.send(t, "bob", "p3", "progress")
	poll.resume <- func() {
		// After this short poll's final claim attempt, the P3 comes due
		// without a new send or a change to the registry's idle status.
		f.now.Add(bus.DigestEvery.Milliseconds())
	}
	poll = f.poll(t, "boundary")
	f.statusIs(t, p3, bus.StatusQueued)
	poll.resume <- nil
	f.frame(t, p3)
}

func TestAClaudeTurnStartingAfterTheClaimGivesBackHeldMembers(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	p1 := f.send(t, "alice", "p1", "answer")
	p3 := f.send(t, "bob", "p3", "progress")
	p0 := f.send(t, "carol", "p0", "stop")
	unranked := f.send(t, "alice", "", "unranked")
	poll.resume <- func() { f.status("waiting") }
	f.frame(t, p0, unranked)
	poll = f.poll(t, "midturn")
	f.statusIs(t, p1, bus.StatusQueued)
	f.statusIs(t, p3, bus.StatusQueued)
	f.status("idle")
	// Wake this midturn poll with a new P0. The next poll can release
	// the returned P1 and P3 at the boundary.
	next := f.send(t, "carol", "p0", "stop again")
	poll.resume <- nil
	f.frame(t, next)
	poll = f.poll(t, "boundary")
	poll.resume <- nil
	f.frame(t, p1, p3)
}

func TestCheckInboxTakesHeldMailEvenWhileClaudePushes(t *testing.T) {
	f := newClaudeInbox(t, "busy")
	p3 := f.send(t, "bob", "p3", "held update")
	var taken []bus.Message
	var conv uint64
	result, rpcErr := f.srv.callTool(context.Background(), json.RawMessage(`{"name":"check_inbox"}`), &taken, &conv)
	if rpcErr != nil || len(taken) != 1 || taken[0].ID != p3 {
		t.Fatalf("check_inbox took %+v, result %v, error %v", taken, result, rpcErr)
	}
}

func TestSendMessageReportsTheEffectivePriorityAndNote(t *testing.T) {
	f := newClaudeInbox(t, "busy")
	var taken []bus.Message
	var conv uint64
	result, rpcErr := f.srv.callTool(context.Background(), json.RawMessage(`{"name":"send_message","arguments":{"to":"claude/desk","content":"(question / not a steer) Which option?","priority":"p3"}}`), &taken, &conv)
	if rpcErr != nil {
		t.Fatal(rpcErr)
	}
	content := result.(map[string]any)["content"].([]map[string]string)[0]["text"]
	var sent struct{ Priority, Note string }
	if err := json.Unmarshal([]byte(content), &sent); err != nil || sent.Priority != "p1" || !strings.HasPrefix(sent.Note, "queued:") {
		t.Fatalf("send_message result = %s, error %v", content, err)
	}
}

func TestAnOversizedP3UsesAPointerAndKeepsItsTextInTheThread(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	body := strings.Repeat("x", 80<<10)
	id := f.send(t, "alice", "p3", body)
	f.now.Add(bus.DigestEvery.Milliseconds())
	poll.resume <- nil
	if frame := f.frame(t, id); !strings.Contains(frame, "(81920 bytes)") || strings.Contains(frame, body) {
		t.Fatalf("frame did not use a pointer for %s", id)
	}
	m, err := f.st.GetMessage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	thread, err := f.sender["alice"].Thread(context.Background(), m.ThreadID)
	if err != nil || len(thread) != 1 || thread[0].Content != body {
		t.Fatalf("did not get the full body back from the thread: %v", err)
	}
}

func TestATemporaryRegistryReadFailureDoesNotStartAnUngatedPoll(t *testing.T) {
	f := newClaudeInbox(t, "busy")
	poll := f.poll(t, "midturn")
	p3 := f.send(t, "bob", "p3", "held update")
	cutShort := make(chan struct{})
	poll.resume <- func() {
		if err := os.WriteFile(filepath.Join(claudeDir(), "sessions", "4242.json"), []byte("{"), 0o600); err != nil {
			t.Error(err)
		}
		close(cutShort)
	}
	select {
	case <-cutShort:
	case <-time.After(7 * time.Second):
		t.Fatal("busy poll did not finish")
	}
	select {
	case poll := <-f.polls:
		t.Fatalf("started a poll at=%q while the registry was unreadable", poll.at)
	case <-time.After(250 * time.Millisecond):
	}
	f.status("busy")
	f.poll(t, "midturn")
	f.statusIs(t, p3, bus.StatusQueued)
}
