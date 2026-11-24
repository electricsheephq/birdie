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
	"reflect"
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
	return newClaudeInboxWithAck(t, initialStatus, nil)
}

func newClaudeInboxWithAck(t *testing.T, initialStatus string, beforeAck func(*http.Request)) *claudeInbox {
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
		if beforeAck != nil && strings.HasSuffix(r.URL.Path, "/ack") {
			beforeAck(r)
		}
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

// loadedMacWait bounds a wait for something that must happen. It only ends a failing test,
// so it allows for a runner or Mac under heavy load (stress-ng runs, 2026-11-24).
const loadedMacWait = 20 * time.Second

func (f *claudeInbox) poll(t *testing.T, at string) observedClaim {
	t.Helper()
	select {
	case p := <-f.polls:
		if p.at != at || (at == "boundary" && p.wait != "5") {
			t.Fatalf("poll at=%q wait=%q, want at=%q and wait=5 at boundaries", p.at, p.wait, at)
		}
		return p
	case <-time.After(loadedMacWait):
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
	case <-time.After(loadedMacWait):
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

func TestAClaudeFrameReturnsTheOverflowInClaimOrder(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	body := strings.Repeat("é", 45<<10)
	p3 := f.send(t, "alice", "p3", "short update")
	p1 := f.send(t, "alice", "p1", body)
	unranked := f.send(t, "bob", "", body)
	p0 := f.send(t, "carol", "p0", body)

	f.now.Add((3 * time.Second).Milliseconds())
	poll.resume <- nil
	if frame := f.frame(t, p0, p1); len(frame) > 256<<10 {
		t.Fatalf("first frame is %d bytes", len(frame))
	}
	poll = f.poll(t, "boundary")
	f.statusIs(t, p0, bus.StatusDelivered)
	f.statusIs(t, p1, bus.StatusDelivered)
	f.statusIs(t, unranked, bus.StatusQueued)
	f.statusIs(t, p3, bus.StatusQueued)

	poll.resume <- nil
	f.frame(t, unranked, p3)
	f.poll(t, "boundary")
	f.statusIs(t, unranked, bus.StatusDelivered)
	f.statusIs(t, p3, bus.StatusDelivered)
}

func TestABusyClaudeCarriesHeldMailWithP0(t *testing.T) {
	f := newClaudeInbox(t, "busy")
	poll := f.poll(t, "midturn")
	p1 := f.send(t, "alice", "p1", "answer")
	p3 := f.send(t, "bob", "p3", "progress")
	p0 := f.send(t, "carol", "p0", "stop")
	f.now.Add((3 * time.Second).Milliseconds())
	poll.resume <- nil
	f.frame(t, p0, p1, p3)
}

func TestAClaudeFrameReachesTheSocketBeforeSlowAcksCanOutlastItsLease(t *testing.T) {
	releaseAck := make(chan struct{}, 1)
	defer close(releaseAck)
	var slowPath atomic.Value
	f := newClaudeInboxWithAck(t, "busy", func(r *http.Request) {
		if r.URL.Path == slowPath.Load() {
			select {
			case <-releaseAck:
			case <-r.Context().Done():
			}
		}
	})
	poll := f.poll(t, "midturn")
	seen := f.send(t, "alice", "p0", "already shown")
	p0 := f.send(t, "bob", "p0", "new stop")
	f.srv.markShown(seen, f.srv.convNow())
	slowPath.Store("/v1/messages/" + seen + "/ack")
	claimTime := f.now.Add(bus.Coalesce.Milliseconds())
	leaseEnd := claimTime + client.Lease.Milliseconds()
	poll.resume <- func() { f.now.Store(leaseEnd - 1) }

	// The old ack cannot finish until after the claim's lease would expire.
	// The frame must reach the socket while that ack is still held.
	f.frame(t, p0)
	f.now.Store(leaseEnd)
	releaseAck <- struct{}{}
	f.poll(t, "midturn")
	f.statusIs(t, p0, bus.StatusDelivered)
	f.statusIs(t, seen, bus.StatusDelivered)
}

func TestAnIdleClaudePollTakesNewP1AndThenP3ComingDue(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	poll.resume <- nil
	p1 := f.send(t, "alice", "p1", "answer")
	f.now.Add((3 * time.Second).Milliseconds())
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
	unranked := f.send(t, "alice", "", "unranked")
	f.now.Add((3 * time.Second).Milliseconds())
	poll.resume <- func() { f.status("waiting") }
	f.frame(t, unranked)
	poll = f.poll(t, "midturn")
	f.statusIs(t, p1, bus.StatusQueued)
	f.statusIs(t, p3, bus.StatusQueued)
	f.status("idle")
	// A later P0 can carry the returned members.
	next := f.send(t, "carol", "p0", "stop again")
	f.now.Add((3 * time.Second).Milliseconds())
	poll.resume <- nil
	f.frame(t, next, p1, p3)
}

func TestAClaudeP0CarriesHeldMailEvenIfATurnStartsAfterTheClaim(t *testing.T) {
	f := newClaudeInbox(t, "idle")
	poll := f.poll(t, "boundary")
	p1 := f.send(t, "alice", "p1", "answer")
	p3 := f.send(t, "bob", "p3", "progress")
	p0 := f.send(t, "carol", "p0", "stop")
	f.now.Add((3 * time.Second).Milliseconds())
	poll.resume <- func() { f.status("waiting") }
	f.frame(t, p0, p1, p3)
	f.poll(t, "midturn")
	f.statusIs(t, p1, bus.StatusDelivered)
	f.statusIs(t, p3, bus.StatusDelivered)
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

func TestCheckInboxLeavesMailForCodexPush(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "codex"), []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	t.Setenv("PATH", dir)
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v1/sessions/desk":
			io.WriteString(w, "{}")
		case "/v1/inbox":
			t.Error("a pushing Codex session tried to claim mail with check_inbox")
			io.WriteString(w, `{"messages":[{"id":"msg_waiting"}]}`)
		default:
			t.Errorf("unexpected request: %s", r.URL.Path)
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(api.Close)
	s := New(client.New(api.URL, "test-tok"), "codex", "test", false, log.New(io.Discard, "", 0))
	s.host = &codexHost{thread: "desk"}
	if !s.pushing() {
		t.Fatal("Codex is not in push mode")
	}

	var taken []bus.Message
	var conv uint64
	result, rpcErr := s.callTool(context.Background(), json.RawMessage(`{"name":"check_inbox"}`), &taken, &conv)
	want := toolText("New messages come to you on their own in this session, so there is nothing to fetch.", false)
	if rpcErr != nil || len(taken) != 0 || !reflect.DeepEqual(result, want) {
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
	case <-time.After(loadedMacWait):
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
