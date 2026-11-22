package mcpserver_test

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/mcpserver"
	"birdie/internal/store"
)

// session drives a Server over pipes the way an MCP client would.
type session struct {
	t   *testing.T
	in  *io.PipeWriter
	out *bufio.Scanner
	id  int
}

func (s *session) send(v map[string]any) {
	b, _ := json.Marshal(v)
	s.in.Write(append(b, '\n'))
}

// next returns the next line written by the server.
func (s *session) next() map[string]any {
	s.t.Helper()
	line := make(chan string, 1)
	go func() {
		if s.out.Scan() {
			line <- s.out.Text()
		}
		close(line)
	}()
	select {
	case l, ok := <-line:
		if !ok {
			s.t.Fatal("server closed output")
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(l), &m); err != nil {
			s.t.Fatalf("bad JSON from server: %q", l)
		}
		return m
	// Longer than any tool call's timeout in these tests (15s), so a slow
	// but successful wait_for_reply isn't cut off here.
	case <-time.After(20 * time.Second):
		s.t.Fatal("timed out waiting for server output")
	}
	return nil
}

func (s *session) call(method string, params any) map[string]any {
	s.t.Helper()
	s.id++
	s.send(map[string]any{"jsonrpc": "2.0", "id": s.id, "method": method, "params": params})
	resp := s.next()
	if e, ok := resp["error"]; ok {
		s.t.Fatalf("%s: error %v", method, e)
	}
	return resp["result"].(map[string]any)
}

func toolJSON(t *testing.T, result map[string]any) map[string]any {
	t.Helper()
	if result["isError"] == true {
		t.Fatalf("tool error: %v", result["content"])
	}
	text := result["content"].([]any)[0].(map[string]any)["text"].(string)
	var out map[string]any
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		t.Fatalf("tool text not JSON: %q", text)
	}
	return out
}

func setup(t *testing.T, channel bool) (*session, *client.Client) {
	t.Helper()
	return setupWith(t, channel, nil)
}

// setupWith is setup with the bus's HTTP API wrapped by wrap, if not nil.
func setupWith(t *testing.T, channel bool, wrap func(http.Handler) http.Handler) (*session, *client.Client) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, n := range []string{"claude", "cursor"} {
		st.CreateAgent(context.Background(), bus.Agent{Name: n, TokenHash: bus.HashToken(n + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now(), Config: picksSession})
	}
	b := bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	var h http.Handler = httpapi.New(b, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if wrap != nil {
		h = wrap(h)
	}
	api := httptest.NewServer(h)
	t.Cleanup(api.Close)

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := mcpserver.New(client.New(api.URL, "claude-tok"), "claude", "test", channel, log.New(io.Discard, "", 0))
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx, inR, outW)
	t.Cleanup(func() { inW.Close(); cancel() })

	s := &session{t: t, in: inW, out: bufio.NewScanner(outR)}
	s.out.Buffer(make([]byte, 64<<10), 64<<20) // long messages
	init := s.call("initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test"}})
	if init["protocolVersion"] != "2025-06-18" {
		t.Fatalf("initialize = %v", init)
	}
	_, hasChannel := init["capabilities"].(map[string]any)["experimental"]
	if hasChannel != channel {
		t.Fatalf("channel capability = %v, want %v", hasChannel, channel)
	}
	s.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	return s, client.New(api.URL, "cursor-tok")
}

func TestTheSendToolShowsWhenARecipientDefaultedThePriority(t *testing.T) {
	s, _ := setupWith(t, false, func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == "POST" && r.URL.Path == "/v1/messages" {
				io.WriteString(w, `{"id":"m1","thread_id":"t1","status":"queued","priority":"p3","defaulted":true}`)
				return
			}
			next.ServeHTTP(w, r)
		})
	})
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "update"}}))
	if got["priority"] != "p3" || got["defaulted"] != true {
		t.Fatalf("send_message = %v", got)
	}
}

func TestToolsRoundTrip(t *testing.T) {
	s, cursor := setup(t, false)

	tools := s.call("tools/list", map[string]any{})["tools"].([]any)
	if len(tools) != 5 {
		t.Fatalf("got %d tools", len(tools))
	}

	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "run the tests", "wanted": "pass/fail"}}))
	thread := sent["thread_id"].(string)

	// The other agent answers over plain HTTP while Claude waits.
	go func() {
		msgs, err := cursor.Inbox(context.Background(), "", 5*time.Second)
		if err != nil || len(msgs) != 1 {
			return
		}
		cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "all green", InReplyTo: msgs[0].ID})
	}()
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 10}}))
	msgs := got["messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["content"] != "all green" {
		t.Fatalf("wait_for_reply = %v", got)
	}
}

func TestChannelPush(t *testing.T) {
	s, cursor := setup(t, true)

	m, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "build broke on main", Meta: map[string]string{"from": "forged", "branch": "main"}})
	if err != nil {
		t.Fatal(err)
	}
	n := s.next()
	if n["method"] != "notifications/claude/channel" {
		t.Fatalf("got %v", n)
	}
	p := n["params"].(map[string]any)
	meta := p["meta"].(map[string]any)
	content := p["content"].(string)
	if !strings.Contains(content, "build broke on main\n</birdie-message>\nFrom agent cursor") || !strings.Contains(content, m.ID) || meta["message_id"] != m.ID || meta["branch"] != "main" {
		t.Fatalf("notification = %v", p)
	}
	if meta["from"] != "cursor" {
		t.Fatalf("sender-supplied meta overrode authenticated from: %v", meta["from"])
	}
	if !strings.HasPrefix(meta["thread_id"].(string), "thr_") {
		t.Fatalf("thread_id = %v", meta["thread_id"])
	}
}

// fakeHost is an agent program that records what it is shown.
type fakeHost struct {
	tier   string
	pushed chan bus.Message
	fail   atomic.Bool
	// id, when set, replaces sess-1, as /clear does.
	id atomic.Pointer[string]
}

func (h *fakeHost) Session() mcpserver.SessionInfo {
	id := "sess-1"
	if p := h.id.Load(); p != nil {
		id = *p
	}
	return mcpserver.SessionInfo{ID: id, Name: "api", Cwd: "/work/api"}
}

func (h *fakeHost) setID(id string)         { h.id.Store(&id) }
func (h *fakeHost) Tier() string            { return h.tier }
func (h *fakeHost) Observe(json.RawMessage) {}
func (h *fakeHost) Push(_ context.Context, session string, m bus.Message) error {
	if h.fail.Load() {
		return errors.New("socket gone")
	}
	if session != h.Session().ID {
		return mcpserver.ErrOtherConversation
	}
	h.pushed <- m
	return nil
}

// picksSession has an agent pick one of its sessions for a message
// addressed to it alone, so tests can send to "claude" (see bus.PicksSession).
var picksSession = map[string]string{"fallback": "on"}

func setupHost(t *testing.T, h *fakeHost) (*session, *client.Client, *store.SQLite) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, n := range []string{"claude", "cursor"} {
		st.CreateAgent(context.Background(), bus.Agent{Name: n, TokenHash: bus.HashToken(n + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now(), Config: picksSession})
	}
	b := bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	api := httptest.NewServer(httpapi.New(b, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(api.Close)
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	srv := mcpserver.New(client.New(api.URL, "claude-tok"), "claude", "test", false, log.New(io.Discard, "", 0))
	srv.DetectHost = func(string) mcpserver.Host { return h }
	ctx, cancel := context.WithCancel(context.Background())
	go srv.Run(ctx, inR, outW)
	t.Cleanup(func() { inW.Close(); cancel() })
	s := &session{t: t, in: inW, out: bufio.NewScanner(outR)}
	s.out.Buffer(make([]byte, 64<<10), 64<<20) // long messages
	s.call("initialize", map[string]any{"protocolVersion": "2025-06-18", "capabilities": map[string]any{}, "clientInfo": map[string]any{"name": "test"}})
	s.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/initialized"})
	return s, client.New(api.URL, "cursor-tok"), st
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func sessionLive(cursor *client.Client) func() bool {
	return func() bool {
		agents, err := cursor.Agents(context.Background())
		if err != nil {
			return false
		}
		for _, a := range agents {
			if a.Name == "claude" && len(a.Sessions) == 1 {
				return true
			}
		}
		return false
	}
}

func TestPushHostGetsMessagesAndConfirmsThem(t *testing.T) {
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	m, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Note, "Claude Code · api gets this right away") {
		t.Fatalf("note = %q", m.Note)
	}
	select {
	case got := <-h.pushed:
		if got.ID != m.ID {
			t.Fatalf("pushed %s", got.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message never pushed")
	}
	waitFor(t, "delivery to be confirmed", func() bool {
		got, _ := st.GetMessage(context.Background(), m.ID)
		return got.Status == bus.StatusDelivered
	})

	// The session's answer records where it came from, so answers to it return here.
	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"in_reply_to": m.ID, "content": "hi back"}}))
	got, _ := st.GetMessage(context.Background(), sent["message_id"].(string))
	if got.FromSession != "sess-1" {
		t.Fatalf("from_session = %q", got.FromSession)
	}
	if orig, _ := st.GetMessage(context.Background(), m.ID); orig.Status != bus.StatusReplied {
		t.Fatalf("status = %q", orig.Status)
	}
}

func TestFailedPushStaysForLater(t *testing.T) {
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	h.fail.Store(true)
	_, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	m, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the message to return to the queue", func() bool {
		got, _ := st.GetMessage(context.Background(), m.ID)
		return got.Attempts == 0 && got.Status == bus.StatusQueued
	})
	h.fail.Store(false)
	select {
	case got := <-h.pushed:
		if got.ID != m.ID {
			t.Fatalf("pushed %s", got.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("message wasn't pushed once the host recovered")
	}
}

func TestPullHostGetsUnreadNotice(t *testing.T) {
	h := &fakeHost{tier: bus.TierPull, pushed: make(chan bus.Message, 1)}
	s, cursor, _ := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))
	if _, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "hello"}); err != nil {
		t.Fatal(err)
	}
	res := s.call("tools/call", map[string]any{"name": "list_agents", "arguments": map[string]any{}})
	text := res["content"].([]any)[0].(map[string]any)["text"].(string)
	if !strings.Contains(text, "1 unread birdie message") {
		t.Fatalf("no unread notice: %s", text)
	}
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "check_inbox", "arguments": map[string]any{}}))
	if msgs := got["messages"].([]any); len(msgs) != 1 {
		t.Fatalf("check_inbox = %v", got)
	}
	select {
	case m := <-h.pushed:
		t.Fatalf("a pull host was pushed %+v", m)
	default:
	}
}

// A sender that addresses claude alone hears, over the API, why it was
// refused and which sessions it can address instead; one it addresses by
// name and id gets it.
func TestSenderLearnsWhichSessionsToAddress(t *testing.T) {
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	_, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))
	if err := st.SetAgentConfig(context.Background(), "claude", "fallback", ""); err != nil {
		t.Fatal(err)
	}

	_, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "hello"})
	var e *client.APIError
	if !errors.As(err, &e) || e.Status != 400 || e.Code != bus.SessionRequired {
		t.Fatalf("err = %v", err)
	}
	if len(e.Sessions) != 1 || e.Sessions[0].Address != "claude/api#sess-1" || !strings.Contains(e.Message, "claude/api#sess-1") {
		t.Fatalf("sessions = %+v, message %q", e.Sessions, e.Message)
	}
	m, err := cursor.Send(context.Background(), bus.SendInput{To: e.Sessions[0].Address, Content: "hello"})
	if err != nil || m.ToSession != "sess-1" {
		t.Fatalf("m = %+v, %v", m, err)
	}
	select {
	case got := <-h.pushed:
		if got.ID != m.ID {
			t.Fatalf("pushed %s", got.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("message never pushed")
	}
}

func send(t *testing.T, s *session, args map[string]any) bus.Message {
	t.Helper()
	out := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": args}))
	return bus.Message{ID: out["message_id"].(string), ThreadID: out["thread_id"].(string), Note: out["note"].(string)}
}

func noPush(t *testing.T, h *fakeHost, within time.Duration) {
	t.Helper()
	select {
	case m := <-h.pushed:
		t.Fatalf("pushed %s (%q)", m.ID, m.Content)
	case <-time.After(within):
	}
}

// After /clear, answers to the old conversation must not be shown in the new
// one, including by a poll that was already out when it changed.
func TestClearKeepsTheOldConversationsAnswersOut(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	q1 := send(t, s, map[string]any{"to": "cursor", "content": "first question"})
	h.setID("sess-2")
	// The next call acts for the new conversation, without waiting for a report.
	q2 := send(t, s, map[string]any{"to": "cursor", "content": "second question"})
	if got, _ := st.GetMessage(ctx, q2.ID); got.FromSession != "sess-2" {
		t.Fatalf("from_session = %q, want sess-2", got.FromSession)
	}

	a1, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q1.ID, Content: "answer to the first"})
	if err != nil || a1.ToSession != "sess-1" || !strings.Contains(a1.Note, "no longer open") {
		t.Fatalf("a1 = %+v, %v", a1, err)
	}
	noPush(t, h, time.Second)
	a2, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q2.ID, Content: "answer to the second"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-h.pushed:
		if got.ID != a2.ID {
			t.Fatalf("pushed %s (%q), want the second answer", got.ID, got.Content)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the new conversation's answer wasn't pushed")
	}
	waitFor(t, "the old answer to be back in the queue", func() bool {
		got, _ := st.GetMessage(ctx, a1.ID)
		return got.Status == bus.StatusQueued && got.ToSession == "sess-1"
	})
}

// A push session can still collect an answer whose session has gone, by
// waiting on its thread.
func TestPushSessionCollectsAHeldAnswerFromItsThread(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	held := bus.Message{ID: "msg_held", ThreadID: "thr_held", From: "cursor", To: "claude", ToSession: "gone", Content: "held answer", Status: bus.StatusQueued, CreatedAt: time.Now()}
	if err := st.InsertMessage(ctx, held); err != nil {
		t.Fatal(err)
	}
	noPush(t, h, 500*time.Millisecond)

	out := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": "thr_held", "timeout_seconds": 15}}))
	msgs, _ := out["messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["id"] != "msg_held" {
		t.Fatalf("wait_for_reply = %+v", out)
	}
	waitFor(t, "delivery to be confirmed", func() bool {
		got, _ := st.GetMessage(ctx, "msg_held")
		return got.Status == bus.StatusDelivered
	})
}

// A bridge acts for a stand-in id until its host names the conversation;
// what was pinned to the stand-in follows the conversation to its real id.
func TestStandInSessionMovesToTheRealOne(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPull, pushed: make(chan bus.Message, 1)}
	h.setID(fmt.Sprintf("proc-%d", os.Getpid()))
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	q := send(t, s, map[string]any{"to": "cursor", "content": "question"})
	direct, err := cursor.Send(ctx, bus.SendInput{To: "claude/api", Content: "for you"})
	if err != nil {
		t.Fatal(err)
	}
	h.setID("real-1")
	s.call("tools/call", map[string]any{"name": "list_agents"})

	answer, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil || answer.ToSession != "real-1" {
		t.Fatalf("answer = %+v, %v", answer, err)
	}
	if got, _ := st.GetMessage(ctx, direct.ID); got.ToSession != "real-1" {
		t.Fatalf("direct message still pinned to %q", got.ToSession)
	}
	out := toolJSON(t, s.call("tools/call", map[string]any{"name": "check_inbox", "arguments": map[string]any{}}))
	if msgs, _ := out["messages"].([]any); len(msgs) != 2 {
		t.Fatalf("check_inbox = %+v", out)
	}
}

// A held answer that arrives while a push session waits on its thread is
// collected too, not only one already there.
func TestPushWaitCollectsAHeldAnswerThatArrivesLater(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	done := make(chan map[string]any, 1)
	go func() {
		done <- s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": "thr_late", "timeout_seconds": 15}})
	}()
	time.Sleep(300 * time.Millisecond)
	late := bus.Message{ID: "msg_late", ThreadID: "thr_late", From: "cursor", To: "claude", ToSession: "gone", Content: "late answer", Status: bus.StatusQueued, CreatedAt: time.Now()}
	if err := st.InsertMessage(ctx, late); err != nil {
		t.Fatal(err)
	}
	select {
	case res := <-done:
		out := toolJSON(t, res)
		if msgs, _ := out["messages"].([]any); len(msgs) != 1 || msgs[0].(map[string]any)["id"] != "msg_late" {
			t.Fatalf("wait_for_reply = %+v", out)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("wait_for_reply didn't return")
	}
	noPush(t, h, 200*time.Millisecond)
}

// /clear with no tool call or report since: the pump's poll still acts for
// the old conversation, so what it takes must not be shown in the new one.
func TestClearBeforeTheBridgeNoticesKeepsOldAnswersOut(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	q := send(t, s, map[string]any{"to": "cursor", "content": "question"})
	h.setID("sess-2")
	a, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q.ID, Content: "answer to the old conversation"})
	if err != nil || a.ToSession != "sess-1" {
		t.Fatalf("a = %+v, %v", a, err)
	}
	noPush(t, h, time.Second)
	waitFor(t, "the answer to wait for its session", func() bool {
		got, _ := st.GetMessage(ctx, a.ID)
		return got.Status == bus.StatusQueued
	})
}
