package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

type delivery struct{ session, message string }

// batchHost delivers to whatever conversation it serves when Push is called,
// without checking the one the message was taken for, so the test sees what
// the pump itself does. Its first Push holds until the test lets it go.
type batchHost struct {
	id             atomic.Value
	calls          atomic.Int32
	delivered      chan delivery
	firstDelivered chan struct{}
	continueBatch  chan struct{}
}

func (h *batchHost) Session() SessionInfo {
	return SessionInfo{ID: h.id.Load().(string), Name: "review"}
}
func (h *batchHost) Tier() string            { return bus.TierPush }
func (h *batchHost) Observe(json.RawMessage) {}
func (h *batchHost) Push(ctx context.Context, _ string, m bus.Message) error {
	h.delivered <- delivery{h.Session().ID, m.ID}
	if h.calls.Add(1) == 1 {
		close(h.firstDelivered)
		select {
		case <-h.continueBatch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

// A /clear while the first answer of a batch is being pushed must leave the
// rest of the batch queued for the old conversation, not push it to the new
// one. (Found in review by eva: the pump checked the conversation once per
// batch.)
func TestClearDuringAPushedBatchKeepsTheRestForTheOldConversation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, logger)
	for _, name := range []string{"claude", "peer"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ReportSession(ctx, "claude", "old", bus.SessionInput{Name: "review", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "peer", Content: "old question", FromSession: "old"})
	if err != nil {
		t.Fatal(err)
	}
	var answers []bus.Message
	for _, content := range []string{"answer one", "answer two", "answer three"} {
		m, err := b.Send(ctx, "peer", bus.SendInput{InReplyTo: q.ID, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		answers = append(answers, m)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("old")
	h := &batchHost{delivered: make(chan delivery, 4), firstDelivered: make(chan struct{}), continueBatch: make(chan struct{})}
	h.id.Store("old")
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case <-h.firstDelivered:
	case <-time.After(10 * time.Second):
		t.Fatal("first answer not pushed")
	}
	if first := <-h.delivered; first.session != "old" {
		t.Fatalf("first answer went to %s", first.session)
	}
	// /clear, and a tool call from the new conversation, while the first
	// push is still out.
	h.id.Store("new")
	srv.syncSession(ctx, h)
	close(h.continueBatch)

	deadline := time.Now().Add(2 * time.Second)
	for {
		select {
		case d := <-h.delivered:
			got, _ := st.GetMessage(ctx, d.message)
			t.Fatalf("after /clear, %s (for %q) was pushed to %q", d.message, got.ToSession, d.session)
		case <-time.After(50 * time.Millisecond):
		}
		queued := 0
		for _, a := range answers[1:] {
			got, err := st.GetMessage(ctx, a.ID)
			if err != nil {
				t.Fatal(err)
			}
			if got.Status == bus.StatusQueued && got.ToSession == "old" {
				queued++
			}
		}
		if queued == len(answers)-1 {
			return
		}
		if time.Now().After(deadline) {
			for _, a := range answers[1:] {
				got, _ := st.GetMessage(ctx, a.ID)
				t.Logf("%s: status %s, for %q", a.ID, got.Status, got.ToSession)
			}
			t.Fatal("the rest of the batch isn't back in the queue for the old conversation")
		}
	}
}

// In channel mode the notification goes to the MCP connection, which outlives
// /clear, so it must not be written once the conversation has changed, even
// if it waited for the output lock while /clear came.
func TestChannelShowOnlyReachesTheConversationItWasTakenFor(t *testing.T) {
	h := &batchHost{}
	h.id.Store("new")
	c := client.New("http://127.0.0.1:1", "claude-tok")
	c.SetSession("new")
	srv := New(c, "claude", "test", true, log.New(io.Discard, "", 0))
	srv.host = pullHost{h}
	var out bytes.Buffer
	srv.out = &out
	m := bus.Message{ID: "msg_1", ThreadID: "t", From: "peer", Content: "for the old one"}
	if err := srv.show(context.Background(), "old", m); !errors.Is(err, ErrOtherConversation) {
		t.Fatalf("show for the old conversation: %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("written: %s", out.String())
	}
	if err := srv.show(context.Background(), "new", m); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(out.Bytes(), []byte("notifications/claude/channel")) {
		t.Fatalf("not written: %s", out.String())
	}
}

// pullHost is a host that can't be pushed to, so messages use the channel.
type pullHost struct{ *batchHost }

func (pullHost) Tier() string { return bus.TierPull }

// refusingHost has /clear land between the pump's check and its first Push:
// the push finds the host serving another conversation and refuses.
type refusingHost struct {
	id     atomic.Value
	pushed chan string
}

func (h *refusingHost) Session() SessionInfo {
	return SessionInfo{ID: h.id.Load().(string), Name: "review"}
}
func (h *refusingHost) Tier() string            { return bus.TierPush }
func (h *refusingHost) Observe(json.RawMessage) {}
func (h *refusingHost) Push(_ context.Context, session string, m bus.Message) error {
	h.id.Store("new")
	if session != h.Session().ID {
		return ErrOtherConversation
	}
	h.pushed <- m.ID
	return nil
}

func TestARefusedPushGivesTheBatchBackAndFollowsTheNewConversation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, logger)
	for _, name := range []string{"claude", "peer"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ReportSession(ctx, "claude", "old", bus.SessionInput{Name: "review", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "peer", Content: "old question", FromSession: "old"})
	if err != nil {
		t.Fatal(err)
	}
	var answers []bus.Message
	for _, content := range []string{"answer one", "answer two"} {
		m, err := b.Send(ctx, "peer", bus.SendInput{InReplyTo: q.ID, Content: content})
		if err != nil {
			t.Fatal(err)
		}
		answers = append(answers, m)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("old")
	h := &refusingHost{pushed: make(chan string, 4)}
	h.id.Store("old")
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(10 * time.Second)
	for c.Session() != "new" {
		if time.Now().After(deadline) {
			t.Fatal("the bridge didn't follow the new conversation")
		}
		time.Sleep(20 * time.Millisecond)
	}
	select {
	case id := <-h.pushed:
		t.Fatalf("%s was pushed to the new conversation", id)
	case <-time.After(300 * time.Millisecond):
	}
	for _, a := range answers {
		got, err := st.GetMessage(ctx, a.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status != bus.StatusQueued || got.ToSession != "old" {
			t.Fatalf("%s: status %s, for %q; want queued for the old conversation", a.ID, got.Status, got.ToSession)
		}
	}
}

// unsureHost can't tell which conversation it serves, as Claude Code's host
// when its registry entry is read while being rewritten.
type unsureHost struct {
	*refusingHost
	asked chan struct{}
}

func (h unsureHost) conversation() (string, error) {
	select {
	case h.asked <- struct{}{}:
	default:
	}
	return "", errUnsure
}
func (h unsureHost) Push(_ context.Context, _ string, m bus.Message) error {
	h.pushed <- m.ID
	return nil
}

func TestABridgeThatCantTellItsConversationPushesNothing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, logger)
	for _, name := range []string{"claude", "peer"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ReportSession(ctx, "claude", "old", bus.SessionInput{Name: "review", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "peer", Content: "old question", FromSession: "old"})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := b.Send(ctx, "peer", bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("old")
	h := unsureHost{&refusingHost{pushed: make(chan string, 4)}, make(chan struct{}, 1)}
	h.id.Store("old")
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case <-h.asked:
	case <-time.After(10 * time.Second):
		t.Fatal("the pump didn't check the conversation")
	}
	// The pump took the answer, couldn't tell, and gives it back.
	deadline := time.Now().Add(5 * time.Second)
	for {
		select {
		case id := <-h.pushed:
			t.Fatalf("%s was pushed", id)
		case <-time.After(20 * time.Millisecond):
		}
		got, err := st.GetMessage(ctx, answer.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == bus.StatusQueued {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("status %s, want queued", got.Status)
		}
	}
	select {
	case id := <-h.pushed:
		t.Fatalf("%s was pushed", id)
	case <-time.After(300 * time.Millisecond):
	}
	if c.Session() != "old" {
		t.Fatalf("the bridge moved to %s", c.Session())
	}
}

// pullUnsureHost is a session without push that can't tell which
// conversation it serves.
type pullUnsureHost struct{ unsureHost }

func (pullUnsureHost) Tier() string { return bus.TierPull }

// check_inbox would take messages for the conversation last seen, which may
// not be the one asking, so it refuses while the host can't tell.
func TestCheckInboxWaitsWhileTheConversationIsUnknown(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, logger)
	for _, name := range []string{"claude", "peer"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.ReportSession(ctx, "claude", "old", bus.SessionInput{Name: "review", Tier: bus.TierPull}); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "peer", Content: "old question", FromSession: "old"})
	if err != nil {
		t.Fatal(err)
	}
	answer, err := b.Send(ctx, "peer", bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("old")
	h := pullUnsureHost{unsureHost{&refusingHost{pushed: make(chan string, 1)}, make(chan struct{}, 1)}}
	h.id.Store("old")
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h

	var taken []bus.Message
	res, rerr := srv.callTool(ctx, json.RawMessage(`{"name":"check_inbox","arguments":{}}`), &taken)
	if rerr != nil {
		t.Fatal(rerr.Message)
	}
	out, _ := json.Marshal(res)
	if !bytes.Contains(out, []byte(`"isError":true`)) || !bytes.Contains(out, []byte("can't tell which conversation")) {
		t.Fatalf("check_inbox = %s", out)
	}
	if got, err := st.GetMessage(ctx, answer.ID); err != nil || got.Status != bus.StatusQueued {
		t.Fatalf("answer: %+v, %v; want queued", got.Status, err)
	}
}
