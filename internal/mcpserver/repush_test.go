package mcpserver

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

type recordingHost struct{ pushed chan string }

func (h *recordingHost) Session() SessionInfo    { return SessionInfo{ID: "s1", Name: "api"} }
func (h *recordingHost) Tier() string            { return bus.TierPush }
func (h *recordingHost) Observe(json.RawMessage) {}
func (h *recordingHost) Push(_ context.Context, _ string, m bus.Message) error {
	h.pushed <- m.ID
	return nil
}

// A message shown once, whose confirmation was lost, comes back when its
// lease runs out: it is confirmed, not shown again.
func TestAMessageShownOnceIsntShownAgain(t *testing.T) {
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
	if err := b.ReportSession(ctx, "claude", "s1", bus.SessionInput{Name: "api", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	again, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/s1", Content: "shown before"})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("s1")
	h := &recordingHost{pushed: make(chan string, 4)}
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	srv.remember(again, srv.convNow()) // shown, and the confirmation lost
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	defer func() { cancel(); <-done }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		got, err := st.GetMessage(ctx, again.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == bus.StatusDelivered {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("not confirmed: %s", got.Status)
		}
		time.Sleep(10 * time.Millisecond)
	}
	fresh, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/s1", Content: "new"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-h.pushed:
		if id != fresh.ID {
			t.Fatalf("pushed %s again", id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a new message wasn't pushed")
	}
}

func TestTheBridgeRemembersOnlyRecentPushes(t *testing.T) {
	srv := New(client.New("http://unused", "t"), "claude", "test", false, log.New(io.Discard, "", 0))
	for i := range maxPushed + 1 {
		srv.remember(bus.Message{ID: fmt.Sprintf("msg_%d", i), ThreadID: "t"}, 0)
	}
	if len(srv.pushed) != maxPushed || len(srv.pushedOrder) != maxPushed || srv.wasPushed("msg_0") || !srv.wasPushed("msg_1") {
		t.Fatalf("%d remembered; oldest kept: %v", len(srv.pushed), srv.wasPushed("msg_0"))
	}
}
