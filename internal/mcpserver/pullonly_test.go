package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

type pullOnlyHost struct{ pushed chan string }

func (h *pullOnlyHost) Session() SessionInfo    { return SessionInfo{ID: "s1", Name: "api"} }
func (h *pullOnlyHost) Tier() string            { return bus.TierPush }
func (h *pullOnlyHost) Observe(json.RawMessage) {}
func (h *pullOnlyHost) Push(_ context.Context, _ string, m bus.Message) error {
	h.pushed <- m.ID
	return nil
}

// The bridge pushes nothing into a pull-only session; check_inbox still gets
// what was sent, and turning it off brings pushes back without a restart.
func TestTheBridgeDoesntPushIntoAPullOnlySession(t *testing.T) {
	defer func(d time.Duration) { bus.PullOnlyCheck = d }(bus.PullOnlyCheck)
	bus.PullOnlyCheck = 50 * time.Millisecond
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
	if _, err := b.SetPullOnly(ctx, "claude/api", true); err != nil {
		t.Fatal(err)
	}
	held, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/api", Content: "held"})
	if err != nil {
		t.Fatal(err)
	}
	api := httptest.NewServer(httpapi.New(b, logger))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("s1")
	h := &pullOnlyHost{pushed: make(chan string, 4)}
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	out := &bytes.Buffer{}
	srv.out = out
	srv.syncSession(ctx, h) // its report says it is pull-only
	if srv.pushing() {
		t.Fatal("the bridge didn't learn it is pull-only")
	}
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case id := <-h.pushed:
		t.Fatalf("pushed %s into a pull-only session", id)
	case <-time.After(time.Second):
	}
	srv.handle(ctx, request{ID: json.RawMessage(`1`), Method: "tools/call", Params: json.RawMessage(`{"name":"check_inbox","arguments":{}}`)})
	if !bytes.Contains(out.Bytes(), []byte(held.ID)) {
		t.Fatalf("check_inbox didn't get it: %s", out)
	}

	// Turned off from another process: the bridge hasn't reported since, so
	// it still thinks it is pull-only. Pushes resume within seconds anyway.
	if err := st.SetSessionPullOnly(ctx, "claude", "s1", false); err != nil {
		t.Fatal(err)
	}
	next, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/api", Content: "pushed"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case id := <-h.pushed:
		if id != next.ID {
			t.Fatalf("pushed %s, want %s", id, next.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("not pushed once pull-only was off")
	}
}

// A bridge that thinks it is pull-only gives back what the bus hands it and
// asks again. If that report fails, it doesn't take the message straight
// back in a loop.
func TestAFailingReportDoesntSpinThePump(t *testing.T) {
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
	if _, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/api", Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	inner := httpapi.New(b, logger)
	var polls atomic.Int32
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/v1/sessions/") {
			http.Error(w, `{"error":"down"}`, http.StatusInternalServerError)
			return
		}
		if r.URL.Path == "/v1/inbox" {
			polls.Add(1)
		}
		inner.ServeHTTP(w, r)
	}))
	defer api.Close()
	c := client.New(api.URL, "claude-tok")
	c.SetSession("s1")
	h := &pullOnlyHost{pushed: make(chan string, 4)}
	srv := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	srv.host = h
	srv.pullOnly.Store(true) // learned before the switch was turned off; reports now fail
	done := make(chan struct{})
	go func() { defer close(done); srv.pump(ctx) }()
	time.Sleep(time.Second)
	cancel()
	<-done
	if n := polls.Load(); n > 3 {
		t.Fatalf("%d inbox polls in a second", n)
	}
}
