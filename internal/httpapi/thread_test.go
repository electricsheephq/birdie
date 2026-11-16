package httpapi_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

// GET /v1/threads/{id} says when a thread is awaiting clarification, and only
// then.
func TestThreadStatusOverHTTP(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, n := range []string{"eva", "claude"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: n, TokenHash: bus.HashToken(n + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now(), Config: map[string]string{"fallback": "on"}}); err != nil {
			t.Fatal(err)
		}
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, quiet)
	api := httptest.NewServer(httpapi.New(b, quiet))
	t.Cleanup(api.Close)
	status := func(thread string) (string, bool) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, api.URL+"/v1/threads/"+thread, nil)
		req.Header.Set("Authorization", "Bearer eva-tok")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("GET thread = %d, %v", resp.StatusCode, err)
		}
		s, ok := out["status"].(string)
		return s, ok
	}

	ask, err := b.Send(ctx, "eva", bus.SendInput{To: "claude", Content: "tidy the repo"})
	if err != nil {
		t.Fatal(err)
	}
	if s, ok := status(ask.ThreadID); ok {
		t.Fatalf("a new thread has status %q", s)
	}
	q, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: ask.ID, Content: bus.ClarifyMarker + "\n1. Which repo?"})
	if err != nil {
		t.Fatal(err)
	}
	if s, _ := status(ask.ThreadID); s != bus.ThreadAwaitingClarification {
		t.Fatalf("status after the question = %q", s)
	}
	if _, err := b.Send(ctx, "eva", bus.SendInput{InReplyTo: q.ID, Content: "birdie"}); err != nil {
		t.Fatal(err)
	}
	if s, ok := status(ask.ThreadID); ok {
		t.Fatalf("status after the answer = %q", s)
	}
}
