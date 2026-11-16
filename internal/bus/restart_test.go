package bus_test

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

// Just after a restart, before sessions report in, a message to a session's
// whole id reaches it. Once birdie has listened long enough, one that never
// came back is refused as before.
func TestAWholeIdIsntRefusedJustAfterARestart(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	stale := time.Now().Add(-10 * time.Minute) // the daemon was down
	for _, id := range []string{"s1", "closed"} {
		if err := st.UpsertSession(ctx, bus.Session{Agent: "claude", ID: id, Name: "api", Tier: bus.TierPush, ActiveAt: stale, LastSeen: stale}); err != nil {
			t.Fatal(err)
		}
	}
	if err := st.EndSession(ctx, "claude", "closed", stale); err != nil {
		t.Fatal(err)
	}
	b := bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	b.QuietFor = 300 * time.Millisecond
	for _, to := range []string{"claude/s1", "claude/api#s1"} {
		m, err := b.Send(ctx, "bot", bus.SendInput{To: to, Content: "hi"})
		if err != nil || m.ToSession != "s1" {
			t.Fatalf("%s while listening: %+v, %v", to, m, err)
		}
	}
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/closed", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("a session that said it closed: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/s1", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("after listening: %v", err)
	}
}

func isAddressError(err error, code string) bool {
	e, ok := bus.AsAddressError(err)
	return ok && e.Code == code
}
