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
	for _, id := range []string{"s1", "s2", "closed"} {
		if err := st.UpsertSession(ctx, bus.Session{Agent: "claude", ID: id, Name: "api", Tier: bus.TierPush, ActiveAt: stale, LastSeen: stale}); err != nil {
			t.Fatal(err)
		}
	}
	// gone was silent long before birdie stopped, while s1 still reported.
	gone := stale.Add(-48 * time.Hour)
	if err := st.UpsertSession(ctx, bus.Session{Agent: "claude", ID: "gone", Name: "old", Tier: bus.TierPush, ActiveAt: gone, LastSeen: gone}); err != nil {
		t.Fatal(err)
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
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/gone", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("a session gone before birdie stopped: %v", err)
	}
	// s1 reports again; that doesn't make the long-gone one look recent.
	if err := b.ReportSession(ctx, "claude", "s1", bus.SessionInput{Name: "api", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/gone", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("a session gone before birdie stopped, once others report: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/s2", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("after listening, one that never came back: %v", err)
	}
}

func isAddressError(err error, code string) bool {
	e, ok := bus.AsAddressError(err)
	return ok && e.Code == code
}

// A message a session took just before birdie restarted is neither lost nor
// given twice: confirmed afterwards, it stays delivered; never confirmed, it
// comes back once its lease ends, and only once.
func TestARestartLosesAndRepeatsNothing(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	b := bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := b.ReportSession(ctx, "claude", "s1", bus.SessionInput{Name: "api", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	var sent []bus.Message
	for _, content := range []string{"confirmed after", "never confirmed"} {
		m, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/s1", Content: content})
		if err != nil {
			t.Fatal(err)
		}
		sent = append(sent, m)
	}
	took, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Lease: 300 * time.Millisecond})
	if err != nil || len(took) != 2 {
		t.Fatalf("took %+v, %v", took, err)
	}
	st.Close() // birdie stops

	st, err = store.Open(path) // and starts again
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	b = bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := b.Ack(ctx, "claude", sent[0].ID); err != nil {
		t.Fatal(err)
	}
	if err := b.ReportSession(ctx, "claude", "s1", bus.SessionInput{Name: "api", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(400 * time.Millisecond) // the lease ends
	var again []string
	for range 2 {
		got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Lease: time.Minute})
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range got {
			again = append(again, m.ID)
		}
	}
	if len(again) != 1 || again[0] != sent[1].ID {
		t.Fatalf("after the restart: %v, want only %s", again, sent[1].ID)
	}
	if got, err := st.GetMessage(ctx, sent[0].ID); err != nil || got.Status != bus.StatusDelivered {
		t.Fatalf("confirmed one: %+v, %v", got.Status, err)
	}
}

// After the machine sleeps, a session that was running when it went to sleep
// gets its whole id until it reports; one gone long before still doesn't.
func TestAWholeIdIsntRefusedJustAfterAWake(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	b.QuietFor = 300 * time.Millisecond
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	now := time.Now()
	slept := now.Add(-10 * time.Minute)
	for id, seen := range map[string]time.Time{"open": slept, "gone": slept.Add(-48 * time.Hour)} {
		if err := st.UpsertSession(ctx, bus.Session{Agent: "claude", ID: id, Name: id, Tier: bus.TierPush, ActiveAt: seen, LastSeen: seen}); err != nil {
			t.Fatal(err)
		}
	}
	b.Woke(slept, now)
	if m, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/open", Content: "hi"}); err != nil || m.ToSession != "open" {
		t.Fatalf("open after a wake: %+v, %v", m, err)
	}
	if _, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/gone", Content: "hi"}); !isAddressError(err, bus.RecipientSessionEnded) {
		t.Fatalf("gone after a wake: %v", err)
	}
}
