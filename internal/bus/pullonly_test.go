package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

// A pull-only session gets nothing from a push delivery, still gets its
// messages when it checks, and senders are told it only sees them then.
func TestAPullOnlySessionGetsNothingPushed(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	addAgent(t, st, "peer", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	s, err := b.SetPullOnly(ctx, "claude/api", true)
	if err != nil {
		t.Fatal(err)
	}
	if !s.PullOnly || s.Tier != bus.TierPull || s.Address != "claude/api#s1" {
		t.Fatalf("session %+v", s)
	}
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now()) // its next heartbeat
	m, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/api", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Note, "only sees this when it checks") {
		t.Fatalf("note %q", m.Note)
	}
	pushed, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Push: true, Wait: 200 * time.Millisecond, Lease: time.Minute})
	if err != nil || len(pushed) != 0 {
		t.Fatalf("pushed into a pull-only session: %+v, %v", pushed, err)
	}
	checked, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Lease: time.Minute})
	if err != nil || len(checked) != 1 || checked[0].ID != m.ID {
		t.Fatalf("check_inbox got %+v, %v", checked, err)
	}
}

// Turned off from another process while a push delivery waits, the session
// gets what is waiting within a few seconds, with nothing restarted.
func TestTurningPullOnlyOffReachesAWaitingPush(t *testing.T) {
	defer func(d time.Duration) { bus.PullOnlyCheck = d }(bus.PullOnlyCheck)
	bus.PullOnlyCheck = 50 * time.Millisecond
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	addAgent(t, st, "peer", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	if _, err := b.SetPullOnly(ctx, "claude/s1", true); err != nil {
		t.Fatal(err)
	}
	m, err := b.Send(ctx, "peer", bus.SendInput{To: "claude/api", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	type result struct {
		msgs []bus.Message
		err  error
	}
	got := make(chan result, 1)
	go func() {
		msgs, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Push: true, Wait: 10 * time.Second, Lease: time.Minute})
		got <- result{msgs, err}
	}()
	time.Sleep(200 * time.Millisecond)
	if err := st.SetSessionPullOnly(ctx, "claude", "s1", false); err != nil { // as `birdie session pull-only` does
		t.Fatal(err)
	}
	select {
	case r := <-got:
		if r.err != nil || len(r.msgs) != 1 || r.msgs[0].ID != m.ID {
			t.Fatalf("got %+v, %v", r.msgs, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("still held after pull-only was turned off")
	}
}

// The switch takes one running session's address.
func TestPullOnlyNeedsARunningSession(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	if _, err := b.SetPullOnly(ctx, "claude", true); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("an agent alone: %v", err)
	}
	if _, err := b.SetPullOnly(ctx, "claude/other", true); !errors.Is(err, bus.ErrNotFound) {
		t.Fatalf("no such session: %v", err)
	}
	if _, err := b.SetPullOnly(ctx, "nobody/api", true); !errors.Is(err, bus.ErrNotFound) {
		t.Fatalf("no such agent: %v", err)
	}
}

// A stand-in id's switch moves to the real id of the same conversation.
func TestPullOnlyMovesFromAStandInToItsRealId(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "proc-1", "api", bus.TierPush, time.Now())
	if _, err := b.SetPullOnly(ctx, "claude/proc-1", true); err != nil {
		t.Fatal(err)
	}
	if err := b.ReportSession(ctx, "claude", "real", bus.SessionInput{Name: "api", Tier: bus.TierPush, Replaces: "proc-1"}); err != nil {
		t.Fatal(err)
	}
	if on, err := st.SessionPullOnly(ctx, "claude", "real"); err != nil || !on {
		t.Fatalf("real id pull-only %v, %v", on, err)
	}
}
