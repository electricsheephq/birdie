package bus_test

import (
	"context"
	"sort"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

// Mail queued for claude alone before sessions were required: no session
// takes it unless the user picks a default or turns fallback on. Its sender
// is told it is held, and it expires only after the hold time counted from
// then. An answer to what claude asked outside a session isn't such mail: it
// waits on its thread, as before.
func TestLegacyBareMailIsHeldThenExpiresOrReachesTheChosenSession(t *testing.T) {
	for _, choice := range []string{"none", "default", "fallback"} {
		t.Run(choice, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, "bot", nil)
			addAgent(t, st, "claude", nil)
			if err := st.SetAgentConfig(ctx, "claude", "fallback", "off"); err != nil {
				t.Fatal(err)
			}
			report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
			if choice == "default" {
				if err := st.SetAgentConfig(ctx, "claude", "default_session", "api"); err != nil {
					t.Fatal(err)
				}
			} else if choice == "fallback" {
				picksSession(t, st, "claude")
			}
			now := time.Now()
			// Inserted directly, because new bare sends already require a
			// session. Held sets held_at to the creation time.
			for _, m := range []bus.Message{
				// Told it is held longer ago than the hold time: expires.
				{ID: "told", CreatedAt: now.Add(-2 * time.Hour), Held: "held"},
				// Sent as long ago, but never told: held now, not expired,
				// so an upgrade leaves time to choose a default.
				{ID: "untold", CreatedAt: now.Add(-2 * time.Hour)},
				// Told recently: neither told again nor expired yet.
				{ID: "fresh", CreatedAt: now.Add(-10 * time.Minute), Held: "held"},
			} {
				m.ThreadID, m.From, m.FromSession, m.To = "legacy", "bot", "sender", "claude"
				m.Content, m.Status = "before the upgrade", bus.StatusQueued
				if err := st.InsertMessage(ctx, m); err != nil {
					t.Fatal(err)
				}
			}
			// Threads claude knows, long ago: it asked outside a session, or
			// read a message without being a session. What follows waits for
			// whoever waits on the thread, however old it is.
			for _, m := range []bus.Message{
				{ID: "asked", ThreadID: "asked", From: "claude", To: "bot", Status: bus.StatusDelivered, CreatedAt: now.Add(-3 * time.Hour)},
				{ID: "answer", ThreadID: "asked", InReplyTo: "asked", From: "bot", FromSession: "sender", To: "claude", Status: bus.StatusQueued, CreatedAt: now.Add(-2 * time.Hour)},
				{ID: "read", ThreadID: "read", From: "bot", FromSession: "sender", To: "claude", Status: bus.StatusDelivered, CreatedAt: now.Add(-3 * time.Hour)},
				{ID: "followup", ThreadID: "read", From: "bot", FromSession: "sender", To: "claude", Status: bus.StatusQueued, CreatedAt: now.Add(-2 * time.Hour)},
			} {
				m.Content = "a thread claude knows"
				if err := st.InsertMessage(ctx, m); err != nil {
					t.Fatal(err)
				}
			}
			b.HoldFor, b.QuietFor = time.Hour, -1
			// Its expiry notice proves the expiry pass ran before any session
			// takes the legacy mail, so delivery can't hide an expiry bug.
			if err := st.InsertMessage(ctx, bus.Message{
				ID: "sweep", ThreadID: "sweep", From: "bot", To: "claude", ToSession: "gone",
				Content: "for a stopped session", Status: bus.StatusQueued, CreatedAt: now.Add(-2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
			run, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				b.RunDelivery(run)
			}()
			t.Cleanup(func() { cancel(); <-done })
			if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: "sweep", Wait: time.Second}); err != nil || len(got) != 1 || got[0].Meta["notice"] != "expired" {
				t.Fatalf("sweep notice: %+v, %v", got, err)
			}
			if choice == "none" {
				if got := take(t, b, "claude", "s1"); len(got) != 0 {
					t.Fatalf("no default but a session took %+v", got)
				}
				seen := map[string]bool{}
				for len(seen) < 2 {
					got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Session: "sender", Thread: "legacy", Wait: time.Second})
					if err != nil || len(got) == 0 {
						t.Fatalf("legacy mail was stranded: notices %v, err %v", seen, err)
					}
					for _, n := range got {
						kind := n.Meta["notice"]
						wantID := map[string]string{"expired": "told", "held": "untold"}[kind]
						if wantID == "" || seen[kind] || n.From != bus.SystemSender || n.ToSession != "sender" || n.InReplyTo != wantID || n.Meta["messages"] != wantID || !strings.Contains(n.Content, bus.SessionRequired) || !strings.Contains(n.Content, "birdie session default") {
							t.Fatalf("legacy notice: %+v", n)
						}
						seen[kind] = true
					}
				}
				// The held notice for "untold" came from the same pass that
				// decided on every other message, so nothing more is coming.
				for _, thread := range []string{"legacy", "asked", "read"} {
					if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: thread}); err != nil || len(got) != 0 {
						t.Fatalf("extra notices on %s: %+v, %v", thread, got, err)
					}
				}
				for id, want := range map[string]string{"told": bus.StatusExpired, "untold": bus.StatusQueued, "fresh": bus.StatusQueued, "answer": bus.StatusQueued, "followup": bus.StatusQueued} {
					if m, err := st.GetMessage(ctx, id); err != nil || m.Status != want {
						t.Fatalf("%s status = %s, err %v; want %s", id, m.Status, err, want)
					}
				}
				for thread, want := range map[string]string{"asked": "answer", "read": "followup"} {
					got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Thread: thread})
					if err != nil || len(got) != 1 || got[0].ID != want {
						t.Fatalf("waiting on thread %s: %+v, %v", thread, got, err)
					}
				}
				return
			}
			got := take(t, b, "claude", "s1")
			var ids []string
			for _, m := range got {
				ids = append(ids, m.ID)
			}
			sort.Strings(ids)
			if strings.Join(ids, ",") != "answer,followup,fresh,told,untold" {
				t.Fatalf("chosen session took %v; want all five", ids)
			}
			for _, thread := range []string{"legacy", "asked", "read"} {
				if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: thread}); err != nil || len(got) != 0 {
					t.Fatalf("deliverable mail on %s got notices: %+v, %v", thread, got, err)
				}
			}
		})
	}
}

// SettleHeld checks the route again in its own transaction: a default chosen,
// or fallback turned on, after the sweep listed the agent as unrouted keeps
// its mail from being held or expired.
func TestHeldBareMailRechecksItsRouteWhenSettled(t *testing.T) {
	for _, choice := range []string{"none", "default", "fallback"} {
		t.Run(choice, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, "bot", nil)
			addAgent(t, st, "claude", nil)
			if err := st.SetAgentConfig(ctx, "claude", "fallback", "off"); err != nil {
				t.Fatal(err)
			}
			report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
			now := time.Now()
			if err := st.InsertMessage(ctx, bus.Message{ID: "told", ThreadID: "legacy", From: "bot", FromSession: "sender",
				To: "claude", Content: "before the upgrade", Status: bus.StatusQueued, CreatedAt: now.Add(-2 * time.Hour), Held: "held"}); err != nil {
				t.Fatal(err)
			}
			switch choice {
			case "default":
				if err := st.SetAgentConfig(ctx, "claude", "default_session", "api"); err != nil {
					t.Fatal(err)
				}
			case "fallback":
				picksSession(t, st, "claude")
			}
			// Unrouted is stale for "default" and "fallback": it was made
			// before the route was chosen.
			held, _, err := st.SettleHeld(ctx, bus.HoldQuery{Expire: true, Lapsed: true, Unrouted: []string{"claude"},
				LiveSince: now.Add(-bus.SessionTTL), Before: now.Add(-time.Hour), Now: now, Limit: 50},
				func([]bus.HeldMessage) []bus.Message { return nil })
			if err != nil {
				t.Fatal(err)
			}
			if want := map[string]int{"none": 1, "default": 0, "fallback": 0}[choice]; len(held) != want {
				t.Fatalf("expired %d messages, want %d", len(held), want)
			}
		})
	}
}

// bareFollowUp queues mail for no session in thread, as a bare follow-up was
// queued before the upgrade (#27). Send no longer queues one there: since #29
// a follow-up goes to the session the recipient used in the thread.
func bareFollowUp(t *testing.T, st interface {
	InsertMessage(context.Context, bus.Message) error
}, thread, from, to string) bus.Message {
	t.Helper()
	// A second old, so a sweep that starts at once already counts it as older
	// than now.
	m := bus.Message{ID: "msg_bare_" + from, ThreadID: thread, From: from, To: to, Content: "follow-up",
		Status: bus.StatusQueued, CreatedAt: time.Now().Add(-time.Second)}
	if err := st.InsertMessage(context.Background(), m); err != nil {
		t.Fatal(err)
	}
	return m
}

// Mail for no session in a thread a claude session took is held: its sender
// is told, and a session that waits on the thread still takes it (#30).
func TestAFollowUpInAThreadASessionTookIsHeldYetStillTaken(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "carol", nil)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	first, err := b.Send(ctx, "carol", bus.SendInput{To: "claude/s1", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range take(t, b, "claude", "s1") {
		if err := b.Ack(ctx, "claude", m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Send(ctx, "carol", bus.SendInput{To: "bot", ThreadID: first.ThreadID, Content: "fyi"}); err != nil {
		t.Fatal(err)
	}
	f := bareFollowUp(t, st, first.ThreadID, "bot", "claude")
	// No grace: the follow-up is new, and nothing waits on the thread.
	b.HoldFor, b.QuietFor, b.UnpinnedGrace = time.Hour, -1, -1
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.RunDelivery(run)
	}()
	t.Cleanup(func() { cancel(); <-done })
	deadline := time.Now().Add(3 * time.Second)
	var notice *bus.Message
	for notice == nil && time.Now().Before(deadline) {
		got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: first.ThreadID, Wait: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		for i := range got {
			if got[i].From == bus.SystemSender {
				notice = &got[i]
			}
		}
	}
	if notice == nil || notice.Meta["notice"] != "held" || notice.Meta["messages"] != f.ID || !strings.Contains(notice.Content, bus.SessionRequired) {
		t.Fatalf("held notice: %+v", notice)
	}
	if m, err := st.GetMessage(ctx, f.ID); err != nil || m.Status != bus.StatusQueued {
		t.Fatalf("follow-up status %s, err %v; want queued", m.Status, err)
	}
	got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Thread: first.ThreadID})
	if err != nil || len(got) != 1 || got[0].ID != f.ID {
		t.Fatalf("a session waiting on the thread: %+v, %v", got, err)
	}
}

// A session already waiting on the thread takes new mail for no session
// within seconds, so the sweep leaves such mail alone for a grace period
// rather than tell its sender it is held.
func TestNewMailForNoSessionIsntHeldDuringTheGrace(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "carol", nil)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	first, err := b.Send(ctx, "carol", bus.SendInput{To: "claude/s1", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range take(t, b, "claude", "s1") {
		if err := b.Ack(ctx, "claude", m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Send(ctx, "carol", bus.SendInput{To: "bot", ThreadID: first.ThreadID, Content: "fyi"}); err != nil {
		t.Fatal(err)
	}
	f := bareFollowUp(t, st, first.ThreadID, "bot", "claude")
	now := time.Now()
	q := bus.HoldQuery{Lapsed: true, Unrouted: []string{"claude"}, LiveSince: now.Add(-bus.SessionTTL), Now: now, Limit: 50}
	none := func([]bus.HeldMessage) []bus.Message { return nil }
	q.UnpinnedBefore = now.Add(-bus.DefaultUnpinnedGrace)
	if held, _, err := st.SettleHeld(ctx, q, none); err != nil || len(held) != 0 {
		t.Fatalf("within the grace: held %d, err %v; want none", len(held), err)
	}
	got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Thread: first.ThreadID, Lease: time.Minute})
	if err != nil || len(got) != 1 || got[0].ID != f.ID {
		t.Fatalf("the waiting session: %+v, %v", got, err)
	}
	// Once the grace has passed, mail nobody took is held: here the session
	// gives it back.
	if err := b.Nack(ctx, "claude", f.ID); err != nil {
		t.Fatal(err)
	}
	q.UnpinnedBefore = time.Now().Add(time.Second)
	if held, _, err := st.SettleHeld(ctx, q, none); err != nil || len(held) != 1 || held[0].ID != f.ID {
		t.Fatalf("after the grace: %+v, %v; want the follow-up held", held, err)
	}
}

// The same through the delivery loop with the default grace: a sweep that
// runs while the follow-up is new tells no one it is held.
func TestTheSweepGivesNewMailForNoSessionItsGrace(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "carol", nil)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	first, err := b.Send(ctx, "carol", bus.SendInput{To: "claude/s1", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range take(t, b, "claude", "s1") {
		if err := b.Ack(ctx, "claude", m.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Send(ctx, "carol", bus.SendInput{To: "bot", ThreadID: first.ThreadID, Content: "fyi"}); err != nil {
		t.Fatal(err)
	}
	bareFollowUp(t, st, first.ThreadID, "bot", "claude")
	// Mail for a stopped session: its held notice proves the sweep's notice
	// pass ran, and that pass also decided on the follow-up.
	if err := st.InsertMessage(ctx, bus.Message{ID: "sentinel", ThreadID: "sentinel", From: "bot", To: "claude", ToSession: "gone",
		Content: "for a stopped session", Status: bus.StatusQueued, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	b.HoldFor, b.QuietFor = time.Hour, -1
	run, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		b.RunDelivery(run)
	}()
	t.Cleanup(func() { cancel(); <-done })
	if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: "sentinel", Wait: 3 * time.Second}); err != nil || len(got) != 1 || got[0].Meta["notice"] != "held" {
		t.Fatalf("sentinel notice: %+v, %v", got, err)
	}
	got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: first.ThreadID})
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range got {
		if m.From == bus.SystemSender {
			t.Fatalf("held within the grace: %+v", m)
		}
	}
}
