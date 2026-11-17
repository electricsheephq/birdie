package bus_test

import (
	"context"
	"testing"
	"time"

	"birdie/internal/bus"
)

// threadWithS1 starts a thread between carol and recipient's session s1, in
// which s1 took carol's message or sent the first one, and then brings bot
// into it. It returns the thread.
func threadWithS1(t *testing.T, b *bus.Bus, recipient, how string) string {
	t.Helper()
	ctx := context.Background()
	var first bus.Message
	var err error
	if how == "took" {
		if first, err = b.Send(ctx, "carol", bus.SendInput{To: recipient + "/s1", Content: "hi"}); err != nil {
			t.Fatal(err)
		}
		for _, m := range take(t, b, recipient, "s1") {
			if err := b.Ack(ctx, recipient, m.ID); err != nil {
				t.Fatal(err)
			}
		}
	} else if first, err = b.Send(ctx, recipient, bus.SendInput{To: "carol", FromSession: "s1", Content: "hi"}); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send(ctx, "carol", bus.SendInput{To: "bot", ThreadID: first.ThreadID, Content: "fyi"}); err != nil {
		t.Fatal(err)
	}
	return first.ThreadID
}

// A follow-up from someone the recipient's session didn't talk with goes to
// the session the recipient used in the thread, not to no session (claude)
// or to its newest conversation (an agent that picks its own) (#29).
func TestAThirdPartysFollowUpGoesToTheSessionInTheThread(t *testing.T) {
	ctx := context.Background()
	for _, recipient := range []string{"claude", "eva"} {
		for _, how := range []string{"took", "sent"} {
			t.Run(recipient+"/"+how, func(t *testing.T) {
				b, st := newBus(t)
				for _, n := range []string{"bot", "carol", recipient} {
					addAgent(t, st, n, nil)
				}
				now := time.Now()
				report(t, b, recipient, "s1", "one", bus.TierPush, now.Add(-time.Minute))
				thread := threadWithS1(t, b, recipient, how)
				// A newer conversation: an agent that picks its own session
				// would pick this one.
				report(t, b, recipient, "s2", "two", bus.TierPush, now)
				f, err := b.Send(ctx, "bot", bus.SendInput{To: recipient, ThreadID: thread, Content: "follow-up"})
				if err != nil || f.ToSession != "s1" || f.Held != "" {
					t.Fatalf("follow-up: to_session %q held %q, err %v; want s1", f.ToSession, f.Held, err)
				}
				if got := take(t, b, recipient, "s2"); len(got) != 0 {
					t.Fatalf("the newer conversation took %+v", got)
				}
				if got := take(t, b, recipient, "s1"); len(got) != 1 || got[0].ID != f.ID {
					t.Fatalf("s1 took %+v", got)
				}
			})
		}
	}
}

// Messages between the two still come first. Without any, the recipient's
// most recent session in the thread wins.
func TestAFollowUpPrefersThePairThenTheLatestSession(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "claude", "dave"} {
		addAgent(t, st, n, nil)
	}
	now := time.Now()
	report(t, b, "claude", "s1", "one", bus.TierPush, now)
	report(t, b, "claude", "s2", "two", bus.TierPush, now)
	thread := threadWithS1(t, b, "claude", "took")
	if _, err := b.Send(ctx, "claude", bus.SendInput{To: "dave", FromSession: "s2", ThreadID: thread, Content: "and you?"}); err != nil {
		t.Fatal(err)
	}
	for from, want := range map[string]string{"carol": "s1", "dave": "s2", "bot": "s2"} {
		f, err := b.Send(ctx, from, bus.SendInput{To: "claude", ThreadID: thread, Content: "follow-up"})
		if err != nil || f.ToSession != want {
			t.Errorf("%s's follow-up: to_session %q, err %v; want %s", from, f.ToSession, err, want)
		}
	}
}

// A session another sender only addressed may never have seen the thread, so
// a third party's follow-up isn't sent to it.
func TestASessionThatWasOnlyAddressedDoesntGetAThirdPartysFollowUp(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "claude"} {
		addAgent(t, st, n, nil)
	}
	report(t, b, "claude", "s1", "one", bus.TierPush, time.Now())
	first, err := b.Send(ctx, "carol", bus.SendInput{To: "claude/s1", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send(ctx, "carol", bus.SendInput{To: "bot", ThreadID: first.ThreadID, Content: "fyi"}); err != nil {
		t.Fatal(err)
	}
	if f, err := b.Send(ctx, "carol", bus.SendInput{To: "claude", ThreadID: first.ThreadID, Content: "again"}); err != nil || f.ToSession != "s1" {
		t.Fatalf("carol's follow-up: to_session %q, err %v; want s1", f.ToSession, err)
	}
	if f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: first.ThreadID, Content: "follow-up"}); err != nil || f.ToSession != "" {
		t.Fatalf("bot's follow-up: to_session %q, err %v; want none", f.ToSession, err)
	}
}

// If that session has stopped, the follow-up waits for it like an answer, and
// its sender is told at once.
func TestAThirdPartysFollowUpForAStoppedSessionIsHeld(t *testing.T) {
	ctx := context.Background()
	for _, recipient := range []string{"claude", "eva"} {
		t.Run(recipient, func(t *testing.T) {
			b, st := newBus(t)
			for _, n := range []string{"bot", "carol", recipient} {
				addAgent(t, st, n, nil)
			}
			now := time.Now()
			report(t, b, recipient, "s1", "one", bus.TierPush, now.Add(-time.Minute))
			thread := threadWithS1(t, b, recipient, "took")
			report(t, b, recipient, "s2", "two", bus.TierPush, now)
			if err := b.EndSession(ctx, recipient, "s1"); err != nil {
				t.Fatal(err)
			}
			f, err := b.Send(ctx, "bot", bus.SendInput{To: recipient, ThreadID: thread, Content: "follow-up"})
			if err != nil || f.ToSession != "s1" || f.Held != bus.RecipientSessionEnded {
				t.Fatalf("follow-up: to_session %q held %q, err %v; want held for s1", f.ToSession, f.Held, err)
			}
			if got := take(t, b, recipient, "s2"); len(got) != 0 {
				t.Fatalf("the running conversation took %+v", got)
			}
		})
	}
}

// When the recipient last took part without a session, a follow-up waits for
// a reader that isn't a session, as before, rather than going to a session
// that took part earlier.
func TestAFollowUpAfterTakingPartWithoutASessionWaitsForAReaderWithoutOne(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "claude"} {
		addAgent(t, st, n, nil)
	}
	report(t, b, "claude", "s1", "one", bus.TierPush, time.Now())
	thread := threadWithS1(t, b, "claude", "took")
	if _, err := b.Send(ctx, "claude", bus.SendInput{To: "bot", ThreadID: thread, Content: "asked without a session"}); err != nil {
		t.Fatal(err)
	}
	f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: thread, Content: "follow-up"})
	if err != nil || f.ToSession != "" {
		t.Fatalf("follow-up: to_session %q, err %v; want none", f.ToSession, err)
	}
	if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Thread: thread}); err != nil || len(got) != 1 || got[0].ID != f.ID {
		t.Fatalf("a reader without a session: %+v, %v", got, err)
	}
}

// A message the recipient sent itself without a session, and took with one,
// counts for the session that took it.
func TestAMessageToItselfCountsForTheSessionThatTookIt(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "claude"} {
		addAgent(t, st, n, nil)
	}
	now := time.Now()
	report(t, b, "claude", "s1", "one", bus.TierPush, now)
	report(t, b, "claude", "s3", "three", bus.TierPush, now)
	thread := threadWithS1(t, b, "claude", "took")
	note, err := b.Send(ctx, "claude", bus.SendInput{To: "claude/s3", ThreadID: thread, Content: "over to you"})
	if err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "claude", "s3"); len(got) != 1 || got[0].ID != note.ID {
		t.Fatalf("s3 took %+v", got)
	}
	if err := b.Ack(ctx, "claude", note.ID); err != nil {
		t.Fatal(err)
	}
	if f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: thread, Content: "follow-up"}); err != nil || f.ToSession != "s3" {
		t.Fatalf("follow-up: to_session %q, err %v; want s3", f.ToSession, err)
	}
}

// A message a session gave back doesn't count as taken, even once it has
// expired: a nack and an expiry both keep taken_by.
func TestAMessageGivenBackDoesntCountAsTaken(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "eva"} {
		addAgent(t, st, n, nil)
	}
	now := time.Now()
	report(t, b, "eva", "s1", "one", bus.TierPush, now.Add(-time.Minute))
	report(t, b, "eva", "s2", "two", bus.TierPush, now)
	thread := threadWithS1(t, b, "eva", "took")
	later, err := b.Send(ctx, "carol", bus.SendInput{To: "eva/s2", ThreadID: thread, Content: "for s2"})
	if err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "eva", "s2"); len(got) != 1 || got[0].ID != later.ID {
		t.Fatalf("s2 took %+v", got)
	}
	if err := b.Nack(ctx, "eva", later.ID); err != nil {
		t.Fatal(err)
	}
	// Asked of the store, so no follow-up joins the thread before the expiry.
	if got, err := st.ThreadSession(ctx, thread, "bot", "eva"); err != nil || got != "s1" {
		t.Fatalf("after the nack: %q, %v; want s1", got, err)
	}
	if err := b.EndSession(ctx, "eva", "s2"); err != nil {
		t.Fatal(err)
	}
	q := bus.HoldQuery{Expire: true, Lapsed: true, LiveSince: now.Add(-bus.SessionTTL), Before: time.Now().Add(time.Hour), Now: time.Now(), Limit: 50}
	if held, _, err := st.SettleHeld(ctx, q, func([]bus.HeldMessage) []bus.Message { return nil }); err != nil || len(held) == 0 {
		t.Fatalf("expire: %+v, %v", held, err)
	}
	if m, err := st.GetMessage(ctx, later.ID); err != nil || m.Status != bus.StatusExpired {
		t.Fatalf("s2's message: %s, %v; want expired", m.Status, err)
	}
	if f, err := b.Send(ctx, "bot", bus.SendInput{To: "eva", ThreadID: thread, Content: "after the expiry"}); err != nil || f.ToSession != "s1" {
		t.Fatalf("after the expiry: to_session %q, err %v; want s1", f.ToSession, err)
	}
}
