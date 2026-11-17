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
	b, st := newBus(t)
	for _, n := range []string{"bot", "carol", "eva"} {
		addAgent(t, st, n, nil)
	}
	now := time.Now()
	report(t, b, "eva", "s1", "one", bus.TierPush, now.Add(-time.Minute))
	thread := threadWithS1(t, b, "eva", "took")
	report(t, b, "eva", "s2", "two", bus.TierPush, now)
	if err := b.EndSession(ctx, "eva", "s1"); err != nil {
		t.Fatal(err)
	}
	f, err := b.Send(ctx, "bot", bus.SendInput{To: "eva", ThreadID: thread, Content: "follow-up"})
	if err != nil || f.ToSession != "s1" || f.Held != bus.RecipientSessionEnded {
		t.Fatalf("follow-up: to_session %q held %q, err %v; want held for s1", f.ToSession, f.Held, err)
	}
	if got := take(t, b, "eva", "s2"); len(got) != 0 {
		t.Fatalf("the running conversation took %+v", got)
	}
}
