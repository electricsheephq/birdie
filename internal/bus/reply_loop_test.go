package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestAnUnaddressedReplyLeavesTheParentDeliverable(t *testing.T) {
	for _, tc := range []struct {
		name, sender, replier, target, recipient string
	}{
		{"sender follow-up", "claude", "sender", "claude/recipient", "recipient"},
		{"third session reply", "claude", "third", "claude/recipient", "recipient"},
		{"sessionless reply to unpinned mail", "claude", "", "claude", ""},
		{"another session of a different recipient agent", "codex", "third", "claude/recipient", "recipient"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, "claude", nil)
			picksSession(t, st, "claude")
			if tc.sender != "claude" {
				addAgent(t, st, tc.sender, nil)
			}
			report(t, b, tc.sender, "sender", "sender", bus.TierPush, time.Now())
			for _, session := range []string{"recipient", "third"} {
				report(t, b, "claude", session, session, bus.TierPush, time.Now())
			}
			parent, err := b.Send(ctx, tc.sender, bus.SendInput{To: tc.target, FromSession: "sender", Content: "question"})
			if err != nil || parent.ToSession != tc.recipient {
				t.Fatalf("parent = %+v, %v", parent, err)
			}

			events, stop := b.Watch()
			defer stop()
			reply, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: parent.ID, FromSession: tc.replier, Content: "follow-up"})
			if err != nil || reply.ToSession != "sender" {
				t.Fatalf("follow-up = %+v, %v", reply, err)
			}
			if e := next(t, events); e.Type != bus.StatusQueued || e.ID != reply.ID {
				t.Fatalf("follow-up event = %+v", e)
			}
			select {
			case e := <-events:
				t.Errorf("unexpected event for the parent: %+v", e)
			default:
			}

			inbox := bus.ReceiveInput{Agent: "claude", Session: tc.recipient}
			got, err := b.ReceiveFor(ctx, inbox)
			if err != nil || len(got) != 1 || got[0].ID != parent.ID {
				t.Fatalf("recipient got %+v, %v, want parent %s once", got, err, parent.ID)
			}
			if again, err := b.ReceiveFor(ctx, inbox); err != nil || len(again) != 0 {
				t.Fatalf("recipient got more mail: %+v, %v", again, err)
			}
		})
	}
}

func TestAReplyWithoutTheClaimKeepsTheParentClaimed(t *testing.T) {
	for _, tc := range []struct {
		name, claimant, replier string
	}{
		{"another session holds the claim", "recipient", "third"},
		{"claim and reply both have no session", "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, "claude", nil)
			picksSession(t, st, "claude")
			report(t, b, "claude", "recipient", "recipient", bus.TierPush, time.Now())
			parent, err := b.Send(ctx, "claude", bus.SendInput{To: "claude", FromSession: "sender", Content: "question"})
			if err != nil {
				t.Fatal(err)
			}
			if got := take(t, b, "claude", tc.claimant); len(got) != 1 || got[0].ID != parent.ID || got[0].Status != bus.StatusClaimed {
				t.Fatalf("claimant got %+v, want claimed parent %s", got, parent.ID)
			}
			if _, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: parent.ID, FromSession: tc.replier, Content: "reply"}); err != nil {
				t.Fatal(err)
			}
			if got, err := st.GetMessage(ctx, parent.ID); err != nil || got.Status != bus.StatusClaimed {
				t.Fatalf("parent status = %q, %v, want claimed", got.Status, err)
			}
		})
	}
}

func TestARecipientReplyMarksTheParentAndOnlyEmitsOnce(t *testing.T) {
	for _, tc := range []struct {
		name, sender, recipient, to, session, status string
	}{
		{"claimed by the replying session", "claude", "claude", "claude", "recipient", bus.StatusClaimed},
		{"delivered", "claude", "claude", "claude/recipient", "recipient", bus.StatusDelivered},
		{"queued for the replying session", "claude", "claude", "claude/recipient", "recipient", bus.StatusQueued},
		{"queued for the replying session of a different agent", "codex", "claude", "claude/recipient", "recipient", bus.StatusQueued},
		{"queued for a session of a different agent replying without a session", "codex", "claude", "claude/recipient", "", bus.StatusQueued},
		{"expired for a session of the same agent", "claude", "claude", "claude/recipient", "recipient", bus.StatusExpired},
		{"unpinned and queued for a different agent, answered by another of its sessions", "claude", "codex", "codex", "lane2", bus.StatusQueued},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, tc.sender, nil)
			if tc.sender != tc.recipient {
				addAgent(t, st, tc.recipient, nil)
			} else {
				picksSession(t, st, tc.recipient)
			}
			report(t, b, tc.recipient, "recipient", "recipient", bus.TierPush, time.Now())
			parent, err := b.Send(ctx, tc.sender, bus.SendInput{To: tc.to, Content: "question"})
			if err != nil {
				t.Fatal(err)
			}
			if tc.status == bus.StatusClaimed || tc.status == bus.StatusDelivered {
				inbox := bus.ReceiveInput{Agent: tc.recipient, Session: tc.session}
				if tc.status == bus.StatusClaimed {
					inbox.Lease = time.Minute
				}
				if got, err := b.ReceiveFor(ctx, inbox); err != nil || len(got) != 1 || got[0].ID != parent.ID || got[0].Status != tc.status {
					t.Fatalf("recipient got %+v, %v, want %s parent %s", got, err, tc.status, parent.ID)
				}
			}
			if tc.status == bus.StatusExpired {
				if err := b.EndSession(ctx, tc.recipient, tc.session); err != nil {
					t.Fatal(err)
				}
				now := time.Now()
				q := bus.HoldQuery{Expire: true, Before: now.Add(time.Hour), Now: now, Limit: 1}
				if held, _, err := st.SettleHeld(ctx, q, func([]bus.HeldMessage) []bus.Message { return nil }); err != nil || len(held) != 1 || held[0].ID != parent.ID {
					t.Fatalf("expire parent: %+v, %v", held, err)
				}
				if got, err := st.GetMessage(ctx, parent.ID); err != nil || got.Status != bus.StatusExpired {
					t.Fatalf("parent status before replying = %q, %v, want expired", got.Status, err)
				}
				report(t, b, tc.recipient, tc.session, tc.session, bus.TierPush, now)
			}

			events, stop := b.Watch()
			defer stop()
			for i := range 2 {
				reply, err := b.Send(ctx, tc.recipient, bus.SendInput{InReplyTo: parent.ID, FromSession: tc.session, Content: "answer"})
				if err != nil {
					t.Fatal(err)
				}
				if e := next(t, events); e.Type != bus.StatusQueued || e.ID != reply.ID {
					t.Fatalf("answer event = %+v", e)
				}
				if got, err := st.GetMessage(ctx, parent.ID); err != nil || got.Status != bus.StatusReplied {
					t.Fatalf("parent status = %q, %v, want replied", got.Status, err)
				}
				if i == 0 {
					if e := next(t, events); e.Type != bus.StatusReplied || e.ID != parent.ID {
						t.Fatalf("replied event = %+v", e)
					}
				}
			}
			select {
			case e := <-events:
				t.Fatalf("second answer emitted another parent event: %+v", e)
			default:
			}
		})
	}
}

func TestEachSessionPairHasItsOwnLoopLimit(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	report(t, b, "claude", "orchestrator-id", "orchestrator", bus.TierPush, time.Now())
	report(t, b, "claude", "another", "another", bus.TierPush, time.Now())
	lanes := []string{"lane-one", "lane-two", "lane-three"}
	for _, lane := range lanes {
		report(t, b, "claude", lane, lane, bus.TierPush, time.Now())
	}
	send := func(lane, to string) (bus.Message, error) {
		return b.Send(ctx, "claude", bus.SendInput{To: to, FromSession: lane, Content: "progress"})
	}
	for _, lane := range lanes {
		for i := range 8 {
			if _, err := send(lane, "claude/orchestrator"); err != nil {
				t.Fatalf("%s send %d: %v", lane, i+1, err)
			}
		}
	}
	t.Run("a sessionless sender has its own limit", func(t *testing.T) {
		for i := range bus.DefaultLoopLimit {
			if _, err := send("", "claude/orchestrator"); err != nil {
				t.Fatalf("sessionless send %d: %v", i+1, err)
			}
		}
		for _, to := range []string{"claude/orchestrator", "claude"} {
			if _, err := send("", to); !errors.Is(err, bus.ErrLoop) || !strings.Contains(err.Error(), "claude has sent claude") {
				t.Errorf("sessionless send 13 to %q: %v, want ErrLoop naming the agent pair", to, err)
			}
		}
		if _, err := send("", "claude/another"); err != nil {
			t.Fatalf("sessionless send to a different recipient session: %v", err)
		}
	})
	t.Run("an unpinned send counts only the sending session", func(t *testing.T) {
		if _, err := send(lanes[1], "claude"); err != nil {
			t.Fatalf("unpinned send from %q: %v", lanes[1], err)
		}
	})
	for i := 8; i < bus.DefaultLoopLimit; i++ {
		if _, err := send(lanes[0], "claude/orchestrator-id"); err != nil {
			t.Fatalf("%s send %d: %v", lanes[0], i+1, err)
		}
	}
	if _, err := send(lanes[0], "claude/orchestrator"); !errors.Is(err, bus.ErrLoop) || !strings.Contains(err.Error(), "claude/lane-one has sent claude/orchestrator-id") {
		t.Fatalf("send 13: %v, want ErrLoop naming both sessions", err)
	}
	if _, err := send(lanes[0], "claude/another"); err != nil {
		t.Fatalf("same sender to a different session: %v", err)
	}
	if _, err := send(lanes[0], "claude"); !errors.Is(err, bus.ErrLoop) || !strings.Contains(err.Error(), "claude has sent claude") {
		t.Errorf("unpinned send from %q: %v, want ErrLoop naming the agent pair", lanes[0], err)
	}
}
