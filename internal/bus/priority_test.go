package bus_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

type priorityInbox struct {
	*bus.Bus
	now *time.Time
}

func newPriorityInbox(t *testing.T) priorityInbox {
	t.Helper()
	b, st := newBus(t)
	for _, name := range []string{"alice", "bob", "carol", "dora"} {
		addAgent(t, st, name, nil)
	}
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	b.LoopLimit = -1
	return priorityInbox{b, &now}
}

func (p priorityInbox) report(t *testing.T, active time.Time) {
	t.Helper()
	if err := p.ReportSession(context.Background(), "bob", "desk", bus.SessionInput{Tier: bus.TierPush, ActiveAt: active}); err != nil {
		t.Fatal(err)
	}
}

func (p priorityInbox) send(t *testing.T, from, priority, body string) bus.Message {
	t.Helper()
	p.report(t, time.Time{})
	m, err := p.Send(context.Background(), from, bus.SendInput{To: "bob/desk", Content: body, Priority: priority})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (p priorityInbox) claim(t *testing.T, at string, want ...string) []bus.Message {
	t.Helper()
	got, err := p.ReceiveFor(context.Background(), bus.ReceiveInput{Agent: "bob", Session: "desk", Push: true, At: at, Lease: 2 * time.Minute})
	var ids []string
	for _, m := range got {
		ids = append(ids, m.ID)
	}
	if err != nil || !slices.Equal(ids, want) {
		t.Fatalf("%q claim ids = %v, %v, want %v", at, ids, err, want)
	}
	return got
}

func (p priorityInbox) ack(t *testing.T, msgs []bus.Message) {
	t.Helper()
	for _, m := range msgs {
		if err := p.Ack(context.Background(), "bob", m.ID); err != nil {
			t.Fatal(err)
		}
	}
}

func TestAStreamOfP3ReachesASessionAsOneClaimPerInterval(t *testing.T) {
	p := newPriorityInbox(t)
	for range 3 {
		var ids []string
		for _, agent := range []string{"alice", "carol", "dora"} {
			ids = append(ids, p.send(t, agent, "p3", "update").ID)
		}
		p.claim(t, "boundary")
		*p.now = p.now.Add(bus.DigestEvery)
		p.ack(t, p.claim(t, "boundary", ids...))
		p.claim(t, "boundary")
	}
}

func TestAP0PassesFiftyHeldMessagesAtEitherPosition(t *testing.T) {
	for _, at := range []string{"midturn", "boundary"} {
		t.Run(at, func(t *testing.T) {
			p := newPriorityInbox(t)
			var held []string
			for range 50 {
				held = append(held, p.send(t, "alice", "p3", "update").ID)
			}
			urgent := p.send(t, "carol", "p0", "stop")
			want := []string{urgent.ID}
			if at == "boundary" {
				want = append(want, held[:49]...)
			}
			p.ack(t, p.claim(t, at, want...))
			p.claim(t, at)
		})
	}
}

func TestAP1SentMidturnWaitsForTheBoundaryAndRidesWithP3(t *testing.T) {
	p := newPriorityInbox(t)
	p.report(t, time.Time{})
	p.claim(t, "midturn")
	fyi := p.send(t, "alice", "p3", "update")
	answer := p.send(t, "carol", "p1", "answer")
	if answer.Note != "held: sees this when its turn ends" || !strings.HasPrefix(fyi.Note, "held: goes in its next digest") {
		t.Fatalf("notes = %q, %q", answer.Note, fyi.Note)
	}
	p.claim(t, "midturn")
	p.claim(t, "boundary", answer.ID, fyi.ID)
}

func TestTheValveReleasesP1AndP3AtMostOnceInFortyMinutes(t *testing.T) {
	p := newPriorityInbox(t)
	first := p.send(t, "alice", "p1", "answer")
	second := p.send(t, "alice", "p3", "update")
	third := p.send(t, "alice", "p3", strings.Repeat("a", 48<<10))
	fourth := p.send(t, "alice", "p3", strings.Repeat("b", 48<<10))
	*p.now = p.now.Add(bus.Valve - time.Millisecond)
	p.claim(t, "midturn")
	*p.now = p.now.Add(time.Millisecond)
	p.ack(t, p.claim(t, "midturn", first.ID, second.ID, third.ID))
	*p.now = p.now.Add(bus.Valve - time.Millisecond)
	p.claim(t, "midturn")
	*p.now = p.now.Add(time.Millisecond)
	p.claim(t, "midturn", fourth.ID)
}

func TestTheCacheRuleReleasesFreshP3AfterFiftyMinutesIdle(t *testing.T) {
	p := newPriorityInbox(t)
	p.report(t, p.now.Add(-bus.CacheIdle+time.Millisecond))
	m, err := p.Send(context.Background(), "alice", bus.SendInput{To: "bob/desk", Content: "update", Priority: "p3"})
	if err != nil {
		t.Fatal(err)
	}
	p.claim(t, "boundary")
	*p.now = p.now.Add(time.Millisecond)
	p.claim(t, "boundary", m.ID)
}

func TestOverflowAndExpiredLeasesRespectTheDigestReservation(t *testing.T) {
	p := newPriorityInbox(t)
	first := p.send(t, "alice", "p3", strings.Repeat("é", 24<<10))
	second := p.send(t, "alice", "p3", strings.Repeat("b", 48<<10))
	*p.now = p.now.Add(bus.DigestEvery)
	p.claim(t, "boundary", first.ID)
	p.claim(t, "boundary")
	*p.now = p.now.Add(2 * time.Minute)
	p.claim(t, "midturn")
	p.claim(t, "boundary")
	*p.now = p.now.Add(bus.DigestEvery - 2*time.Minute)
	p.ack(t, p.claim(t, "boundary", first.ID))
	p.claim(t, "boundary")
	*p.now = p.now.Add(bus.DigestEvery)
	p.claim(t, "boundary", second.ID)
}

func TestLegacyClaimsAndUnrankedMailKeepTheirDelivery(t *testing.T) {
	p := newPriorityInbox(t)
	ranked := p.send(t, "alice", "p3", "held with position")
	unranked := p.send(t, "carol", "", "ready immediately")
	if unranked.Priority != "" || ranked.Priority != "p3" {
		t.Fatalf("effective priorities = %q, %q", unranked.Priority, ranked.Priority)
	}
	p.ack(t, p.claim(t, "midturn", unranked.ID))
	p.claim(t, "", ranked.ID)
	_, err := p.Send(context.Background(), "alice", bus.SendInput{To: "bob/desk", Content: "invalid", Priority: "p2"})
	if !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("invalid priority error = %v", err)
	}
}

func TestAFirstReplyAndClarificationLiftOnlyP3(t *testing.T) {
	p := newPriorityInbox(t)
	ctx := context.Background()
	ask := func() bus.Message {
		m, err := p.Send(ctx, "alice", bus.SendInput{To: "bob/desk", FromSession: "lane", Content: "request"})
		if err != nil {
			t.Fatal(err)
		}
		p.ack(t, p.claim(t, "", m.ID))
		return m
	}
	p.report(t, time.Time{})
	for _, tc := range []struct{ priority, want string }{{"p3", "p1"}, {"", ""}} {
		parent := ask()
		m, err := p.Send(ctx, "bob", bus.SendInput{InReplyTo: parent.ID, FromSession: "desk", Content: "answer", Priority: tc.priority})
		if err != nil || m.Priority != tc.want {
			t.Fatalf("first reply %q: effective priority = %q, %v", tc.priority, m.Priority, err)
		}
		m, err = p.Send(ctx, "bob", bus.SendInput{InReplyTo: parent.ID, FromSession: "desk", Content: "follow up", Priority: "p3"})
		if err != nil || m.Priority != "p3" {
			t.Fatalf("follow-up priority = %q, %v", m.Priority, err)
		}
	}
	m := p.send(t, "alice", "p3", bus.ClarifyMarker+" Which option?")
	if m.Priority != "p1" {
		t.Fatalf("clarification priority = %q", m.Priority)
	}
}

func TestADaemonRestartKeepsHeldMailAndTheUnconfirmedDigestClock(t *testing.T) {
	b, st := newBus(t)
	for _, name := range []string{"alice", "bob"} {
		addAgent(t, st, name, nil)
	}
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	p := priorityInbox{b, &now}
	first := p.send(t, "alice", "p3", strings.Repeat("x", 48<<10))
	second := p.send(t, "alice", "p3", strings.Repeat("y", 48<<10))
	now = now.Add(bus.DigestEvery)
	p.claim(t, "boundary", first.ID)
	p.Bus = bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	p.Now = b.Now
	p.claim(t, "midturn")
	p.claim(t, "boundary")
	p.ack(t, []bus.Message{first})
	now = now.Add(bus.DigestEvery)
	p.claim(t, "boundary", second.ID)
}

func TestReturningARetriedDigestDoesNotEraseItsEarlierReservation(t *testing.T) {
	p := newPriorityInbox(t)
	first := p.send(t, "alice", "p3", strings.Repeat("x", 48<<10))
	second := p.send(t, "carol", "p3", strings.Repeat("y", 48<<10))
	*p.now = p.now.Add(bus.DigestEvery)
	p.claim(t, "boundary", first.ID)

	*p.now = p.now.Add(2 * time.Minute)
	urgent := p.send(t, "dora", "p0", "stop")
	p.claim(t, "boundary", urgent.ID, first.ID)
	p.ack(t, []bus.Message{urgent})
	if err := p.Nack(context.Background(), "bob", first.ID); err != nil {
		t.Fatal(err)
	}
	p.claim(t, "boundary")
	*p.now = p.now.Add(bus.DigestEvery - 2*time.Minute)
	p.ack(t, p.claim(t, "boundary", first.ID))
	*p.now = p.now.Add(bus.DigestEvery)
	p.claim(t, "boundary", second.ID)
}
