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
			*p.now = p.now.Add(3 * time.Second)
			want := append([]string{urgent.ID}, held...)
			p.ack(t, p.claim(t, at, want...))
			p.claim(t, at)
		})
	}
}

func TestAP0HasDoubleTheLoopLimitAndCountsTowardBothLimits(t *testing.T) {
	p := newPriorityInbox(t)
	p.LoopLimit = 0
	p.report(t, time.Time{})
	ctx := context.Background()
	if err := p.ReportSession(ctx, "alice", "lane", bus.SessionInput{Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	send := func(priority string) error {
		_, err := p.Send(ctx, "alice", bus.SendInput{
			FromSession: "lane", To: "bob/desk", Priority: priority, Content: "update",
		})
		return err
	}
	for i := range bus.DefaultLoopLimit {
		if err := send("p3"); err != nil {
			t.Fatalf("send %d: %v", i+1, err)
		}
	}
	if err := send("p3"); !errors.Is(err, bus.ErrLoop) {
		t.Fatalf("13th send with p3: %v, want ErrLoop", err)
	}
	if err := send("p0"); err != nil {
		t.Fatalf("13th send with p0: %v", err)
	}
	if err := send("p3"); !errors.Is(err, bus.ErrLoop) || !strings.Contains(err.Error(), "13 messages") {
		t.Fatalf("p3 after p0: %v, want ErrLoop counting all 13 sends", err)
	}
	for i := 13; i < 2*bus.DefaultLoopLimit; i++ {
		if err := send("p0"); err != nil {
			t.Fatalf("send %d with p0: %v", i+1, err)
		}
	}
	if err := send("p0"); !errors.Is(err, bus.ErrLoop) || !strings.Contains(err.Error(), "p0 limit") {
		t.Fatalf("25th send with p0: %v, want ErrLoop naming the p0 limit", err)
	}
}

func TestAnOldHeldP3RidesWithFreshMailAfterItSettles(t *testing.T) {
	p := newPriorityInbox(t)
	held := p.send(t, "alice", "p3", "earlier update")
	*p.now = p.now.Add(time.Minute)
	p.claim(t, bus.Boundary)
	p0 := p.send(t, "alice", "p0", "stop")
	p.claim(t, bus.Boundary)
	*p.now = p.now.Add(time.Second)
	p3 := p.send(t, "carol", "p3", "details")
	*p.now = p.now.Add(2 * time.Second)
	p.claim(t, bus.Boundary)
	*p.now = p.now.Add(time.Second)
	p.claim(t, bus.Boundary, p0.ID, held.ID, p3.ID)
}

func TestAP0CarriesAtMostThreeHundredRowsAndLeavesTheRestQueued(t *testing.T) {
	p := newPriorityInbox(t)
	var held []string
	for range 400 {
		held = append(held, p.send(t, "alice", "p3", "update").ID)
	}
	*p.now = p.now.Add(time.Minute)
	p0 := p.send(t, "carol", "p0", "stop")
	*p.now = p.now.Add(bus.Coalesce)
	got, err := p.ReceiveFor(context.Background(), bus.ReceiveInput{
		Agent: "bob", Session: "desk", Push: true, At: bus.Midturn, Lease: 2 * time.Minute,
	})
	if err != nil || len(got) != 300 {
		t.Fatalf("p0 carry claimed %d rows, %v, want 300", len(got), err)
	}
	if got[0].ID != p0.ID {
		t.Fatalf("first row = %s, want p0 %s", got[0].ID, p0.ID)
	}
	for i, m := range got[1:] {
		if m.ID != held[i] {
			t.Fatalf("row %d = %s, want %s", i+1, m.ID, held[i])
		}
	}
	p.ack(t, got)
	p.claim(t, bus.Midturn)
	next := p.send(t, "carol", "p0", "stop again")
	*p.now = p.now.Add(bus.Coalesce)
	p.claim(t, bus.Midturn, append([]string{next.ID}, held[299:]...)...)
}

func TestALoneP0WakesAWaitingClaimAfterThreeSeconds(t *testing.T) {
	p := newPriorityInbox(t)
	p.Now = nil
	m := p.send(t, "alice", "p0", "stop")
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	got, err := p.ReceiveFor(ctx, bus.ReceiveInput{
		Agent: "bob", Session: "desk", Push: true, At: bus.Midturn, Wait: 5 * time.Second,
	})
	age := time.Since(m.CreatedAt)
	if err != nil || len(got) != 1 || got[0].ID != m.ID || age < 3*time.Second-time.Millisecond || age > 4*time.Second {
		t.Fatalf("lone p0 claimed after %v: %v, %v, want its message at about 3s", age, got, err)
	}
}

func TestASteadyStreamSettlesWithinTenSeconds(t *testing.T) {
	p := newPriorityInbox(t)
	var ids []string
	for second := range 15 {
		ids = append(ids, p.send(t, "alice", "p0", "stop").ID)
		switch {
		case second == 10:
			p.ack(t, p.claim(t, bus.Midturn, ids...))
			ids = nil
		default:
			p.claim(t, bus.Midturn)
		}
		*p.now = p.now.Add(time.Second)
	}
	*p.now = p.now.Add(2 * time.Second)
	p.claim(t, bus.Midturn, ids...)
}

func TestASteadyStreamSettlesOnTheNextSparsePoll(t *testing.T) {
	for _, tc := range []struct {
		name    string
		seconds int
	}{
		{"at the deadline", 10},
		{"after a missed poll", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPriorityInbox(t)
			ids := []string{p.send(t, "alice", "p0", "stop").ID}
			// claim does not set Wait, so these polls do not wait for a window.
			p.claim(t, bus.Midturn)
			for range tc.seconds {
				*p.now = p.now.Add(time.Second)
				ids = append(ids, p.send(t, "alice", "p0", "stop").ID)
			}
			p.ack(t, p.claim(t, bus.Midturn, ids...))

			fresh := p.send(t, "alice", "p0", "another stop")
			p.claim(t, bus.Midturn)
			*p.now = p.now.Add(bus.Coalesce)
			p.claim(t, bus.Midturn, fresh.ID)
		})
	}
}

func TestAMidturnP0CarriesHeldMailAndRestartsTheDigestClock(t *testing.T) {
	p := newPriorityInbox(t)
	p3 := p.send(t, "alice", "p3", strings.Repeat("a", 40<<10))
	overflow := p.send(t, "alice", "p3", strings.Repeat("b", 40<<10))
	p1 := p.send(t, "carol", "p1", "answer")
	*p.now = p.now.Add(19 * time.Minute)
	p.claim(t, bus.Midturn)
	p0 := p.send(t, "dora", "p0", "stop")
	*p.now = p.now.Add(3 * time.Second)
	p.ack(t, p.claim(t, bus.Midturn, p0.ID, p1.ID, p3.ID))

	*p.now = p.now.Add(20*time.Minute - time.Millisecond)
	p.claim(t, bus.Boundary)
	*p.now = p.now.Add(time.Millisecond)
	p.claim(t, bus.Boundary, overflow.ID)
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
	*p.now = p.now.Add(3 * time.Second)
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
	p.claim(t, "boundary")
	*p.now = p.now.Add(3*time.Second - time.Millisecond)
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

func TestAnOversizedP3ClaimGivesWatchersAPointerPreview(t *testing.T) {
	p := newPriorityInbox(t)
	body := strings.Repeat("é", 40<<10)
	m := p.send(t, "alice", "p3", body)
	*p.now = p.now.Add(bus.DigestEvery)
	events, stop := p.Watch()
	defer stop()

	msgs := p.claim(t, "boundary", m.ID)
	if msgs[0].ContentBytes != len(body) {
		t.Fatalf("claimed pointer size = %d, want %d", msgs[0].ContentBytes, len(body))
	}
	if e := next(t, events); e.Type != bus.StatusClaimed || e.ID != m.ID || e.Text != "(81920 bytes; read it in the thread)" {
		t.Fatalf("claimed pointer event = %+v", e)
	}
}

func TestLegacyClaimsAndUnrankedMailKeepTheirDelivery(t *testing.T) {
	p := newPriorityInbox(t)
	ranked := p.send(t, "alice", "p3", "held with position")
	unranked := p.send(t, "carol", "", "ready immediately")
	if unranked.Priority != "" || ranked.Priority != "p3" {
		t.Fatalf("effective priorities = %q, %q", unranked.Priority, ranked.Priority)
	}
	p.claim(t, "midturn")
	*p.now = p.now.Add(3 * time.Second)
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
	*p.now = p.now.Add(3 * time.Second)
	p.claim(t, "boundary", urgent.ID, first.ID)
	p.ack(t, []bus.Message{urgent})
	if err := p.Nack(context.Background(), "bob", first.ID); err != nil {
		t.Fatal(err)
	}
	p.claim(t, "boundary")
	*p.now = p.now.Add(bus.DigestEvery - 2*time.Minute - 3*time.Second)
	p.ack(t, p.claim(t, "boundary", first.ID))
	*p.now = p.now.Add(bus.DigestEvery)
	p.claim(t, "boundary", second.ID)
}
