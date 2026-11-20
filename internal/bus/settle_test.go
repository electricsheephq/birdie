package bus_test

import (
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestABoundaryWindowDoesNotCutAMidturnBatchShort(t *testing.T) {
	for _, tc := range []struct {
		name  string
		steps []time.Duration
	}{
		{"batch", []time.Duration{5500 * time.Millisecond}},
		{"burst", []time.Duration{500 * time.Millisecond, 2500 * time.Millisecond, 2500 * time.Millisecond}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newPriorityInbox(t)
			p.send(t, "alice", "p1", "answer")
			p.claim(t, bus.Boundary)
			*p.now = p.now.Add(2 * time.Second)
			ids := []string{p.send(t, "carol", "", "unranked").ID}
			p.claim(t, bus.Boundary)
			*p.now = p.now.Add(2 * time.Second)
			p.claim(t, bus.Midturn)
			for _, step := range tc.steps {
				*p.now = p.now.Add(step)
				ids = append(ids, p.send(t, "carol", "", "another unranked").ID)
				p.claim(t, bus.Midturn)
			}
			// The boundary deadline arrives just half a second after the newest row.
			*p.now = p.now.Add(500 * time.Millisecond)
			p.claim(t, bus.Midturn)
			// The whole batch can claim together at its own deadline.
			*p.now = p.now.Add(2 * time.Second)
			p.claim(t, bus.Midturn, ids...)
		})
	}
}

func TestABoundaryBatchSettlesAcrossAlternatingSparsePolls(t *testing.T) {
	p := newPriorityInbox(t)
	ids := []string{p.send(t, "alice", "p1", "answer").ID}
	p.claim(t, bus.Boundary)
	for second := 1; second <= 10; second++ {
		*p.now = p.now.Add(time.Second)
		ids = append(ids, p.send(t, "alice", "p1", "answer").ID)
		if second == 5 {
			p.claim(t, bus.Midturn)
		}
	}
	p.claim(t, bus.Boundary, ids...)
}

func TestAWindowLeftByALegacyClaimDoesNotCutFreshMailShort(t *testing.T) {
	p := newPriorityInbox(t)
	old := p.send(t, "alice", "p3", "old")
	p.claim(t, "", old.ID)
	*p.now = p.now.Add(115 * time.Second)
	first := p.send(t, "carol", "p0", "first stop")
	p.claim(t, bus.Midturn)
	*p.now = p.now.Add(time.Second)
	p.ack(t, p.claim(t, "", first.ID))

	// The older lease has expired, but that row was never in the first window.
	*p.now = p.now.Add(7 * time.Second)
	fresh := p.send(t, "dora", "p0", "another stop")
	p.claim(t, bus.Midturn)
	*p.now = p.now.Add(2 * time.Second)
	p.claim(t, bus.Midturn)
	*p.now = p.now.Add(time.Second)
	p.claim(t, bus.Midturn, fresh.ID, old.ID)
}
