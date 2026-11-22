package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
)

type digestInbox struct {
	api http.Handler
	b   *bus.Bus
	now *time.Time
}

func newDigestInbox(t *testing.T) digestInbox {
	t.Helper()
	api, b := priorityAPI(t)
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	d := digestInbox{api, b, &now}
	d.report(t, "bob", "desk", now)
	return d
}

func (d digestInbox) report(t *testing.T, agent, session string, active time.Time) {
	t.Helper()
	if err := d.b.ReportSession(context.Background(), agent, session, bus.SessionInput{Tier: bus.TierPush, ActiveAt: active}); err != nil {
		t.Fatal(err)
	}
}

func (d digestInbox) send(t *testing.T, from, to, priority, content string) bus.Message {
	t.Helper()
	m, err := d.b.Send(context.Background(), from, bus.SendInput{To: to, Priority: priority, Content: content})
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func (d digestInbox) claim(t *testing.T, agent, session, query string, want ...string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/inbox?push=1&session="+session+"&"+query, nil)
	req.Header.Set("Authorization", "Bearer "+agent+"-tok")
	rec := httptest.NewRecorder()
	d.api.ServeHTTP(rec, req)
	var data struct{ Messages []bus.Message }
	if err := json.Unmarshal(rec.Body.Bytes(), &data); rec.Code != http.StatusOK || err != nil {
		t.Fatalf("%s claim: HTTP %d, %v", query, rec.Code, err)
	}
	var got []string
	for _, m := range data.Messages {
		got = append(got, m.ID)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("%s claim IDs = %v, want %v", query, got, want)
	}
}

func TestATurnClaimUsesItsDigestIntervalAndDefaultsToTwentyMinutes(t *testing.T) {
	for _, tc := range []struct {
		name, query   string
		held, release time.Duration
	}{
		{"forty minutes", "&digest=2400", 20 * time.Minute, 40 * time.Minute},
		{"omitted", "", 20*time.Minute - time.Millisecond, 20 * time.Minute},
		{"zero", "&digest=0", 20*time.Minute - time.Millisecond, 20 * time.Minute},
		{"malformed", "&digest=abc", 20*time.Minute - time.Millisecond, 20 * time.Minute},
		{"negative", "&digest=-300", 20*time.Minute - time.Millisecond, 20 * time.Minute},
		{"fractional", "&digest=300.5", 20*time.Minute - time.Millisecond, 20 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newDigestInbox(t)
			active := *d.now
			m := d.send(t, "alice", "bob/desk", "p3", "update")
			for _, age := range []time.Duration{tc.held, tc.release - time.Millisecond} {
				*d.now = active.Add(age)
				d.report(t, "bob", "desk", active)
				d.claim(t, "bob", "desk", "at=turn"+tc.query)
			}
			*d.now = active.Add(tc.release)
			d.claim(t, "bob", "desk", "at=turn"+tc.query, m.ID)
		})
	}
}

func TestATurnClaimClampsDigestSecondsToTenThroughFiftyMinutes(t *testing.T) {
	for _, tc := range []struct {
		seconds string
		minutes time.Duration
	}{
		{"300", 10},
		{"7200", 50},
		{"18446744073709551616", 50},
	} {
		t.Run(tc.seconds, func(t *testing.T) {
			d := newDigestInbox(t)
			active := *d.now
			m := d.send(t, "alice", "bob/desk", "p3", "update")
			query := "at=turn&digest=" + tc.seconds
			*d.now = active.Add(tc.minutes*time.Minute - time.Millisecond)
			d.report(t, "bob", "desk", active)
			d.claim(t, "bob", "desk", query)
			*d.now = d.now.Add(time.Millisecond)
			d.claim(t, "bob", "desk", query, m.ID)
		})
	}
}

func TestADigestSpacesTurnClaimsEvenAfterAnotherSessionReserves(t *testing.T) {
	for _, tc := range []struct {
		name, query string
		held, due   time.Duration
	}{
		{"spacing", "at=turn&digest=2400", 20 * time.Minute, 40 * time.Minute},
		{"cross session prune", "at=turn&digest=2700", 41 * time.Minute, 45 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := newDigestInbox(t)
			first := d.send(t, "alice", "bob/desk", "p3", strings.Repeat("a", 48<<10))
			*d.now = d.now.Add(time.Millisecond)
			second := d.send(t, "alice", "bob/desk", "p3", strings.Repeat("b", 48<<10))
			*d.now = d.now.Add(tc.due)
			firstDigest := *d.now
			d.report(t, "bob", "desk", firstDigest)
			d.claim(t, "bob", "desk", tc.query, first.ID)
			if tc.held > bus.Valve {
				// A boundary reservation for another recipient triggers the global prune.
				*d.now = firstDigest.Add(tc.held - bus.Coalesce - time.Millisecond)
				d.report(t, "alice", "desk", *d.now)
				other := d.send(t, "bob", "alice/desk", "p3", "another recipient")
				*d.now = d.now.Add(time.Millisecond)
				urgent := d.send(t, "bob", "alice/desk", "p1", "answer")
				*d.now = d.now.Add(bus.Coalesce)
				d.claim(t, "alice", "desk", "at=boundary", urgent.ID, other.ID)
			}
			*d.now = firstDigest.Add(tc.held)
			d.report(t, "bob", "desk", firstDigest)
			d.claim(t, "bob", "desk", tc.query)
			*d.now = firstDigest.Add(tc.due - time.Millisecond)
			d.report(t, "bob", "desk", firstDigest)
			d.claim(t, "bob", "desk", tc.query)
			*d.now = d.now.Add(time.Millisecond)
			d.claim(t, "bob", "desk", tc.query, second.ID)
		})
	}
}

func TestBoundaryAndMidturnClaimsIgnoreDigestSeconds(t *testing.T) {
	for _, tc := range []struct {
		at       string
		priority string
		due      time.Duration
	}{
		{"boundary", "p3", 20 * time.Minute},
		{"midturn", "p1", 40 * time.Minute},
	} {
		t.Run(tc.at, func(t *testing.T) {
			d := newDigestInbox(t)
			active := *d.now
			m := d.send(t, "alice", "bob/desk", tc.priority, "update")
			*d.now = active.Add(tc.due - time.Millisecond)
			d.report(t, "bob", "desk", active)
			d.claim(t, "bob", "desk", "at="+tc.at+"&digest=300")
			*d.now = d.now.Add(time.Millisecond)
			d.claim(t, "bob", "desk", "at="+tc.at+"&digest=7200", m.ID)
		})
	}
}

func TestSendersSeeDigestTimingForEachTurnRecipient(t *testing.T) {
	const tentative20 = "held: goes in its next digest (one every 20 minutes) while it is active, otherwise with its next turn (at most 12 hours)"
	const tentative40 = "held: goes in its next digest (one every 40 minutes) while it is active, otherwise with its next turn (at most 12 hours)"
	for _, tc := range []struct {
		age         time.Duration
		desk, other string
	}{
		{10 * time.Minute, "held: goes in its next digest, within about 20 minutes", "held: goes in its next digest, within about 40 minutes"},
		{10*time.Minute + time.Millisecond, "held: goes in its next digest, within about 20 minutes", tentative40},
		{30 * time.Minute, "held: goes in its next digest, within about 20 minutes", tentative40},
		{30*time.Minute + time.Millisecond, tentative20, tentative40},
		{50*time.Minute + time.Millisecond, tentative20, tentative40},
	} {
		d := newDigestInbox(t)
		active := *d.now
		d.report(t, "bob", "other", active)
		d.claim(t, "bob", "desk", "at=turn")
		d.claim(t, "bob", "other", "at=turn&digest=2400")
		*d.now = active.Add(tc.age)
		for _, recipient := range []struct{ session, note string }{{"desk", tc.desk}, {"other", tc.other}} {
			d.report(t, "bob", recipient.session, active)
			if got := d.send(t, "alice", "bob/"+recipient.session, "p3", "update").Note; got != recipient.note {
				t.Errorf("%s at activity age %v: note = %q, want %q", recipient.session, tc.age, got, recipient.note)
			}
		}
	}
}

func TestAGoClientCanClaimWithItsTurnDigestInterval(t *testing.T) {
	d := newDigestInbox(t)
	srv := httptest.NewServer(d.api)
	defer srv.Close()
	c := client.New(srv.URL, "bob-tok")
	c.SetSession("desk")
	// Check the capability at runtime so the baseline test can exercise the old client.
	turn, ok := any(c).(interface {
		PushTurnInbox(context.Context, time.Duration, time.Duration) ([]bus.Message, error)
	})
	if !ok {
		t.Fatal("the Go client cannot specify a turn digest interval")
	}
	active := *d.now
	m := d.send(t, "alice", "bob/desk", "p3", "update")
	*d.now = active.Add(20 * time.Minute)
	d.report(t, "bob", "desk", active)
	if got, err := turn.PushTurnInbox(context.Background(), 0, 40*time.Minute); err != nil || len(got) != 0 {
		t.Fatalf("claim at 20 minutes = %+v, %v", got, err)
	}
	*d.now = active.Add(40 * time.Minute)
	d.report(t, "bob", "desk", active)
	if got, err := turn.PushTurnInbox(context.Background(), 0, 40*time.Minute); err != nil || len(got) != 1 || got[0].ID != m.ID {
		t.Fatalf("claim at 40 minutes = %+v, %v", got, err)
	}
}
