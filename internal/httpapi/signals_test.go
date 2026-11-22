package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func senderSignalRequest(t *testing.T, api http.Handler, method, path, body string, want int, out any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer alice-tok")
	rec := httptest.NewRecorder()
	api.ServeHTTP(rec, req)
	if err := json.Unmarshal(rec.Body.Bytes(), out); rec.Code != want || err != nil {
		t.Fatalf("%s %s: HTTP %d, %v, %s", method, path, rec.Code, err, rec.Body)
	}
}

func bobSessions(t *testing.T, api http.Handler) map[string]map[string]any {
	t.Helper()
	var data struct {
		Agents []struct {
			Name     string
			Sessions []map[string]any
		}
	}
	senderSignalRequest(t, api, http.MethodGet, "/v1/agents", "", http.StatusOK, &data)
	sessions := map[string]map[string]any{}
	for _, a := range data.Agents {
		if a.Name == "bob" {
			for _, s := range a.Sessions {
				sessions[s["id"].(string)] = s
			}
			return sessions
		}
	}
	t.Fatal("bob missing from /v1/agents")
	return nil
}

func TestTheAgentListingMarksWarmAndColdAtTheCacheIdleEdge(t *testing.T) {
	api, b := priorityAPI(t)
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	edge := now.Add(-bus.CacheIdle)
	for id, active := range map[string]time.Time{"warm": edge, "cold": edge.Add(-time.Millisecond)} {
		if err := b.ReportSession(context.Background(), "bob", id, bus.SessionInput{ActiveAt: active, Tier: bus.TierPush}); err != nil {
			t.Fatal(err)
		}
	}
	sessions := bobSessions(t, api)
	for id, want := range map[string]bool{"warm": true, "cold": false} {
		if s := sessions[id]; s["warm"] != want || s["live"] != true || s["active_at"] == nil {
			t.Errorf("%s session = %v", id, s)
		}
	}
}

func heldSessionListing(t *testing.T) map[string]map[string]any {
	t.Helper()
	api, b := priorityAPI(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond).Add(-bus.DefaultHoldFor - time.Millisecond)
	b.Now = func() time.Time { return now }
	report := func(id string, queued int) {
		t.Helper()
		if err := b.ReportSession(ctx, "bob", id, bus.SessionInput{Tier: bus.TierPush}); err != nil {
			t.Fatal(err)
		}
		for range queued {
			if _, err := b.Send(ctx, "alice", bus.SendInput{To: "bob/" + id, Content: "update"}); err != nil {
				t.Fatal(err)
			}
		}
	}
	report("expired", 1)
	if err := b.EndSession(ctx, "bob", "expired"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(bus.DefaultHoldFor - 2*time.Hour + time.Millisecond)
	report("lapsed", 2)
	report("empty", 0)
	report("ended", 1)
	if err := b.EndSession(ctx, "bob", "ended"); err != nil {
		t.Fatal(err)
	}
	now = now.Add(2 * time.Hour)
	return bobSessions(t, api)
}

func TestTheAgentListingKeepsLapsedAndEndedSessionsWithHeldMail(t *testing.T) {
	sessions := heldSessionListing(t)
	for id, held := range map[string]float64{"lapsed": 2, "ended": 1} {
		if s := sessions[id]; s["live"] != false || s["held"] != held {
			t.Errorf("%s session = %v", id, s)
		}
	}
}

func TestTheAgentListingOmitsALapsedSessionWithoutHeldMail(t *testing.T) {
	sessions := heldSessionListing(t)
	if sessions["empty"] != nil || sessions["lapsed"] == nil {
		t.Errorf("want only lapsed sessions with held mail: %v", sessions)
	}
}

func TestTheAgentListingOmitsAnEndedSessionOutsideTheHold(t *testing.T) {
	sessions := heldSessionListing(t)
	if sessions["expired"] != nil || sessions["ended"] == nil {
		t.Errorf("want only ended sessions with mail within the hold: %v", sessions)
	}
}

func TestTheSendResultReportsTheCostOfDelivery(t *testing.T) {
	for _, tc := range []struct {
		name, priority, at, cost string
		idle                     time.Duration
	}{
		{"warm p0", "p0", bus.Turn, "warm", bus.CacheIdle},
		{"warm p1", "p1", bus.Turn, "warm", time.Minute},
		{"warm unranked", "", bus.Turn, "warm", time.Minute},
		{"cold p0", "p0", bus.Turn, "wakes_cold", bus.CacheIdle + time.Millisecond},
		{"cold p1", "p1", bus.Turn, "wakes_cold", time.Hour},
		{"cold unranked", "", bus.Turn, "wakes_cold", time.Hour},
		{"digest", "p3", bus.Turn, "held", time.Hour},
		{"turn boundary", "p1", bus.Midturn, "held", time.Minute},
		{"stopped", "p1", "", "waits", time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			api, b := priorityAPI(t)
			ctx := context.Background()
			now := time.Now().Truncate(time.Millisecond)
			b.Now = func() time.Time { return now }
			const session = "cost-desk"
			if err := b.ReportSession(ctx, "bob", session, bus.SessionInput{Tier: bus.TierPush, ActiveAt: now.Add(-tc.idle)}); err != nil {
				t.Fatal(err)
			}
			payload := map[string]string{"to": "bob/" + session, "content": "update", "priority": tc.priority}
			if tc.at != "" {
				if _, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bob", Session: session, Push: true, At: tc.at}); err != nil {
					t.Fatal(err)
				}
			} else {
				parent, err := b.Send(ctx, "bob", bus.SendInput{To: "alice", FromSession: session, Content: "request"})
				if err != nil {
					t.Fatal(err)
				}
				if err := b.EndSession(ctx, "bob", session); err != nil {
					t.Fatal(err)
				}
				delete(payload, "to")
				payload["in_reply_to"] = parent.ID
			}
			body, err := json.Marshal(payload)
			if err != nil {
				t.Fatal(err)
			}
			var result map[string]any
			senderSignalRequest(t, api, http.MethodPost, "/v1/messages", string(body), http.StatusCreated, &result)
			if result["cost"] != tc.cost {
				t.Errorf("cost = %v, want %q (note %q)", result["cost"], tc.cost, result["note"])
			}
		})
	}
}
