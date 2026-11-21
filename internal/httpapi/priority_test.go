package httpapi_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

func priorityAPI(t *testing.T) (http.Handler, *bus.Bus) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	for _, name := range []string{"alice", "bob"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, quiet)
	if err := b.ReportSession(ctx, "bob", "desk", bus.SessionInput{Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	return httpapi.New(b, quiet), b
}

func TestAClientCanPassItsTurnPositionAndAnOldClientStillTakesEverything(t *testing.T) {
	api, _ := priorityAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	ctx := context.Background()
	alice, bob := client.New(srv.URL, "alice-tok"), client.New(srv.URL, "bob-tok")
	bob.SetSession("desk")
	m, err := alice.Send(ctx, bus.SendInput{To: "bob/desk", Content: "answer", Priority: "p1"})
	if err != nil || m.Priority != "p1" || m.Note == "" {
		t.Fatalf("send = %+v, %v", m, err)
	}
	if msgs, err := bob.PushInboxAt(ctx, 0, "midturn"); err != nil || len(msgs) != 0 {
		t.Fatalf("positioned inbox = %+v, %v", msgs, err)
	}
	msgs, err := bob.PushInbox(ctx, 0)
	if err != nil || len(msgs) != 1 || msgs[0].ID != m.ID || msgs[0].Priority != "p1" {
		t.Fatalf("legacy inbox = %+v, %v", msgs, err)
	}
}

func TestAClientCanClaimAtTurnAndAnUnknownPositionIsStillRejected(t *testing.T) {
	api, _ := priorityAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	ctx := context.Background()
	bob := client.New(srv.URL, "bob-tok")
	bob.SetSession("desk")
	var invalid *client.APIError
	if _, err := bob.PushInboxAt(ctx, 0, "unknown"); !errors.As(err, &invalid) || invalid.Status != http.StatusBadRequest || !strings.Contains(invalid.Message, "at must be") {
		t.Fatalf("unknown position: %v", err)
	}
	if msgs, err := bob.PushInboxAt(ctx, 0, "turn"); err != nil || len(msgs) != 0 {
		t.Fatalf("turn inbox = %+v, %v", msgs, err)
	}
}

func TestTheSessionsAPIOnlyCountsQueuedMailForTheAddressedSession(t *testing.T) {
	api, b := priorityAPI(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	if err := b.ReportSession(ctx, "bob", "desk", bus.SessionInput{Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	for _, session := range [][2]string{{"alice", "desk"}, {"bob", "other"}} {
		if err := b.ReportSession(ctx, session[0], session[1], bus.SessionInput{Tier: bus.TierPull}); err != nil {
			t.Fatal(err)
		}
	}
	send := func(from, to, priority string) bus.Message {
		t.Helper()
		m, err := b.Send(ctx, from, bus.SendInput{To: to, Content: "update", Priority: priority})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	readHeld := func() map[string]float64 {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, "/v1/agents", nil)
		req.Header.Set("Authorization", "Bearer alice-tok")
		rec := httptest.NewRecorder()
		api.ServeHTTP(rec, req)
		var data struct {
			Agents []struct {
				Name     string
				Sessions []map[string]any
			}
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &data); rec.Code != http.StatusOK || err != nil {
			t.Fatalf("agents: HTTP %d, %v", rec.Code, err)
		}
		counts := map[string]float64{}
		for _, agent := range data.Agents {
			for _, session := range agent.Sessions {
				if held, ok := session["held"]; ok {
					counts[agent.Name+"/"+session["id"].(string)] = held.(float64)
				}
			}
		}
		return counts
	}
	checkHeld := func(want map[string]float64) {
		t.Helper()
		if got := readHeld(); !maps.Equal(got, want) {
			t.Errorf("held counts = %v, want %v", got, want)
		}
	}
	delivered := send("alice", "bob/desk", "")
	claim := func() {
		t.Helper()
		if msgs, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bob", Session: "desk", Lease: time.Minute}); err != nil || len(msgs) != 1 {
			t.Fatalf("claim = %+v, %v", msgs, err)
		}
	}
	claim()
	if err := b.Ack(ctx, "bob", delivered.ID); err != nil {
		t.Fatal(err)
	}
	send("alice", "bob/desk", "")
	claim() // still claimed, with no confirmation yet
	if m := send("alice", "bob", "p3"); m.ToSession != "" {
		t.Fatalf("agent-only mail was pinned to %q", m.ToSession)
	}
	checkHeld(map[string]float64{"bob/desk": 1})
	send("alice", "bob/desk", "p3")
	checkHeld(map[string]float64{"bob/desk": 2})
	send("bob", "alice/desk", "")
	checkHeld(map[string]float64{"bob/desk": 2, "alice/desk": 1})
	now = now.Add(time.Minute)
	checkHeld(map[string]float64{"bob/desk": 3, "alice/desk": 1})
}

type failedInboxWriter struct {
	*httptest.ResponseRecorder
	beforeFailure func()
}

func (w failedInboxWriter) Write([]byte) (int, error) {
	if w.beforeFailure != nil {
		w.beforeFailure()
	}
	return 0, io.ErrClosedPipe
}

func TestAnUnwrittenInboxResponseGivesItsClaimBack(t *testing.T) {
	api, b := priorityAPI(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob/desk", Content: "answer", Priority: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/inbox?session=desk&push=1&at=boundary&lease=120", nil)
	req.Header.Set("Authorization", "Bearer bob-tok")
	now = now.Add(3 * time.Second)
	api.ServeHTTP(failedInboxWriter{ResponseRecorder: httptest.NewRecorder()}, req)
	msgs, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bob", Session: "desk", Push: true, At: "boundary"})
	if err != nil || len(msgs) != 1 || msgs[0].ID != m.ID {
		t.Fatalf("claim after failed write = %+v, %v", msgs, err)
	}
}

func TestAnInvalidTurnPositionIsRefused(t *testing.T) {
	api, _ := priorityAPI(t)
	srv := httptest.NewServer(api)
	defer srv.Close()
	bob := client.New(srv.URL, "bob-tok")
	bob.SetSession("desk")
	_, err := bob.PushInboxAt(context.Background(), 0, "other")
	var apiErr *client.APIError
	if !errors.As(err, &apiErr) || apiErr.Status != http.StatusBadRequest {
		t.Fatalf("invalid position error = %v", err)
	}
}

func TestALateHTTPFailureLeavesTheNewerClaimAndItsReservationInPlace(t *testing.T) {
	api, b := priorityAPI(t)
	ctx := context.Background()
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob/desk", Content: "older update", Priority: "p3"})
	if err != nil {
		t.Fatal(err)
	}
	now = now.Add(bus.DigestEvery)
	in := bus.ReceiveInput{Agent: "bob", Session: "desk", Push: true, At: "boundary", Lease: client.Lease}
	req := httptest.NewRequest(http.MethodGet, "/v1/inbox?session=desk&push=1&lease=120", nil)
	req.Header.Set("Authorization", "Bearer bob-tok")
	api.ServeHTTP(failedInboxWriter{httptest.NewRecorder(), func() {
		now = now.Add(client.Lease)
		msgs, err := b.ReceiveFor(ctx, in)
		if err != nil || len(msgs) != 1 || msgs[0].ID != m.ID {
			t.Fatalf("new claim before late write failure = %+v, %v", msgs, err)
		}
	}}, req)
	if msgs, err := b.ReceiveFor(ctx, in); err != nil || len(msgs) != 0 {
		t.Fatalf("late response failure cancelled a newer claim: %+v, %v", msgs, err)
	}
}
