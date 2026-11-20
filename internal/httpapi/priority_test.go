package httpapi_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
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
	m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob/desk", Content: "answer", Priority: "p1"})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/inbox?session=desk&push=1&at=boundary&lease=120", nil)
	req.Header.Set("Authorization", "Bearer bob-tok")
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
