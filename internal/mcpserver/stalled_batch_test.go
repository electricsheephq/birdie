package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
)

// A batch stops calling the daemon once a call can't reach it: the rest go
// back to the queue when their leases end. A refusal isn't a stall, so the
// batch carries on after one (#26).
func TestABatchStopsCallingADaemonThatCantBeReached(t *testing.T) {
	for _, tc := range []struct {
		name  string
		fail  func(w http.ResponseWriter)
		calls int32
	}{
		{"connection drops", func(w http.ResponseWriter) {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				conn.Close()
			}
		}, 1},
		{"refused", func(w http.ResponseWriter) { http.Error(w, `{"error":"not yours"}`, http.StatusConflict) }, 4},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if strings.HasSuffix(r.URL.Path, "/ack") || strings.HasSuffix(r.URL.Path, "/nack") {
					calls.Add(1)
					tc.fail(w)
					return
				}
				io.WriteString(w, `{}`)
			}))
			defer api.Close()
			s := New(client.New(api.URL, "test-token"), "claude", "test", false, log.New(io.Discard, "", 0))
			b := s.newBatch(context.Background())
			b.ack(bus.Message{ID: "m1"})
			b.nack(bus.Message{ID: "m2"})
			b.ack(bus.Message{ID: "m3"})
			b.nack(bus.Message{ID: "m4"})
			if got := calls.Load(); got != tc.calls {
				t.Fatalf("calls to the daemon: %d, want %d", got, tc.calls)
			}
		})
	}
}

// A stalled daemon holds a batch for one call's deadline, not one per
// message.
func TestAStalledDaemonHoldsABatchForOneDeadline(t *testing.T) {
	t.Parallel() // waits out the real ten-second deadline
	var calls atomic.Int32
	release := make(chan struct{})
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		calls.Add(1)
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	defer api.Close()
	defer close(release)
	s := New(client.New(api.URL, "test-token"), "claude", "test", false, log.New(io.Discard, "", 0))
	start := time.Now()
	b := s.newBatch(context.Background())
	for _, id := range []string{"m1", "m2", "m3"} {
		b.ack(bus.Message{ID: id})
	}
	if took := time.Since(start); took > 15*time.Second || calls.Load() != 1 {
		t.Fatalf("a batch of 3 took %s and %d calls; want one deadline and 1 call", took, calls.Load())
	}
}

// pullTierHost is a host that can't be pushed to, so check_inbox fetches.
type pullTierHost struct{ *pullOnlyHost }

func (pullTierHost) Tier() string { return bus.TierPull }

// check_inbox doesn't show again what was shown before: its confirmation
// was lost and its lease ran out. It is confirmed again, though (#26).
func TestCheckInboxDoesntShowAgainWhatItShowedBefore(t *testing.T) {
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		if r.URL.Path == "/v1/inbox" {
			io.WriteString(w, `{"messages":[{"id":"m1","thread_id":"t1","from":"bot","content":"old"},{"id":"m2","thread_id":"t2","from":"bot","content":"new"}]}`)
			return
		}
		io.WriteString(w, `{}`)
	}))
	defer api.Close()
	c := client.New(api.URL, "test-token")
	c.SetSession("s1")
	s := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	s.host = pullTierHost{&pullOnlyHost{}}
	s.markShown("m1")
	var taken []bus.Message
	res, rerr := s.callTool(context.Background(), json.RawMessage(`{"name":"check_inbox","arguments":{}}`), &taken)
	if rerr != nil {
		t.Fatal(rerr)
	}
	b, _ := json.Marshal(res)
	if strings.Contains(string(b), "m1") || !strings.Contains(string(b), "m2") {
		t.Fatalf("check_inbox answer: %s; want m2 only", b)
	}
	if len(taken) != 2 {
		t.Fatalf("taken: %+v; want both, so both are confirmed", taken)
	}
}
