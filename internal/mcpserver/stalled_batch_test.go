package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
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
// message, whether it never answers or stops while giving its reason.
func TestAStalledDaemonHoldsABatchForOneDeadline(t *testing.T) {
	for _, tc := range []struct {
		name  string
		stall func(w http.ResponseWriter)
	}{
		{"no answer", func(http.ResponseWriter) {}},
		{"answer without its reason", func(w http.ResponseWriter) {
			w.WriteHeader(http.StatusInternalServerError)
			io.WriteString(w, `{"error":`)
			w.(http.Flusher).Flush()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel() // waits out the real ten-second deadline
			var calls atomic.Int32
			release := make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				calls.Add(1)
				tc.stall(w)
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
		})
	}
}

// pullTierHost is a host that can't be pushed to, so check_inbox fetches.
type pullTierHost struct{ *pullOnlyHost }

func (pullTierHost) Tier() string { return bus.TierPull }

// convHost is a pull-tier host whose conversation id changes, as /clear or
// the real id arriving changes it.
type convHost struct {
	pullOnlyHost
	id atomic.Pointer[string]
}

func newConvHost(id string) *convHost {
	h := &convHost{}
	h.set(id)
	return h
}

func (h *convHost) set(id string)        { h.id.Store(&id) }
func (h *convHost) Session() SessionInfo { return SessionInfo{ID: *h.id.Load(), Name: "api"} }
func (h *convHost) Tier() string         { return bus.TierPull }

// claims is a daemon that hands out ids, one per claim, and records what is
// confirmed. during, if set, runs inside the first claim: what changes while
// a call waits. The first confirmation of lost is refused: it is lost.
func claims(t *testing.T, h Host, ids []string, lost string, during func(*Server)) (*Server, func() []string) {
	t.Helper()
	var srv atomic.Pointer[Server]
	var n atomic.Int32
	var mu sync.Mutex
	acked := []string{}
	refused := false
	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		switch {
		case r.URL.Path == "/v1/inbox":
			i := int(n.Add(1)) - 1
			if i == 0 && during != nil {
				during(srv.Load())
			}
			if i >= len(ids) {
				io.WriteString(w, `{"messages":[]}`)
				return
			}
			fmt.Fprintf(w, `{"messages":[{"id":%q,"thread_id":"t1","from":"bot","content":"hi"}]}`, ids[i])
		case strings.HasSuffix(r.URL.Path, "/ack"):
			id := strings.Split(r.URL.Path, "/")[3]
			mu.Lock()
			defer mu.Unlock()
			if id == lost && !refused {
				refused = true
				http.Error(w, `{"error":"lost"}`, http.StatusConflict)
				return
			}
			acked = append(acked, id)
			io.WriteString(w, `{}`)
		default:
			io.WriteString(w, `{}`)
		}
	}))
	t.Cleanup(api.Close)
	c := client.New(api.URL, "test-token")
	c.SetSession(h.Session().ID)
	s := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
	s.host = h
	srv.Store(s)
	return s, func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(acked)
	}
}

// claimAPI hands out m1, which the conversation was shown before (its
// confirmation was lost and its lease ran out), then m2 on the next claim.
func claimAPI(t *testing.T, h Host) (*Server, func() []string) {
	t.Helper()
	s, acked := claims(t, h, []string{"m1", "m2"}, "", nil)
	s.markShown("m1", s.convNow())
	return s, acked
}

// ask makes a check_inbox call the way the client does, and returns what was
// written back.
func ask(s *Server) string {
	var out bytes.Buffer
	s.out = &out
	s.handle(context.Background(), request{ID: json.RawMessage(`1`), Method: "tools/call", Params: json.RawMessage(`{"name":"check_inbox","arguments":{}}`)})
	return out.String()
}

// fetch makes a check_inbox call and returns its answer and what it took.
func fetch(t *testing.T, s *Server, call string) (string, []bus.Message) {
	t.Helper()
	var taken []bus.Message
	res, rerr := s.callTool(context.Background(), json.RawMessage(call), &taken, new(uint64))
	if rerr != nil {
		t.Fatal(rerr)
	}
	b, _ := json.Marshal(res)
	return string(b), taken
}

// check_inbox and wait_for_reply don't show again what was shown before.
// They confirm it and go on waiting for something new (#26). A push
// session's wait_for_reply checks the thread itself too.
func TestAPullCallConfirmsWhatItShowedBeforeAndWaitsOn(t *testing.T) {
	for _, tc := range []struct {
		call string
		host Host
	}{
		{`{"name":"check_inbox","arguments":{"wait_seconds":5}}`, pullTierHost{&pullOnlyHost{}}},
		{`{"name":"wait_for_reply","arguments":{"thread_id":"t1","timeout_seconds":5}}`, pullTierHost{&pullOnlyHost{}}},
		{`{"name":"wait_for_reply","arguments":{"thread_id":"t1","timeout_seconds":5}}`, &pullOnlyHost{}},
	} {
		s, acked := claimAPI(t, tc.host)
		answer, taken := fetch(t, s, tc.call)
		if strings.Contains(answer, `m1`) || !strings.Contains(answer, `m2`) {
			t.Fatalf("%s answer: %s; want m2 only", tc.call, answer)
		}
		if len(taken) != 1 || taken[0].ID != "m2" {
			t.Fatalf("%s taken: %+v; want m2, to confirm once answered", tc.call, taken)
		}
		if got := acked(); len(got) != 1 || got[0] != "m1" {
			t.Fatalf("%s confirmed %v; want m1", tc.call, got)
		}
	}
}

// What an earlier conversation was shown, before /clear, is shown in the new
// one: the new conversation hasn't seen it (#26).
func TestAMessageShownBeforeClearIsShownAgainAfterIt(t *testing.T) {
	h := newConvHost("s1")
	s, acked := claimAPI(t, h)
	h.set("s2") // /clear
	answer, taken := fetch(t, s, `{"name":"check_inbox","arguments":{}}`)
	if !strings.Contains(answer, `m1`) || len(taken) != 1 || taken[0].ID != "m1" {
		t.Fatalf("check_inbox answer: %s, taken %+v; want m1, to confirm once answered", answer, taken)
	}
	if got := acked(); len(got) != 0 {
		t.Fatalf("confirmed %v before answering", got)
	}
}

// What the conversation was shown under its stand-in id counts as shown once
// its real id is known: it is the same conversation (#26).
func TestAMessageShownUnderTheStandInCountsOnceTheRealIDIsKnown(t *testing.T) {
	h := newConvHost(procID())
	s, acked := claimAPI(t, h)
	h.set("s1") // the real id arrives
	answer, _ := fetch(t, s, `{"name":"check_inbox","arguments":{"wait_seconds":5}}`)
	if strings.Contains(answer, `m1`) || !strings.Contains(answer, `m2`) {
		t.Fatalf("check_inbox answer: %s; want m2 only", answer)
	}
	if got := acked(); len(got) != 1 || got[0] != "m1" {
		t.Fatalf("confirmed %v; want m1", got)
	}
}

// A call made under the stand-in id that answers after the real id arrived
// counts for the same conversation: what it showed isn't shown again (#26).
func TestAStandInCallsAnswerCountsOnceTheRealIDIsKnown(t *testing.T) {
	h := newConvHost(procID())
	s, acked := claims(t, h, []string{"m1", "m1", "m2"}, "m1", func(s *Server) {
		h.set("s1") // another call brings the real id while this one waits
		s.syncSession(context.Background(), h)
	})
	if out := ask(s); !strings.Contains(out, `m1`) {
		t.Fatalf("first answer: %s; want m1", out)
	}
	answer, _ := fetch(t, s, `{"name":"check_inbox","arguments":{"wait_seconds":5}}`)
	if strings.Contains(answer, `m1`) || !strings.Contains(answer, `m2`) {
		t.Fatalf("check_inbox answer: %s; want m2 only", answer)
	}
	if got := acked(); len(got) != 1 || got[0] != "m1" {
		t.Fatalf("confirmed %v; want m1", got)
	}
}

// When the stand-in id comes back after the real one, it is a new
// conversation: what a call showed before is shown in it, not confirmed
// unseen (#26).
func TestAMessageShownBeforeTheStandInCameBackIsShownAgain(t *testing.T) {
	h := newConvHost(procID())
	s, acked := claims(t, h, []string{"m1", "m1"}, "m1", func(s *Server) {
		h.set("s1") // the real id arrives
		s.syncSession(context.Background(), h)
		h.set(procID()) // and goes: a new conversation
		s.syncSession(context.Background(), h)
	})
	if out := ask(s); !strings.Contains(out, `m1`) {
		t.Fatalf("first answer: %s; want m1", out)
	}
	answer, taken := fetch(t, s, `{"name":"check_inbox","arguments":{}}`)
	if !strings.Contains(answer, `m1`) || len(taken) != 1 || taken[0].ID != "m1" {
		t.Fatalf("check_inbox answer: %s, taken %+v; want m1, to confirm once answered", answer, taken)
	}
	if got := acked(); len(got) != 0 {
		t.Fatalf("confirmed %v before answering", got)
	}
}

// A call's answer is recorded for the conversation that made the call, even
// if /clear switches the bridge to another one while it runs: the new one
// is shown it (#26).
func TestAnAnswerIsRecordedForTheConversationThatAsked(t *testing.T) {
	h := newConvHost("s1")
	s, acked := claims(t, h, []string{"m2", "m2"}, "m2", func(s *Server) {
		h.set("s2") // /clear lands while the call runs
		s.syncSession(context.Background(), h)
	})
	if out := ask(s); !strings.Contains(out, `m2`) {
		t.Fatalf("first answer: %s; want m2", out)
	}
	answer, taken := fetch(t, s, `{"name":"check_inbox","arguments":{}}`)
	if !strings.Contains(answer, `m2`) || len(taken) != 1 || taken[0].ID != "m2" {
		t.Fatalf("check_inbox answer in the new conversation: %s, taken %+v; want m2", answer, taken)
	}
	if got := acked(); len(got) != 0 {
		t.Fatalf("confirmed %v before answering", got)
	}
}
