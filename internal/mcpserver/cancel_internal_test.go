package mcpserver

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"log/slog"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
	"birdie/internal/httpapi"
	"birdie/internal/store"
)

// bridge is a Server with no session, on a test daemon where peer has sent
// claude one message.
func bridge(t *testing.T) (*Server, *store.SQLite, bus.Message, *bytes.Buffer) {
	t.Helper()
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	b := bus.New(st, logger)
	for _, name := range []string{"claude", "peer"} {
		if err := st.CreateAgent(ctx, bus.Agent{Name: name, TokenHash: bus.HashToken(name + "-tok"), Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
			t.Fatal(err)
		}
	}
	b.LoopLimit = -1
	api := httptest.NewServer(httpapi.New(b, logger))
	t.Cleanup(api.Close)
	m, err := b.Send(ctx, "peer", bus.SendInput{To: "claude", Content: "hello"})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(client.New(api.URL, "claude-tok"), "claude", "test", false, log.New(io.Discard, "", 0))
	out := &bytes.Buffer{}
	srv.out = out
	return srv, st, m, out
}

func status(t *testing.T, st *store.SQLite, id string) string {
	t.Helper()
	m, err := st.GetMessage(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return m.Status
}

var checkInbox = request{ID: json.RawMessage(`7`), Method: "tools/call", Params: json.RawMessage(`{"name":"check_inbox","arguments":{}}`)}

// A call answers, then confirms what it took, even with no session.
func TestACallConfirmsWhatItTookOnceAnswered(t *testing.T) {
	srv, st, m, out := bridge(t)
	srv.handle(context.Background(), checkInbox)
	if !bytes.Contains(out.Bytes(), []byte(m.ID)) {
		t.Fatalf("answer: %s", out)
	}
	if got := status(t, st, m.ID); got != bus.StatusDelivered {
		t.Fatalf("status %s", got)
	}
}

// A cancellation that comes while the answer waits to be written stops it,
// and what the call took goes back.
func TestACallCancelledBeforeItsAnswerIsWrittenGivesBackWhatItTook(t *testing.T) {
	srv, st, m, out := bridge(t)
	ctx, cancel := context.WithCancel(context.Background())
	srv.outMu.Lock() // another answer is being written
	done := make(chan struct{})
	go func() {
		srv.handle(ctx, checkInbox)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for status(t, st, m.ID) != bus.StatusClaimed {
		if time.Now().After(deadline) {
			t.Fatal("the call didn't take the message")
		}
		time.Sleep(5 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // at the output lock
	cancel()
	srv.outMu.Unlock()
	<-done
	if out.Len() != 0 {
		t.Fatalf("answered a cancelled call: %s", out)
	}
	if got := status(t, st, m.ID); got != bus.StatusQueued {
		t.Fatalf("status %s, want queued", got)
	}
}

// What the pump already showed and confirmed is returned to a wait, but not
// the wait's to confirm or give back.
func TestAWaitDoesntSettleWhatThePumpShowed(t *testing.T) {
	srv, _, _, _ := bridge(t)
	shown := bus.Message{ID: "msg_shown", ThreadID: "thr_x"}
	srv.remember(shown)
	msgs, took, err := srv.waitForReply(context.Background(), "thr_x", time.Second, true)
	if err != nil || len(msgs) != 1 || msgs[0].ID != shown.ID || len(took) != 0 {
		t.Fatalf("msgs %+v, took %+v, %v", msgs, took, err)
	}
}

func TestRequestIdsCompareAsJSON(t *testing.T) {
	for _, c := range [][2]string{{`"a"`, `"a"`}, {`1`, ` 1 `}, {`1`, `1.0`}} {
		if callKey(json.RawMessage(c[0])) != callKey(json.RawMessage(c[1])) {
			t.Errorf("%s and %s differ", c[0], c[1])
		}
	}
	if callKey(json.RawMessage(`1`)) == callKey(json.RawMessage(`"1"`)) {
		t.Error(`1 and "1" are the same`)
	}
}

// A request that reuses a finished one's id is still cancellable.
func TestAReusedIdStaysCancellable(t *testing.T) {
	srv, _, _, _ := bridge(t)
	_, doneFirst := srv.trackCall(context.Background(), json.RawMessage(`5`))
	second, doneSecond := srv.trackCall(context.Background(), json.RawMessage(`5`))
	defer doneSecond()
	doneFirst()
	srv.cancelCall(json.RawMessage(`5`))
	if second.Err() == nil {
		t.Fatal("the second request wasn't cancelled")
	}
}
