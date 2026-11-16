package mcpserver_test

import (
	"context"
	"testing"
	"time"

	"birdie/internal/bus"
)

// startWait starts wait_for_reply on thread and returns its request id.
func startWait(s *session, thread string) int {
	s.id++
	s.send(map[string]any{"jsonrpc": "2.0", "id": s.id, "method": "tools/call",
		"params": map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 15}}})
	return s.id
}

// cancel tells the server the client gave up on request id.
func cancel(s *session, id int) {
	s.send(map[string]any{"jsonrpc": "2.0", "method": "notifications/cancelled", "params": map[string]any{"requestId": id, "reason": "interrupted"}})
}

// reply calls method and returns its answer, failing if the server answers
// the cancelled request instead.
func reply(t *testing.T, s *session, cancelled int, method string, params any) map[string]any {
	t.Helper()
	s.id++
	s.send(map[string]any{"jsonrpc": "2.0", "id": s.id, "method": method, "params": params})
	for {
		resp := s.next()
		switch resp["id"] {
		case float64(s.id):
			return resp["result"].(map[string]any)
		case float64(cancelled):
			t.Fatalf("answered the cancelled call: %v", resp)
		}
	}
}

// A wait_for_reply the user interrupted stops waiting: an answer that comes
// afterwards is pushed to the conversation, not confirmed for a call nobody
// reads.
func TestACancelledWaitLeavesTheAnswerToBePushed(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, _ := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))
	q := send(t, s, map[string]any{"to": "cursor", "content": "question"})
	id := startWait(s, q.ThreadID)
	time.Sleep(300 * time.Millisecond)
	cancel(s, id)
	time.Sleep(300 * time.Millisecond)
	a, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-h.pushed:
		if m.ID != a.ID {
			t.Fatalf("pushed %s", m.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the answer wasn't pushed")
	}
	reply(t, s, id, "ping", map[string]any{})
}

// In a session that checks for itself, the answer waits for the next check.
func TestACancelledWaitLeavesTheAnswerForTheNextCheck(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPull, pushed: make(chan bus.Message, 4)}
	s, cursor, _ := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))
	q := send(t, s, map[string]any{"to": "cursor", "content": "question"})
	id := startWait(s, q.ThreadID)
	time.Sleep(300 * time.Millisecond)
	cancel(s, id)
	time.Sleep(300 * time.Millisecond)
	a, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	out := toolJSON(t, reply(t, s, id, "tools/call", map[string]any{"name": "check_inbox", "arguments": map[string]any{"wait_seconds": 5}}))
	if msgs, _ := out["messages"].([]any); len(msgs) != 1 || msgs[0].(map[string]any)["id"] != a.ID {
		t.Fatalf("check_inbox = %+v", out)
	}
}

// An answer the pump hands to a waiting wait_for_reply is confirmed once the
// call returns it, and isn't pushed as well.
func TestAnAnswerHandedToAWaitIsConfirmed(t *testing.T) {
	ctx := context.Background()
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))
	q := send(t, s, map[string]any{"to": "cursor", "content": "question"})
	id := startWait(s, q.ThreadID)
	time.Sleep(300 * time.Millisecond)
	a, err := cursor.Send(ctx, bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil {
		t.Fatal(err)
	}
	resp := s.next()
	if resp["id"] != float64(id) {
		t.Fatalf("resp = %v", resp)
	}
	out := toolJSON(t, resp["result"].(map[string]any))
	if msgs, _ := out["messages"].([]any); len(msgs) != 1 || msgs[0].(map[string]any)["id"] != a.ID {
		t.Fatalf("wait_for_reply = %+v", out)
	}
	waitFor(t, "the answer to be confirmed", func() bool {
		m, err := st.GetMessage(ctx, a.ID)
		return err == nil && m.Status == bus.StatusDelivered
	})
	noPush(t, h, 200*time.Millisecond)
}
