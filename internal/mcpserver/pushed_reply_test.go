package mcpserver_test

import (
	"context"
	"testing"
	"time"

	"birdie/internal/bus"
)

// An answer pushed before the session waits for it is still what
// wait_for_reply returns: leaving out what was shown before (#26) applies
// only to what the call itself took.
func TestWaitForReplyReturnsAnAnswerAlreadyPushed(t *testing.T) {
	h := &fakeHost{tier: bus.TierPush, pushed: make(chan bus.Message, 4)}
	s, cursor, st := setupHost(t, h)
	waitFor(t, "the session to register", sessionLive(cursor))

	asked := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "which branch?"}}))
	q, err := cursor.Inbox(context.Background(), "", 0)
	if err != nil || len(q) != 1 {
		t.Fatalf("cursor inbox: %v %v", q, err)
	}
	a, err := cursor.Send(context.Background(), bus.SendInput{InReplyTo: q[0].ID, Content: "main"})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-h.pushed:
		if got.ID != a.ID {
			t.Fatalf("pushed %s, want %s", got.ID, a.ID)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("answer never pushed")
	}
	// Confirmed once the bridge has recorded it as shown.
	waitFor(t, "the answer to be confirmed", func() bool {
		got, _ := st.GetMessage(context.Background(), a.ID)
		return got.Status == bus.StatusDelivered
	})
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": asked["thread_id"], "timeout_seconds": 2}}))
	msgs, _ := got["messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["id"] != a.ID {
		t.Fatalf("wait_for_reply: %v; want the pushed answer %s", got, a.ID)
	}
}
