package mcpserver_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/mcpserver"
)

// A reply that asks questions before acting shows the sender's thread as
// awaiting clarification, until the sender answers.
func TestClarifyBackThreadStatus(t *testing.T) {
	s, cursor := setup(t, false)
	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "tidy the repo"}}))
	thread := sent["thread_id"].(string)

	go func() {
		msgs, err := cursor.Inbox(context.Background(), "", 5*time.Second)
		if err != nil || len(msgs) != 1 {
			return
		}
		cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: bus.ClarifyMarker + "\n1. Which repo?\n2. Delete branches too?", InReplyTo: msgs[0].ID})
	}()
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 10}}))
	if got["thread_status"] != bus.ThreadAwaitingClarification {
		t.Fatalf("wait_for_reply = %v", got)
	}
	question := got["messages"].([]any)[0].(map[string]any)["id"].(string)
	th := toolJSON(t, s.call("tools/call", map[string]any{"name": "get_thread", "arguments": map[string]any{"thread_id": thread}}))
	if th["status"] != bus.ThreadAwaitingClarification {
		t.Fatalf("get_thread = %v", th)
	}

	// Once the sender answers, the thread isn't waiting on it any more.
	toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"in_reply_to": question, "content": "1. birdie\n2. no"}}))
	th = toolJSON(t, s.call("tools/call", map[string]any{"name": "get_thread", "arguments": map[string]any{"thread_id": thread}}))
	if _, ok := th["status"]; ok {
		t.Fatalf("get_thread after the answer = %v", th)
	}
}

func TestClarifyBackNotes(t *testing.T) {
	m := bus.Message{ID: "msg_1", ThreadID: "thr_1", From: "grok-bot", Content: "tidy the repo"}
	if text := mcpserver.MessageText(m); !strings.Contains(text, "ask first: reply starting \""+bus.ClarifyMarker+"\"") {
		t.Fatalf("a new request should say how to ask first: %s", text)
	}
	m.InReplyTo = "msg_0"
	if text := mcpserver.MessageText(m); strings.Contains(text, bus.ClarifyMarker) {
		t.Fatalf("an ordinary reply shouldn't invite questions: %s", text)
	}
	m.Content = "  (Question / not a steer)\n1. Which repo?"
	if text := mcpserver.MessageText(m); !strings.Contains(text, "asks you questions, and may wait") {
		t.Fatalf("a clarify-back reply should say it asks questions: %s", text)
	}
	m.InReplyTo = ""
	if text := mcpserver.MessageText(m); strings.Contains(text, "asks you questions") {
		t.Fatalf("a new message starting with the marker isn't a clarify-back reply: %s", text)
	}
}
