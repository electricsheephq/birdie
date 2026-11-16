package mcpserver_test

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/mcpserver"
)

// A reply that asks questions before acting shows the sender's thread as
// awaiting clarification, until the sender answers.
func TestClarifyBackThreadStatus(t *testing.T) {
	for _, channel := range []bool{false, true} {
		t.Run(map[bool]string{false: "pull", true: "push"}[channel], func(t *testing.T) { clarifyBack(t, channel) })
	}
}

func clarifyBack(t *testing.T, channel bool) {
	s, cursor := setup(t, channel)

	// An ordinary answer has no status.
	plain := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "run the tests"}}))
	go func() {
		msgs, err := cursor.Inbox(context.Background(), "", 5*time.Second)
		if err != nil || len(msgs) != 1 {
			return
		}
		cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: "all green", InReplyTo: msgs[0].ID})
	}()
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": plain["thread_id"], "timeout_seconds": 10}}))
	if _, ok := got["thread_status"]; ok || len(got["messages"].([]any)) != 1 {
		t.Fatalf("wait_for_reply for an ordinary answer = %v", got)
	}

	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "tidy the repo"}}))
	thread := sent["thread_id"].(string)

	go func() {
		msgs, err := cursor.Inbox(context.Background(), "", 5*time.Second)
		if err != nil || len(msgs) != 1 {
			return
		}
		cursor.Send(context.Background(), bus.SendInput{To: "claude", Content: bus.ClarifyMarker + "\n1. Which repo?\n2. Delete branches too?", InReplyTo: msgs[0].ID})
	}()
	got = toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 10}}))
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
	// A question that starts a thread, or continues one by its id, is
	// answered, not met with questions of its own.
	m.InReplyTo = ""
	if text := mcpserver.MessageText(m); !strings.Contains(text, "asks you questions") || strings.Contains(text, "ask first") || !strings.Contains(text, "SendMessage") {
		t.Fatalf("a message starting with the marker should say it asks questions: %s", text)
	}
}

// A question continuing the thread by its id, without in_reply_to, counts.
func TestClarifyBackByThreadID(t *testing.T) {
	s, cursor := setup(t, false)
	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "tidy the repo"}}))
	thread := sent["thread_id"].(string)
	if _, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", ThreadID: thread, Content: bus.ClarifyMarker + " 1. Which repo?"}); err != nil {
		t.Fatal(err)
	}
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 10}}))
	if got["thread_status"] != bus.ThreadAwaitingClarification {
		t.Fatalf("wait_for_reply = %v", got)
	}
}

// A slow read of the thread's status doesn't hold up the reply wait_for_reply
// already has: it comes back without the status.
func TestClarifyBackStatusReadIsBounded(t *testing.T) {
	release := make(chan struct{})
	s, cursor := setupWith(t, false, func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/v1/threads/") {
				select { // stall until the caller gives up
				case <-r.Context().Done():
				case <-release:
				}
				return
			}
			h.ServeHTTP(w, r)
		})
	})
	t.Cleanup(func() { close(release) }) // before the API server closes
	sent := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": "tidy the repo"}}))
	thread := sent["thread_id"].(string)
	if _, err := cursor.Send(context.Background(), bus.SendInput{To: "claude", ThreadID: thread, Content: bus.ClarifyMarker + " 1. Which repo?"}); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "wait_for_reply", "arguments": map[string]any{"thread_id": thread, "timeout_seconds": 30}}))
	if took := time.Since(start); took > 6*time.Second {
		t.Fatalf("wait_for_reply took %v behind a stalled status read", took)
	}
	if msgs, _ := got["messages"].([]any); len(msgs) != 1 {
		t.Fatalf("wait_for_reply = %v", got)
	}
	if _, ok := got["thread_status"]; ok {
		t.Fatalf("a status that couldn't be read was reported: %v", got)
	}
}
