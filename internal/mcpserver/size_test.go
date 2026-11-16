package mcpserver_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

// Long messages go through whole, both ways, over the bridge and HTTP.
func TestLongMessagesGoThrough(t *testing.T) {
	ctx := context.Background()
	s, cursor := setup(t, false)
	out := strings.Repeat("from claude ", 500_000) // 6 MB
	toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor", "content": out}}))
	got, err := cursor.Inbox(ctx, "", 5*time.Second)
	if err != nil || len(got) != 1 || got[0].Content != out {
		t.Fatalf("cursor got %d messages, %v", len(got), err)
	}
	in := strings.Repeat("from cursor ", 500_000)
	if _, err := cursor.Send(ctx, bus.SendInput{To: "claude", Content: in}); err != nil {
		t.Fatal(err)
	}
	res := toolJSON(t, s.call("tools/call", map[string]any{"name": "check_inbox", "arguments": map[string]any{"wait_seconds": 5}}))
	msgs, _ := res["messages"].([]any)
	if len(msgs) != 1 || msgs[0].(map[string]any)["content"] != in {
		t.Fatalf("claude got %d messages", len(msgs))
	}
}
