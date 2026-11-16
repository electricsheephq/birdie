package mcpserver

import (
	"strings"
	"testing"

	"birdie/internal/bus"
)

// A long message is pushed as a pointer to get_thread; a short one whole.
func TestALongMessageIsPushedAsAPointer(t *testing.T) {
	short := bus.Message{ID: "msg_s", ThreadID: "thr_s", From: "peer", Content: "hello"}
	if got := PushText(short); got != MessageText(short) {
		t.Fatalf("short: %q", got)
	}
	long := bus.Message{ID: "msg_l", ThreadID: "thr_l", From: "peer", Content: strings.Repeat("x", maxPushBytes)}
	got := PushText(long)
	if len(got) > 1000 || !strings.Contains(got, "get_thread") || !strings.Contains(got, "msg_l") || !strings.Contains(got, "thr_l") {
		t.Fatalf("long: %q", got)
	}
}
