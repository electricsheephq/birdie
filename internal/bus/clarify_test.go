package bus_test

import (
	"testing"

	"birdie/internal/bus"
)

func TestThreadStatus(t *testing.T) {
	ask := bus.Message{ID: "m1", From: "eva", Content: "please do X"}
	question := bus.Message{ID: "m2", From: "claude", InReplyTo: "m1", Content: "  (Question / not a steer) 1. which X?"}
	answer := bus.Message{ID: "m3", From: "eva", InReplyTo: "m2", Content: "the first"}
	notice := bus.Message{ID: "m4", From: bus.SystemSender, InReplyTo: "m2", Content: "held"}
	for _, c := range []struct {
		msgs []bus.Message
		want string
	}{
		{nil, ""},
		{[]bus.Message{ask}, ""},
		{[]bus.Message{ask, question}, bus.ThreadAwaitingClarification},
		{[]bus.Message{ask, question, notice}, bus.ThreadAwaitingClarification},
		{[]bus.Message{ask, question, answer}, ""},
		{[]bus.Message{{ID: "m0", From: "eva", Content: "(question / not a steer) a new thread"}}, ""},
	} {
		if got := bus.ThreadStatus(c.msgs); got != c.want {
			t.Errorf("%d messages: %q, want %q", len(c.msgs), got, c.want)
		}
	}
}
