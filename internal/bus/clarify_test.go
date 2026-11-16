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
		// The starter's own follow-up under the everyday header isn't a
		// question holding up its request.
		{[]bus.Message{ask, {ID: "m2", From: "eva", InReplyTo: "m1", Content: "(question / not a steer) also Y?"}}, ""},
		{[]bus.Message{ask, question, answer, {ID: "m5", From: "eva", InReplyTo: "m3", Content: "(question / not a steer) done yet?"}}, ""},
		// Two sessions of one agent are different authors.
		{[]bus.Message{
			{ID: "m1", From: "claude", FromSession: "s1", Content: "please do X"},
			{ID: "m2", From: "claude", FromSession: "s2", InReplyTo: "m1", Content: "(question / not a steer) 1. which X?"},
		}, bus.ThreadAwaitingClarification},
		// Without the first message, it can't tell who started the thread.
		{[]bus.Message{question}, ""},
	} {
		if got := bus.ThreadStatus(c.msgs); got != c.want {
			t.Errorf("%+v: %q, want %q", c.msgs, got, c.want)
		}
	}
	if !bus.AsksFirst(question) || bus.AsksFirst(ask) || bus.AsksFirst(answer) {
		t.Error("AsksFirst is for replies starting with the marker")
	}
}
