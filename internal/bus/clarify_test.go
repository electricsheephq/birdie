package bus_test

import (
	"testing"

	"birdie/internal/bus"
)

func TestThreadStatus(t *testing.T) {
	ask := bus.Message{ID: "m1", From: "eva", To: "claude", Content: "please do X"}
	question := bus.Message{ID: "m2", From: "claude", To: "eva", InReplyTo: "m1", Content: "  (Question / not a steer) 1. which X?"}
	answer := bus.Message{ID: "m3", From: "eva", To: "claude", InReplyTo: "m2", Content: "the first"}
	again := bus.Message{ID: "m4", From: "claude", To: "eva", InReplyTo: "m3", Content: "(question / not a steer) 1. and Y?"}
	done := bus.Message{ID: "m5", From: "claude", To: "eva", InReplyTo: "m1", Content: "done"}
	notice := bus.Message{ID: "m9", From: bus.SystemSender, InReplyTo: "m2", Content: "held"}
	for _, c := range []struct {
		name string
		msgs []bus.Message
		want string
	}{
		{"empty", nil, ""},
		{"just the request", []bus.Message{ask}, ""},
		{"asked first", []bus.Message{ask, question}, bus.ThreadAwaitingClarification},
		{"a notice after the question", []bus.Message{ask, question, notice}, bus.ThreadAwaitingClarification},
		{"answered", []bus.Message{ask, question, answer}, ""},
		{"asked again after the answer", []bus.Message{ask, question, answer, again}, bus.ThreadAwaitingClarification},
		{"a new thread that is a question", []bus.Message{{ID: "m0", From: "eva", Content: "(question / not a steer) a new thread"}}, ""},
		// The everyday header on the starter's own follow-ups doesn't hold
		// up anything.
		{"the starter's follow-up question", []bus.Message{ask, {ID: "m2", From: "eva", InReplyTo: "m1", Content: "(question / not a steer) also Y?"}}, ""},
		{"the starter asks after an answer", []bus.Message{ask, question, answer, {ID: "m6", From: "eva", InReplyTo: "m3", Content: "(question / not a steer) done yet?"}}, ""},
		// Once the other side has answered or acted, its questions are
		// ordinary ones.
		{"a question after done", []bus.Message{ask, done, {ID: "m6", From: "claude", To: "eva", InReplyTo: "m1", Content: "(question / not a steer) was that useful?"}}, ""},
		// A question that continues the thread by its id, without
		// in_reply_to, counts too.
		{"asked by thread id", []bus.Message{ask, {ID: "m2", From: "claude", To: "eva", Content: "(question / not a steer) 1. which X?"}}, bus.ThreadAwaitingClarification},
		// A question for a third agent doesn't hold up the starter.
		{"a question for someone else", []bus.Message{ask, {ID: "m2", From: "claude", To: "codex", Content: "(question / not a steer) 1. which X?"}}, ""},
		{"passed on, then asked", []bus.Message{ask, {ID: "m2", From: "claude", To: "codex", Content: "please help with X"}, {ID: "m3", From: "codex", To: "claude", InReplyTo: "m2", Content: "(question / not a steer) 1. which X?"}}, ""},
		// Two sessions of one agent are different authors.
		{"one agent's two sessions", []bus.Message{
			{ID: "m1", From: "claude", FromSession: "s1", To: "claude", Content: "please do X"},
			{ID: "m2", From: "claude", FromSession: "s2", To: "claude", InReplyTo: "m1", Content: "(question / not a steer) 1. which X?"},
		}, bus.ThreadAwaitingClarification},
	} {
		if got := bus.ThreadStatus(c.msgs); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
	if !bus.AsksQuestions(question) || bus.AsksQuestions(ask) || bus.AsksQuestions(answer) {
		t.Error("AsksQuestions is for messages starting with the marker")
	}
}
