package bus_test

import (
	"context"
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
		// Only the agent the request went to asks first.
		{"an observer asks the starter", []bus.Message{ask, {ID: "m2", From: "codex", To: "eva", Content: "(question / not a steer) 1. why X?"}}, ""},
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

// The status as each agent reads the thread, which shows it only the messages
// it sent or got.
func TestThreadStatusInThreadsWithThreeAgents(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, n := range []string{"a", "b", "c"} {
		addAgent(t, st, n, nil)
	}
	send := func(from string, in bus.SendInput) bus.Message {
		t.Helper()
		m, err := b.Send(ctx, from, in)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	status := func(thread, reader string) string {
		t.Helper()
		msgs, err := b.Thread(ctx, thread, reader)
		if err != nil {
			t.Fatal(err)
		}
		return bus.ThreadStatus(msgs)
	}

	// c finishing its part doesn't answer b's questions to a.
	work := send("a", bus.SendInput{To: "b", Content: "implement it"})
	review := send("a", bus.SendInput{To: "c", ThreadID: work.ThreadID, Content: "review it"})
	send("c", bus.SendInput{InReplyTo: review.ID, Content: "review complete"})
	send("b", bus.SendInput{InReplyTo: work.ID, Content: bus.ClarifyMarker + " 1. Which branch?"})
	for _, reader := range []string{"a", "b"} {
		if got := status(work.ThreadID, reader); got != bus.ThreadAwaitingClarification {
			t.Errorf("as %s reads it: %q, want %q", reader, got, bus.ThreadAwaitingClarification)
		}
	}

	// c sees only replies in this thread, so it can't tell who started it:
	// its question about b's result holds up nothing.
	work = send("a", bus.SendInput{To: "b", Content: "implement it"})
	send("b", bus.SendInput{InReplyTo: work.ID, Content: "done"})
	result := send("b", bus.SendInput{InReplyTo: work.ID, To: "c", Content: "the result, for your records"})
	send("c", bus.SendInput{InReplyTo: result.ID, Content: bus.ClarifyMarker + " 1. Why this approach?"})
	for _, reader := range []string{"a", "b", "c"} {
		if got := status(work.ThreadID, reader); got != "" {
			t.Errorf("after done, as %s reads it: %q, want none", reader, got)
		}
	}

	// Nor does c's question to a, who never asked c for anything.
	work = send("a", bus.SendInput{To: "b", Content: "implement it"})
	send("b", bus.SendInput{InReplyTo: work.ID, Content: "done"})
	result = send("b", bus.SendInput{InReplyTo: work.ID, To: "c", Content: "the result, for your records"})
	send("c", bus.SendInput{InReplyTo: result.ID, To: "a", Content: bus.ClarifyMarker + " 1. Why this approach?"})
	for _, reader := range []string{"a", "b", "c"} {
		if got := status(work.ThreadID, reader); got != "" {
			t.Errorf("an observer's question, as %s reads it: %q, want none", reader, got)
		}
	}
}
