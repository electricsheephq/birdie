package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestPrintThread(t *testing.T) {
	ctx := context.Background()
	b, st := statusBus(t)
	// A new conversation for claude, given to the session it picks.
	if err := st.SetAgentConfig(ctx, "claude", "fallback", "on"); err != nil {
		t.Fatal(err)
	}
	q, err := b.Send(ctx, "codex", bus.SendInput{To: "claude", Content: "first line\nsecond line\n" + strings.Repeat("long ", 200)})
	if err != nil {
		t.Fatal(err)
	}
	if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s-api"}); err != nil || len(got) != 1 {
		t.Fatalf("take = %+v, %v", got, err)
	}
	if _, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: q.ID, Content: "done", FromSession: "s-api"}); err != nil {
		t.Fatal(err)
	}
	msgs, err := st.TrackThread(ctx, q.ThreadID)
	if err != nil {
		t.Fatal(err)
	}
	agents, _ := b.AgentsWithRecentSessions(ctx)

	var out bytes.Buffer
	printThread(&out, agents, msgs, false, time.Now())
	text := out.String()
	for _, want := range []string{
		"2 messages between Codex and Claude Code",
		"Codex → Claude Code\n",
		"Status: answered; delivered after ",
		"s to Claude Code · api\n",
		"Claude Code · api → Codex",
		"   second line",
		"--full",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("missing %q in:\n%s", want, text)
		}
	}
	if strings.Contains(text, "Awaiting clarification") {
		t.Errorf("an answered thread isn't awaiting clarification:\n%s", text)
	}
	out.Reset()
	printThread(&out, agents, msgs, true, time.Now())
	if strings.Contains(out.String(), "--full") || !strings.Contains(out.String(), strings.Repeat("long ", 199)) {
		t.Errorf("--full should show everything:\n%s", out.String())
	}

	// A question after "done" is an ordinary one: nothing waits on it.
	if _, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: q.ID, Content: bus.ClarifyMarker + "\n1. Was that useful?", FromSession: "s-api"}); err != nil {
		t.Fatal(err)
	}
	if msgs, err = st.TrackThread(ctx, q.ThreadID); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	printThread(&out, agents, msgs, false, time.Now())
	if strings.Contains(out.String(), "Awaiting clarification") {
		t.Errorf("a question after the work isn't a clarification:\n%s", out.String())
	}

	// Questions before acting leave a new request awaiting clarification.
	q2, err := b.Send(ctx, "codex", bus.SendInput{To: "claude", Content: "tidy the repo"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: q2.ID, Content: bus.ClarifyMarker + "\n1. Which repo?", FromSession: "s-api"}); err != nil {
		t.Fatal(err)
	}
	if msgs, err = st.TrackThread(ctx, q2.ThreadID); err != nil {
		t.Fatal(err)
	}
	out.Reset()
	printThread(&out, agents, msgs, false, time.Now())
	if !strings.Contains(out.String(), "Awaiting clarification") {
		t.Errorf("missing the clarification status in:\n%s", out.String())
	}
}
