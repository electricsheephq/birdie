package bus_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestUnrankedMailUsesTheRecipientsP3SettingAndWaitsForTheDigest(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	addAgent(t, st, "alice", nil)
	if err := st.SetAgentConfig(ctx, "bob", "unranked_as", "p3"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	p := priorityInbox{b, &now}
	p.report(t, now)
	p.claim(t, bus.Turn)
	m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob", Content: "update"})
	if err != nil {
		t.Fatal(err)
	}
	stored, err := st.GetMessage(ctx, m.ID)
	if err != nil || stored.Priority != "p3" || m.Priority != "p3" || m.ToSession != "" {
		t.Fatalf("stored priority = %q, sent = %+v, err = %v", stored.Priority, m, err)
	}
	p.claim(t, bus.Turn)
	now = now.Add(bus.DigestEvery)
	p.report(t, now)
	p.claim(t, bus.Turn, m.ID)
}

func TestASessionNameOverrideWinsWithoutChangingOtherSessions(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	for _, agent := range []string{"alice", "claude"} {
		addAgent(t, st, agent, nil)
	}
	for key, value := range map[string]string{"unranked_as": "p1", "unranked_as:api": "p3", "unranked_as:web": "immediate", "default_session": "api"} {
		if err := st.SetAgentConfig(ctx, "claude", key, value); err != nil {
			t.Fatal(err)
		}
	}
	for id, name := range map[string]string{"s1": "api", "s2": "web", "s3": "other"} {
		report(t, b, "claude", id, name, bus.TierPush, time.Now())
	}
	for _, tc := range []struct{ to, want string }{
		{"claude/api", "p3"}, {"claude/s1", "p3"}, {"claude/api#s1", "p3"},
		{"claude", "p3"}, {"claude/web", ""}, {"claude/other", "p1"},
	} {
		m, err := b.Send(ctx, "alice", bus.SendInput{To: tc.to, Content: "update"})
		if err != nil || m.Priority != tc.want {
			t.Errorf("send to %s = %q, %v, want %q", tc.to, m.Priority, err, tc.want)
		}
	}
}

func TestImmediateAndMissingSettingsLeaveMailUnranked(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	for _, tc := range []struct{ setting, want string }{{"p1", "p1"}, {"immediate", ""}, {"", ""}} {
		if err := st.SetAgentConfig(ctx, "bob", "unranked_as", tc.setting); err != nil {
			t.Fatal(err)
		}
		m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob", Content: "update"})
		if err != nil || m.Priority != tc.want {
			t.Errorf("setting %q: priority = %q, %v, want %q", tc.setting, m.Priority, err, tc.want)
		}
	}
}

func TestARecipientDefaultPreservesExplicitPriorities(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	for _, tc := range []struct{ setting, sent, want string }{
		{"p3", "p0", "p0"}, {"p3", "p1", "p1"}, {"p1", "p3", "p3"}, {"p3", "", "p3"},
	} {
		if err := st.SetAgentConfig(ctx, "bob", "unranked_as", tc.setting); err != nil {
			t.Fatal(err)
		}
		m, err := b.Send(ctx, "alice", bus.SendInput{To: "bob", Content: "update", Priority: tc.sent})
		if err != nil || m.Priority != tc.want {
			t.Errorf("setting %q and sent %q: priority = %q, %v, want %q", tc.setting, tc.sent, m.Priority, err, tc.want)
		}
	}
}

func TestTheP3FloorLiftsAnUnrankedFirstReplyAndQuestionsToP1(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "bob", nil)
	if err := st.SetAgentConfig(ctx, "bob", "unranked_as:work", "p3"); err != nil {
		t.Fatal(err)
	}
	report(t, b, "bob", "desk", "work", bus.TierPush, time.Now())
	parent, err := b.Send(ctx, "bob", bus.SendInput{To: "alice", FromSession: "desk", Content: "request"})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		in   bus.SendInput
		want string
	}{
		{bus.SendInput{InReplyTo: parent.ID, Content: "answer"}, "p1"},
		{bus.SendInput{InReplyTo: parent.ID, Content: "follow-up", Priority: "p3"}, "p3"},
		{bus.SendInput{To: "bob/work", Content: bus.ClarifyMarker + " Which option?"}, "p1"},
	} {
		m, err := b.Send(ctx, "alice", tc.in)
		if err != nil || m.Priority != tc.want {
			t.Errorf("%q: priority = %q, %v, want %q", tc.in.Content, m.Priority, err, tc.want)
		}
	}
}

func TestBothAnUnrankedAckAndAnswerToTheRecipientsRequestAreLiftedToP1(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "bob", nil)
	if err := st.SetAgentConfig(ctx, "bob", "unranked_as", "p3"); err != nil {
		t.Fatal(err)
	}
	report(t, b, "bob", "desk", "work", bus.TierPush, time.Now())
	for _, priority := range []string{"", "p3"} {
		parent, err := b.Send(ctx, "bob", bus.SendInput{To: "alice", FromSession: "desk", Content: "request"})
		if err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct{ content, sent, want string }{
			{"ack", "", "p1"}, {"answer", priority, "p1"},
		} {
			if tc.sent == "p3" {
				tc.want = "p3"
			}
			m, err := b.Send(ctx, "alice", bus.SendInput{InReplyTo: parent.ID, Priority: tc.sent, Content: tc.content})
			if err != nil || m.Priority != tc.want || m.Defaulted != (tc.sent == "") {
				t.Errorf("%s (sent %q): priority = %q, defaulted = %t, err = %v; want %q", tc.content, tc.sent, m.Priority, m.Defaulted, err, tc.want)
			}
			if tc.content == "answer" && tc.sent == "" && !strings.Contains(m.Note, "reply to its own message") {
				t.Errorf("answer note = %q", m.Note)
			}
		}
	}
}

func TestMailRoutedToAChosenSessionUsesThatSessionsSetting(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	for key, value := range map[string]string{"unranked_as": "p1", "unranked_as:api": "p3", "default_session": "api"} {
		if err := st.SetAgentConfig(ctx, "claude", key, value); err != nil {
			t.Fatal(err)
		}
	}
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	for _, tc := range []struct{ to, want string }{{"claude", "p3"}, {"claude/api", "p3"}} {
		m, err := b.Send(ctx, "alice", bus.SendInput{To: tc.to, Content: "update"})
		if err != nil || m.Priority != tc.want || m.ToSession != "s1" {
			t.Errorf("send to %s: priority = %q, session = %q, err = %v; want %q to s1", tc.to, m.Priority, m.ToSession, err, tc.want)
		}
	}
}

func TestPersonalMessagesAndBirdieNoticesStayUnranked(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	if err := st.SetAgentConfig(ctx, "bob", "unranked_as", "p3"); err != nil {
		t.Fatal(err)
	}
	for from, want := range map[string]string{"me": "", bus.SystemSender: "", bus.LegacySystemSender: "", "alice": "p3"} {
		m, err := b.Send(ctx, from, bus.SendInput{To: "bob", Content: "update"})
		if err != nil || m.Priority != want {
			t.Errorf("send from %s = %q, %v, want %q", from, m.Priority, err, want)
		}
	}
}

func TestTheSendResultExplainsARecipientDefault(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	if err := st.SetAgentConfig(ctx, "bob", "unranked_as", "p3"); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Truncate(time.Millisecond)
	b.Now = func() time.Time { return now }
	p := priorityInbox{b, &now}
	p.report(t, now)
	p.claim(t, bus.Turn)
	for _, priority := range []string{"", "p3"} {
		m := p.sendToDesk(t, "alice", priority, "update")
		data, err := json.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		var fields map[string]any
		if err := json.Unmarshal(data, &fields); err != nil {
			t.Fatal(err)
		}
		if priority == "" {
			if fields["defaulted"] != true || !strings.HasPrefix(m.Note, "p3 by the recipient's setting for unranked mail; held:") {
				t.Errorf("defaulted send = %s", data)
			}
		} else if _, found := fields["defaulted"]; found || !strings.HasPrefix(m.Note, "held:") {
			t.Errorf("explicit send = %s", data)
		}
	}
}

func TestBareMailToAnAgentThatPicksItsSessionUsesTheAgentWideSetting(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bob", nil)
	for key, value := range map[string]string{"unranked_as:work": "p3", "default_session": "work"} {
		if err := st.SetAgentConfig(ctx, "bob", key, value); err != nil {
			t.Fatal(err)
		}
	}
	report(t, b, "bob", "s1", "work", bus.TierPush, time.Now())
	for _, tc := range []struct{ to, want string }{{"bob", ""}, {"bob/work", "p3"}} {
		m, err := b.Send(ctx, "alice", bus.SendInput{To: tc.to, Content: "update"})
		if err != nil || m.Priority != tc.want {
			t.Errorf("send to %s = %q (session %q), %v; want %q", tc.to, m.Priority, m.ToSession, err, tc.want)
		}
	}
}
