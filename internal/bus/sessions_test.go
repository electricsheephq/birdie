package bus_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

func report(t *testing.T, b *bus.Bus, agent, id, name, tier string, active time.Time) {
	t.Helper()
	if err := b.ReportSession(context.Background(), agent, id, bus.SessionInput{Name: name, Tier: tier, ActiveAt: active}); err != nil {
		t.Fatal(err)
	}
}

// picksSession has agent pick a session itself for a message addressed to it
// alone, as every agent did before claude stopped (see bus.PicksSession).
func picksSession(t *testing.T, st *store.SQLite, agent string) {
	t.Helper()
	if err := st.SetAgentConfig(context.Background(), agent, "fallback", "on"); err != nil {
		t.Fatal(err)
	}
}

func take(t *testing.T, b *bus.Bus, agent, session string) []bus.Message {
	t.Helper()
	msgs, err := b.ReceiveFor(context.Background(), bus.ReceiveInput{Agent: agent, Session: session, Lease: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	return msgs
}

func TestParseAddress(t *testing.T) {
	for in, want := range map[string]bus.Address{
		"claude":            {Agent: "claude"},
		"claude/api":        {Agent: "claude", Session: "api"},
		"claude/api@laptop": {Agent: "claude", Session: "api", Node: "laptop"},
		"claude@laptop":     {Agent: "claude", Node: "laptop"},
	} {
		got, err := bus.ParseAddress(in)
		if err != nil || got != want || got.String() != in {
			t.Errorf("ParseAddress(%q) = %+v, %v", in, got, err)
		}
	}
	for _, bad := range []string{"", "/x", "claude/", "claude@", "claude/a b"} {
		if _, err := bus.ParseAddress(bad); !errors.Is(err, bus.ErrInvalid) {
			t.Errorf("ParseAddress(%q): err = %v, want ErrInvalid", bad, err)
		}
	}
}

func TestOtherMachinesAreRefusedForNow(t *testing.T) {
	b, st := newBus(t)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "bob", nil)
	_, err := b.Send(context.Background(), "alice", bus.SendInput{To: "bob@laptop", Content: "hi"})
	if !errors.Is(err, bus.ErrInvalid) || !strings.Contains(err.Error(), "this machine") {
		t.Fatalf("err = %v", err)
	}
}

func TestSystemSenderIsReserved(t *testing.T) {
	if bus.ValidName(bus.SystemSender) {
		t.Fatal("an agent must not be able to take the bus's own name")
	}
}

func TestNewConversationsGoToTheDefaultSession(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	now := time.Now()
	report(t, b, "claude", "old", "old", bus.TierPush, now.Add(-time.Hour))
	report(t, b, "claude", "new", "new", bus.TierPush, now)
	report(t, b, "claude", "headless", "cli", bus.TierPull, now.Add(time.Second))

	m, err := b.Send(ctx, "alice", bus.SendInput{To: "claude", Content: "hi"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(m.Note, "Claude Code · new gets this right away") {
		t.Fatalf("note = %q", m.Note)
	}
	// A session that can't see it soon doesn't win just by being most recent.
	if got := take(t, b, "claude", "headless"); len(got) != 0 {
		t.Fatalf("pull session took %+v", got)
	}
	if got := take(t, b, "claude", "old"); len(got) != 0 {
		t.Fatalf("old session took %+v", got)
	}
	if got := take(t, b, "claude", "new"); len(got) != 1 {
		t.Fatalf("default session got %+v", got)
	}

	// A chosen default wins, by name, across restarts of that session.
	if err := st.SetAgentConfig(ctx, "claude", "default_session", "old"); err != nil {
		t.Fatal(err)
	}
	b.Send(ctx, "alice", bus.SendInput{To: "claude", Content: "again"})
	if got := take(t, b, "claude", "new"); len(got) != 0 {
		t.Fatalf("new session took %+v", got)
	}
	if got := take(t, b, "claude", "old"); len(got) != 1 {
		t.Fatalf("chosen session got %+v", got)
	}
}

func TestAnswersGoBackToTheSessionThatAsked(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	now := time.Now()
	report(t, b, "claude", "asker", "asker", bus.TierPush, now.Add(-time.Hour))
	report(t, b, "claude", "listener", "listener", bus.TierPush, now)

	q, err := b.Send(ctx, "claude", bus.SendInput{To: "bot", Content: "question", FromSession: "asker"})
	if err != nil {
		t.Fatal(err)
	}
	a, err := b.Send(ctx, "bot", bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil || a.ToSession != "asker" {
		t.Fatalf("answer = %+v, %v", a, err)
	}
	if got := take(t, b, "claude", "listener"); len(got) != 0 {
		t.Fatalf("the default session took another session's answer: %+v", got)
	}
	if got := take(t, b, "claude", "asker"); len(got) != 1 {
		t.Fatalf("asker got %+v", got)
	}

	// If the asker has gone, its answer waits for it rather than falling to
	// the default session, which never saw the question.
	b.Send(ctx, "bot", bus.SendInput{InReplyTo: q.ID, Content: "one more thing"})
	if err := b.EndSession(ctx, "claude", "asker"); err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "claude", "listener"); len(got) != 0 {
		t.Fatalf("the default session took a closed session's answer: %+v", got)
	}
	// The pin delivers it, not being the default: listener stays the default.
	if err := st.SetAgentConfig(ctx, "claude", "default_session", "listener"); err != nil {
		t.Fatal(err)
	}
	report(t, b, "claude", "asker", "asker", bus.TierPush, time.Now())
	if got := take(t, b, "claude", "asker"); len(got) != 1 {
		t.Fatalf("the asker came back and got %+v", got)
	}
}

// An answer sent after its session closed must not reach a session opened
// since, however recent: issue #1.
func TestAnswerToAClosedSessionWaitsForIt(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "eva", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	report(t, b, "claude", "oneshot", "oneshot", bus.TierPush, time.Now())

	q, err := b.Send(ctx, "claude", bus.SendInput{To: "eva", Content: "question", FromSession: "oneshot"})
	if err != nil {
		t.Fatal(err)
	}
	if err := b.EndSession(ctx, "claude", "oneshot"); err != nil {
		t.Fatal(err)
	}
	a, err := b.Send(ctx, "eva", bus.SendInput{InReplyTo: q.ID, Content: "go ahead with that"})
	if err != nil || a.ToSession != "oneshot" {
		t.Fatalf("answer = %+v, %v", a, err)
	}
	if !strings.Contains(a.Note, "oneshot is no longer open") || !strings.Contains(a.Note, "unless one waits on this thread") {
		t.Fatalf("note = %q", a.Note)
	}

	report(t, b, "claude", "fresh", "fresh", bus.TierPush, time.Now())
	if n, err := b.Unread(ctx, bus.ReceiveInput{Agent: "claude", Session: "fresh"}); err != nil || n != 0 {
		t.Fatalf("unread in the new session = %d, %v", n, err)
	}
	if got := take(t, b, "claude", "fresh"); len(got) != 0 {
		t.Fatalf("the new default session took the answer: %+v", got)
	}
	// New conversations still go to the default.
	if _, err := b.Send(ctx, "eva", bus.SendInput{To: "claude", Content: "new topic"}); err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "claude", "fresh"); len(got) != 1 || got[0].Content != "new topic" {
		t.Fatalf("default session got %+v", got)
	}

	// Waiting on the thread names it, so that can still collect the answer.
	got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "fresh", Thread: q.ThreadID, Lease: time.Minute})
	if err != nil || len(got) != 1 || got[0].ID != a.ID {
		t.Fatalf("thread wait got %+v, %v", got, err)
	}
}

// A session that stops reporting without closing counts as gone too.
func TestAnswerToALapsedSessionWaitsForIt(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "eva", nil)
	addAgent(t, st, "claude", nil)
	now := time.Now()
	report(t, b, "claude", "asker", "asker", bus.TierPush, now)
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "eva", Content: "question", FromSession: "asker"})
	if err != nil {
		t.Fatal(err)
	}
	old := now.Add(-2 * bus.SessionTTL)
	if err := st.UpsertSession(ctx, bus.Session{Agent: "claude", ID: "asker", Name: "asker", Tier: bus.TierPush, ActiveAt: old, LastSeen: old}); err != nil {
		t.Fatal(err)
	}
	report(t, b, "claude", "fresh", "fresh", bus.TierPush, now)
	a, err := b.Send(ctx, "eva", bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil || !strings.Contains(a.Note, "no longer open") {
		t.Fatalf("answer = %+v, %v", a, err)
	}
	if got := take(t, b, "claude", "fresh"); len(got) != 0 {
		t.Fatalf("the default session took a lapsed session's answer: %+v", got)
	}
	if err := st.SetAgentConfig(ctx, "claude", "default_session", "fresh"); err != nil {
		t.Fatal(err)
	}
	report(t, b, "claude", "asker", "asker", bus.TierPush, time.Now())
	if got := take(t, b, "claude", "asker"); len(got) != 1 {
		t.Fatalf("the asker came back and got %+v", got)
	}
}

// A follow-up that names only the thread goes where an answer would.
func TestThreadFollowUpsGoToTheSessionInTheThread(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	now := time.Now()
	report(t, b, "claude", "asker", "asker", bus.TierPush, now.Add(-time.Hour))
	report(t, b, "claude", "listener", "listener", bus.TierPush, now)

	q, err := b.Send(ctx, "claude", bus.SendInput{To: "bot", Content: "question", FromSession: "asker"})
	if err != nil {
		t.Fatal(err)
	}
	f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: q.ThreadID, Content: "and another thing"})
	if err != nil || f.ToSession != "asker" {
		t.Fatalf("follow-up = %+v, %v", f, err)
	}
	if got := take(t, b, "claude", "listener"); len(got) != 0 {
		t.Fatalf("the default session took a follow-up: %+v", got)
	}
	if got := take(t, b, "claude", "asker"); len(got) != 1 {
		t.Fatalf("asker got %+v", got)
	}

	// A thread no session of claude has been in yet is still a new conversation.
	n, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_custom", Content: "hello"})
	if err != nil || n.ToSession != "" {
		t.Fatalf("new thread = %+v, %v", n, err)
	}
	if got := take(t, b, "claude", "listener"); len(got) != 1 {
		t.Fatalf("default session got %+v", got)
	}
}

func TestSessionsSayWhatTheyAreDoing(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	for id, in := range map[string]bus.SessionInput{
		"s1": {Name: "api", State: bus.StateWaiting, Branch: "fix/login"},
		"s2": {Name: "web", State: "dreaming", Branch: "bad\x1b[31m"},
	} {
		if err := b.ReportSession(ctx, "claude", id, in); err != nil {
			t.Fatal(err)
		}
	}
	agents, err := b.Agents(ctx)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bus.Session{}
	for _, s := range agents[0].Sessions {
		got[s.ID] = s
	}
	if s := got["s1"]; s.State != bus.StateWaiting || s.Branch != "fix/login" {
		t.Fatalf("s1 = %+v", s)
	}
	// What this version can't use is dropped, not refused.
	if s := got["s2"]; s.State != "" || s.Branch != "" {
		t.Fatalf("s2 = %+v", s)
	}
}

func TestClosedSessionsAreListedAfterRunningOnes(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	now := time.Now()
	report(t, b, "claude", "old", "old", bus.TierPush, now.Add(-time.Hour))
	report(t, b, "claude", "gone", "gone", bus.TierPush, now)
	report(t, b, "claude", "new", "new", bus.TierPush, now.Add(-time.Minute))
	if err := b.EndSession(ctx, "claude", "gone"); err != nil {
		t.Fatal(err)
	}

	a, _ := st.GetAgent(ctx, "claude")
	live, err := b.Sessions(ctx, a)
	if err != nil || len(live) != 2 {
		t.Fatalf("live = %+v, %v", live, err)
	}
	recent, err := b.RecentSessions(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	var order []string
	for _, s := range recent {
		order = append(order, s.ID)
	}
	if strings.Join(order, " ") != "new old gone" {
		t.Fatalf("order = %v", order)
	}
	if !recent[0].Default || recent[2].Live(time.Now()) || recent[2].EndedAt.IsZero() {
		t.Fatalf("recent = %+v", recent)
	}
}

func TestAddressingOneSession(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	now := time.Now()
	report(t, b, "claude", "s1", "api", bus.TierPush, now)
	report(t, b, "claude", "s2", "web", bus.TierPush, now.Add(-time.Minute))
	report(t, b, "claude", "s3", "web", bus.TierPush, now.Add(-2*time.Minute))
	report(t, b, "claude", "s4", "docs", bus.TierPush, now)
	if err := b.EndSession(ctx, "claude", "s4"); err != nil {
		t.Fatal(err)
	}
	// A session named like another's id: the id wins.
	report(t, b, "claude", "s5", "s1", bus.TierPush, now)
	send := func(to string) (bus.Message, error) {
		return b.Send(ctx, "bot", bus.SendInput{To: to, Content: "x"})
	}
	code := func(err error) string {
		if e, ok := bus.AsAddressError(err); ok {
			return e.Code
		}
		return ""
	}

	for to, want := range map[string]string{"claude/api": "s1", "claude/s3": "s3", "claude/web#s3": "s3", "claude/s1": "s1", "claude/s1#s5": "s5"} {
		m, err := send(to)
		if err != nil || m.To != "claude" || m.ToSession != want || len(m.OtherSessions) != 0 {
			t.Fatalf("%s: m = %+v, %v; want session %s", to, m, err, want)
		}
	}

	// A name two running sessions share goes to the most recently active,
	// and the sender hears about the other.
	m, err := send("claude/web")
	if err != nil || m.ToSession != "s2" {
		t.Fatalf("shared name: m = %+v, %v", m, err)
	}
	if len(m.OtherSessions) != 1 || m.OtherSessions[0].ID != "s3" || m.OtherSessions[0].Address != "claude/web#s3" || !strings.Contains(m.Note, "claude/web#s3") {
		t.Fatalf("shared name: others = %+v, note %q", m.OtherSessions, m.Note)
	}

	for to, want := range map[string]string{
		"claude/none":    bus.RecipientNotFound,
		"claude/api#s2":  bus.RecipientNotFound, // name and id don't match
		"claude/docs":    bus.RecipientSessionEnded,
		"claude/s4":      bus.RecipientSessionEnded,
		"claude/docs#s4": bus.RecipientSessionEnded,
	} {
		_, err := send(to)
		if code(err) != want || !errors.Is(err, bus.ErrNotFound) {
			t.Fatalf("%s: err = %v; want %s", to, err, want)
		}
		e, _ := bus.AsAddressError(err)
		if len(e.Live) != 4 || !strings.Contains(err.Error(), "claude/api#s1") {
			t.Fatalf("%s: running sessions = %+v in %v", to, e.Live, err)
		}
	}
}

// claude doesn't pick one of its sessions for a message addressed to claude
// alone: each is its own conversation, and a message in the wrong one
// interrupts it. The sender is told which sessions are running.
func TestAMessageForClaudeAloneNeedsASession(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	addAgent(t, st, "codex", nil)
	send := func(to string) (bus.Message, error) {
		return b.Send(ctx, "bot", bus.SendInput{To: to, Content: "x"})
	}

	_, err := send("claude")
	if e, ok := bus.AsAddressError(err); !ok || e.Code != bus.SessionRequired || !errors.Is(err, bus.ErrInvalid) || len(e.Live) != 0 {
		t.Fatalf("no sessions: err = %v", err)
	}

	now := time.Now()
	report(t, b, "claude", "s1", "api", bus.TierPush, now.Add(-time.Minute))
	report(t, b, "claude", "s2", "web", bus.TierPush, now)
	_, err = send("claude")
	e, ok := bus.AsAddressError(err)
	if !ok || e.Code != bus.SessionRequired || len(e.Live) != 2 || e.Live[0].Address != "claude/web#s2" {
		t.Fatalf("two sessions: err = %v", err)
	}
	if !strings.Contains(err.Error(), "claude/api#s1") || !strings.Contains(err.Error(), "claude/web#s2") {
		t.Fatalf("two sessions: err = %v", err)
	}
	if got := take(t, b, "claude", "s2"); len(got) != 0 {
		t.Fatalf("refused message was queued: %+v", got)
	}

	// A session the user chose takes them.
	if err := st.SetAgentConfig(ctx, "claude", "default_session", "api"); err != nil {
		t.Fatal(err)
	}
	m, err := send("claude")
	if err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "claude", "s2"); len(got) != 0 {
		t.Fatalf("the session that wasn't chosen got %+v", got)
	}
	if got := take(t, b, "claude", "s1"); len(got) != 1 || got[0].ID != m.ID {
		t.Fatalf("the chosen session got %+v", got)
	}
	// Not while it isn't running.
	if err := b.EndSession(ctx, "claude", "s1"); err != nil {
		t.Fatal(err)
	}
	if _, err := send("claude"); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("chosen session closed: err = %v", err)
	}

	// Other agents still pick one, and claude can be told to.
	report(t, b, "codex", "c1", "repo", bus.TierPush, now)
	report(t, b, "codex", "c2", "repo2", bus.TierPush, now)
	if _, err := send("codex"); err != nil {
		t.Fatalf("codex: %v", err)
	}
	picksSession(t, st, "claude")
	if _, err := send("claude"); err != nil {
		t.Fatalf("claude with fallback on: %v", err)
	}
}

func TestClaimedMessagesComeBackUnlessConfirmed(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
	claim := func(lease time.Duration) []bus.Message {
		msgs, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "s1", Lease: lease})
		if err != nil {
			t.Fatal(err)
		}
		return msgs
	}
	m, _ := b.Send(ctx, "bot", bus.SendInput{To: "claude", Content: "x"})

	if got := claim(time.Millisecond); len(got) != 1 || got[0].Status != bus.StatusClaimed {
		t.Fatalf("first claim = %+v", got)
	}
	time.Sleep(5 * time.Millisecond)
	if got := claim(time.Minute); len(got) != 1 {
		t.Fatalf("an unconfirmed message should come back after its lease: %+v", got)
	}
	if got := claim(time.Minute); len(got) != 0 {
		t.Fatalf("a message under lease was taken twice: %+v", got)
	}
	if err := b.Nack(ctx, "claude", m.ID); err != nil {
		t.Fatal(err)
	}
	if got := claim(time.Minute); len(got) != 1 {
		t.Fatalf("a returned message should be available again: %+v", got)
	}
	if err := b.Ack(ctx, "claude", m.ID); err != nil {
		t.Fatal(err)
	}
	got, _ := st.GetMessage(ctx, m.ID)
	if got.Status != bus.StatusDelivered {
		t.Fatalf("status = %q", got.Status)
	}
	if err := b.Nack(ctx, "claude", m.ID); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetMessage(ctx, m.ID); got.Status != bus.StatusDelivered {
		t.Fatalf("a confirmed message went back to %q", got.Status)
	}
	// Answering records that too, and only the recipient's answer counts.
	if _, err := b.Send(ctx, "bot", bus.SendInput{InReplyTo: m.ID, Content: "still there?"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetMessage(ctx, m.ID); got.Status != bus.StatusDelivered {
		t.Fatalf("the sender's own follow-up made it %q", got.Status)
	}
	if _, err := b.Send(ctx, "claude", bus.SendInput{InReplyTo: m.ID, Content: "yes"}); err != nil {
		t.Fatal(err)
	}
	if got, _ := st.GetMessage(ctx, m.ID); got.Status != bus.StatusReplied {
		t.Fatalf("status after answering = %q", got.Status)
	}
}

func TestLoopLimit(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "a", nil)
	addAgent(t, st, "b", nil)
	b.LoopLimit = 3
	for i := range 3 {
		if _, err := b.Send(ctx, "a", bus.SendInput{To: "b", Content: "x"}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if _, err := b.Send(ctx, "a", bus.SendInput{To: "b", Content: "x"}); !errors.Is(err, bus.ErrLoop) {
		t.Fatalf("err = %v, want ErrLoop", err)
	}
	if _, err := b.Send(ctx, "b", bus.SendInput{To: "a", Content: "x"}); err != nil {
		t.Fatalf("the other direction has its own count: %v", err)
	}
	b.LoopLimit = -1
	if _, err := b.Send(ctx, "a", bus.SendInput{To: "b", Content: "x"}); err != nil {
		t.Fatalf("limit off: %v", err)
	}
}

type fakeDeliverer struct {
	kind string
	err  error
	got  chan bus.Message
}

func (f *fakeDeliverer) Kind() string { return f.kind }
func (f *fakeDeliverer) Deliver(_ context.Context, _ bus.Agent, m bus.Message) error {
	f.got <- m
	return f.err
}

func waitStatus(t *testing.T, st interface {
	GetMessage(context.Context, string) (bus.Message, error)
}, id, want string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		m, _ := st.GetMessage(context.Background(), id)
		if m.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("status = %q, want %q", m.Status, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestSenderHearsWhenDeliveryGivesUp(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	d := &fakeDeliverer{kind: "webhook", err: errors.New("HTTP 500"), got: make(chan bus.Message, 1)}
	b, st := newBus(t, d)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "down", map[string]string{"url": "http://x"})
	report(t, b, "alice", "s1", "api", bus.TierPush, time.Now())

	m, err := b.Send(ctx, "alice", bus.SendInput{To: "down", Content: "x", FromSession: "s1"})
	if err != nil {
		t.Fatal(err)
	}
	// Seven failures already, so the next attempt is the last.
	for range 7 {
		if err := st.RecordFailure(ctx, m.ID, "HTTP 500", time.Time{}, false); err != nil {
			t.Fatal(err)
		}
	}
	go b.RunDelivery(ctx)
	got, err := b.Receive(ctx, "alice", m.ThreadID, 10*time.Second)
	if err != nil || len(got) != 1 {
		t.Fatalf("got %+v, %v", got, err)
	}
	n := got[0]
	if n.From != bus.SystemSender || n.InReplyTo != m.ID || n.ToSession != "s1" || !strings.Contains(n.Content, "HTTP 500") {
		t.Fatalf("notice = %+v", n)
	}
	if _, err := b.Send(ctx, "alice", bus.SendInput{InReplyTo: n.ID, Content: "thanks"}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("answering a notice: err = %v", err)
	}
}

func TestAcceptedWorkRunsAgainAfterRestart(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	d := &fakeDeliverer{kind: "webhook", err: bus.ErrAccepted, got: make(chan bus.Message, 2)}
	b, st := newBus(t, d)
	addAgent(t, st, "alice", nil)
	addAgent(t, st, "slow", map[string]string{"url": "http://x"})
	m, err := b.Send(ctx, "alice", bus.SendInput{To: "slow", Content: "x"})
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { b.RunDelivery(ctx); close(done) }()
	<-d.got
	waitStatus(t, st, m.ID, bus.StatusClaimed)
	cancel()
	<-done

	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	go b.RunDelivery(ctx)
	select {
	case again := <-d.got:
		if again.ID != m.ID {
			t.Fatalf("redelivered %s", again.ID)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("unfinished work wasn't picked up after the restart")
	}
	waitStatus(t, st, m.ID, bus.StatusClaimed)
	if err := b.Complete(ctx, "slow", m.ID); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, st, m.ID, bus.StatusDelivered)
}

// A follow-up goes to the session that took the last message in the thread,
// even if it hasn't answered yet, and to the one that collected a held answer.
func TestThreadFollowUpsGoToTheSessionThatTookTheLastMessage(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	now := time.Now()
	report(t, b, "claude", "first", "first", bus.TierPush, now)

	m, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_t", Content: "start"})
	if err != nil || m.ToSession != "" {
		t.Fatalf("m = %+v, %v", m, err)
	}
	if got := take(t, b, "claude", "first"); len(got) != 1 {
		t.Fatalf("first got %+v", got)
	}
	report(t, b, "claude", "second", "second", bus.TierPush, now.Add(time.Second))
	f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_t", Content: "more"})
	if err != nil || f.ToSession != "first" {
		t.Fatalf("follow-up = %+v, %v", f, err)
	}

	// first closes; second collects the follow-up by naming the thread, and
	// becomes the session in it.
	if err := b.EndSession(ctx, "claude", "first"); err != nil {
		t.Fatal(err)
	}
	got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "claude", Session: "second", Thread: "thr_t", Lease: time.Minute})
	if err != nil || len(got) != 1 {
		t.Fatalf("thread wait got %+v, %v", got, err)
	}
	g, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_t", Content: "and more"})
	if err != nil || g.ToSession != "second" {
		t.Fatalf("follow-up after collecting = %+v, %v", g, err)
	}
}

// A stand-in id reported before the real one moves everything pinned to it.
func TestReplacingAStandInMovesItsMessages(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	report(t, b, "claude", "proc-1", "api", bus.TierPush, time.Now())
	q, err := b.Send(ctx, "claude", bus.SendInput{To: "bot", Content: "question", FromSession: "proc-1"})
	if err != nil {
		t.Fatal(err)
	}
	direct, err := b.Send(ctx, "bot", bus.SendInput{To: "claude/api", Content: "for you"})
	if err != nil || direct.ToSession != "proc-1" {
		t.Fatalf("direct = %+v, %v", direct, err)
	}
	if err := b.ReportSession(ctx, "claude", "real", bus.SessionInput{Name: "api", Tier: bus.TierPush, Replaces: "proc-1"}); err != nil {
		t.Fatal(err)
	}
	a, err := b.Send(ctx, "bot", bus.SendInput{InReplyTo: q.ID, Content: "answer"})
	if err != nil || a.ToSession != "real" {
		t.Fatalf("answer = %+v, %v", a, err)
	}
	if got := take(t, b, "claude", "real"); len(got) != 2 {
		t.Fatalf("real got %+v", got)
	}
	agent, _ := st.GetAgent(ctx, "claude")
	if live, _ := b.Sessions(ctx, agent); len(live) != 1 || live[0].ID != "real" {
		t.Fatalf("live = %+v", live)
	}
	if err := b.ReportSession(ctx, "claude", "x", bus.SessionInput{Replaces: "bad id"}); !errors.Is(err, bus.ErrInvalid) {
		t.Fatalf("bad replaces: err = %v", err)
	}
}

// A session that took a message and gave it back hasn't taken part.
func TestAMessageGivenBackDoesntMakeASessionPartOfTheThread(t *testing.T) {
	ctx := context.Background()
	b, st := newBus(t)
	addAgent(t, st, "bot", nil)
	addAgent(t, st, "claude", nil)
	picksSession(t, st, "claude")
	now := time.Now()
	report(t, b, "claude", "first", "first", bus.TierPush, now)
	m, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_n", Content: "start"})
	if err != nil {
		t.Fatal(err)
	}
	if got := take(t, b, "claude", "first"); len(got) != 1 {
		t.Fatalf("first got %+v", got)
	}
	if err := b.Nack(ctx, "claude", m.ID); err != nil {
		t.Fatal(err)
	}
	f, err := b.Send(ctx, "bot", bus.SendInput{To: "claude", ThreadID: "thr_n", Content: "more"})
	if err != nil || f.ToSession != "" {
		t.Fatalf("follow-up = %+v, %v", f, err)
	}
}
