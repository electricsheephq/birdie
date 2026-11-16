package bus

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
)

const (
	// MaxRequestBytes bounds one request to the bus, so a runaway sender
	// can't exhaust its memory. Messages themselves have no size limit; this
	// is far beyond any an agent writes.
	MaxRequestBytes = 256 << 20
	// MaxBatchBytes bounds the content one inbox claim returns, so a batch of
	// long messages can't fill memory. The first message is always taken,
	// however long.
	MaxBatchBytes  = 16 << 20
	MaxWait        = 2 * time.Minute
	inboxBatch     = 50
	maxAttempts    = 8
	deliverTimeout = 30 * time.Second
	sweepInterval  = 5 * time.Second

	// SessionTTL is how long a session counts as running after it last
	// reported. Sessions report every SessionBeat.
	SessionTTL  = 90 * time.Second
	SessionBeat = 30 * time.Second
	// SessionRetention is how long a session is listed after it stops.
	SessionRetention = 24 * time.Hour
	// DefaultHoldFor is how long a message waits for a session that stopped
	// to come back, before it expires: counted from when the session stopped,
	// or from the send if that was later.
	DefaultHoldFor = 7 * 24 * time.Hour
	// expireEvery is how often the bus looks for held messages to expire.
	expireEvery = time.Minute
	// SessionKeep is how long a session is remembered after it stops, so a
	// message addressed to its id is refused rather than given to another
	// session with that name.
	SessionKeep    = 30 * 24 * time.Hour
	maxBranchBytes = 255

	// DefaultLoopLimit messages from one agent to another within LoopWindow
	// probably means they are stuck answering each other.
	DefaultLoopLimit = 12
	LoopWindow       = 10 * time.Minute

	noticeKey = "notice"
)

type Bus struct {
	store      Store
	deliverers map[string]Deliverer
	log        *slog.Logger
	// LoopLimit overrides DefaultLoopLimit; negative turns the check off.
	LoopLimit int
	// HoldFor overrides DefaultHoldFor.
	HoldFor time.Duration
	// QuietFor overrides how long the bus listens, after it starts or the
	// machine wakes, before it takes a session it hasn't heard from as
	// stopped or expires anything: SessionTTL, long enough for every running
	// session to report. Negative means not at all.
	QuietFor  time.Duration
	listening atomic.Int64 // since when, in Unix ms
	// heard is when birdie last heard from any session before it started
	// listening, in Unix ms (0 if never). Taken as listening starts, before
	// sessions report again and overwrite their last_seen.
	heard      atomic.Int64
	lastExpire time.Time

	mu      sync.Mutex
	waiters map[string]map[chan struct{}]struct{}
	kick    chan struct{}
	watch   watchers
}

func New(s Store, log *slog.Logger, ds ...Deliverer) *Bus {
	b := &Bus{
		store:      s,
		deliverers: make(map[string]Deliverer),
		log:        log,
		waiters:    make(map[string]map[chan struct{}]struct{}),
		kick:       make(chan struct{}, 1),
	}
	now := time.Now()
	b.listening.Store(now.UnixMilli())
	// Nothing can have reported to this bus yet.
	if t, err := s.LastSeenBefore(context.Background(), now); err == nil && !t.IsZero() {
		b.heard.Store(t.UnixMilli())
	}
	for _, d := range ds {
		b.deliverers[d.Kind()] = d
	}
	return b
}

func HashToken(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:])
}

func NewToken() string { return "ab_" + randHex(32) }

func randHex(n int) string {
	b := make([]byte, n)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func (b *Bus) Authenticate(ctx context.Context, token string) (Agent, error) {
	if token == "" {
		return Agent{}, ErrUnauthorized
	}
	a, err := b.store.AgentByTokenHash(ctx, HashToken(token))
	if errors.Is(err, ErrNotFound) {
		return Agent{}, ErrUnauthorized
	}
	return a, err
}

// Agents lists every agent with its running sessions.
func (b *Bus) Agents(ctx context.Context) ([]Agent, error) {
	return b.listAgents(ctx, b.Sessions)
}

// AgentsWithRecentSessions lists every agent with its sessions from the last
// day, as RecentSessions returns them.
func (b *Bus) AgentsWithRecentSessions(ctx context.Context) ([]Agent, error) {
	return b.listAgents(ctx, b.RecentSessions)
}

func (b *Bus) listAgents(ctx context.Context, sessions func(context.Context, Agent) ([]Session, error)) ([]Agent, error) {
	agents, err := b.store.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	for i := range agents {
		if agents[i].Delivery != DeliveryPull {
			continue
		}
		if agents[i].Sessions, err = sessions(ctx, agents[i]); err != nil {
			return nil, err
		}
	}
	return agents, nil
}

// Sessions returns the agent's running sessions, with its default marked.
func (b *Bus) Sessions(ctx context.Context, a Agent) ([]Session, error) {
	live, err := b.store.LiveSessions(ctx, a.Name, time.Now().Add(-SessionTTL))
	if err != nil {
		return nil, err
	}
	if d := defaultSession(live, a); d != nil {
		d.Default = true
	}
	for i := range live {
		live[i].Address = addressOf(a, live[i])
	}
	return live, nil
}

// RecentSessions returns the agent's sessions from the last day: the running
// ones as Sessions returns them, then the ones that stopped, latest first.
func (b *Bus) RecentSessions(ctx context.Context, a Agent) ([]Session, error) {
	now := time.Now()
	all, err := b.store.RecentSessions(ctx, a.Name, now.Add(-SessionRetention))
	if err != nil {
		return nil, err
	}
	var live, stopped []Session
	for _, s := range all {
		if s.Live(now) {
			live = append(live, s)
		} else {
			stopped = append(stopped, s)
		}
	}
	if d := defaultSession(live, a); d != nil {
		d.Default = true
	}
	slices.SortStableFunc(stopped, func(x, y Session) int { return y.StoppedAt().Compare(x.StoppedAt()) })
	all = append(live, stopped...)
	for i := range all {
		all[i].Address = addressOf(a, all[i])
	}
	return all, nil
}

// defaultSession picks the session of a that takes new conversations: the
// one with the name the user chose (the most recently active, if several
// have it), else, if the agent picks one itself (see
// PicksSession), the one that can see messages soonest, most recently active
// first. It returns nil if there are none.
func defaultSession(live []Session, a Agent) *Session {
	chosen := a.Config["default_session"]
	auto := PicksSession(a)
	var best *Session
	for i := range live {
		s := &live[i]
		if chosen != "" && s.Name == chosen && (best == nil || s.ActiveAt.After(best.ActiveAt)) {
			best = s
		}
	}
	if best != nil || !auto {
		return best
	}
	for i := range live {
		s := &live[i]
		if best == nil || tierRank(s.Tier) > tierRank(best.Tier) ||
			(tierRank(s.Tier) == tierRank(best.Tier) && s.ActiveAt.After(best.ActiveAt)) {
			best = s
		}
	}
	return best
}

// noAutoDefault lists agents that, unless told otherwise, don't pick a session
// for a message addressed to the agent alone. Their sessions are unrelated
// conversations, often many at once, and a message landing in the wrong one
// interrupts it and costs it context.
var noAutoDefault = map[string]bool{"claude": true}

// PicksSession reports whether a message addressed to agent a alone, not to
// one of its sessions, may go to the session a picks itself: the most
// recently active one. Config["fallback"] "on" or "off" says; the default is
// on, except for the agents in noAutoDefault. A session the user chose with
// `birdie session default` takes such messages either way.
func PicksSession(a Agent) bool {
	switch a.Config["fallback"] {
	case "on":
		return true
	case "off":
		return false
	}
	return !noAutoDefault[a.Name]
}

func tierRank(tier string) int {
	switch tier {
	case TierPush:
		return 2
	case TierTurn:
		return 1
	}
	return 0
}

// ReportSession records that a session is running.
func (b *Bus) ReportSession(ctx context.Context, agent, id string, in SessionInput) error {
	if !sessionPattern.MatchString(id) {
		return fmt.Errorf("%w: bad session id %q", ErrInvalid, id)
	}
	if in.Name != "" && !sessionPattern.MatchString(in.Name) {
		return fmt.Errorf("%w: bad session name %q", ErrInvalid, in.Name)
	}
	if in.Replaces != "" && !sessionPattern.MatchString(in.Replaces) {
		return fmt.Errorf("%w: bad session id %q", ErrInvalid, in.Replaces)
	}
	switch in.Tier {
	case TierPush, TierTurn, TierPull:
	case "":
		in.Tier = TierPull
	default:
		return fmt.Errorf("%w: bad tier %q", ErrInvalid, in.Tier)
	}
	// What a session says about itself only informs people, so anything this
	// version can't use is dropped rather than refused.
	switch in.State {
	case StateBusy, StateIdle, StateWaiting:
	default:
		in.State = ""
	}
	if len(in.Branch) > maxBranchBytes || strings.ContainsFunc(in.Branch, unicode.IsControl) {
		in.Branch = ""
	}
	now := time.Now().UTC()
	s := Session{ID: id, Agent: agent, Name: in.Name, Cwd: in.Cwd, Branch: in.Branch, Tier: in.Tier, State: in.State, ActiveAt: in.ActiveAt, LastSeen: now}
	if s.ActiveAt.IsZero() || s.ActiveAt.After(now) {
		s.ActiveAt = now
	}
	if err := b.store.UpsertSession(ctx, s); err != nil {
		return err
	}
	if in.Replaces != "" && in.Replaces != id {
		if err := b.store.MoveSession(ctx, agent, in.Replaces, id); err != nil {
			return err
		}
		if err := b.store.EndSession(ctx, agent, in.Replaces, now); err != nil {
			return err
		}
		b.emit(Event{Kind: "session", Type: "ended", Agent: agent, Session: in.Replaces})
	}
	b.notify(agent)
	b.emit(Event{Kind: "session", Type: "reported", Agent: agent, Session: id, Name: s.Name, State: s.State})
	return nil
}

// EndSession records that a session closed, so new conversations go to the
// default session without delay. Messages pinned to it wait for it to come
// back; see claimFilter.
func (b *Bus) EndSession(ctx context.Context, agent, id string) error {
	now := time.Now().UTC()
	if err := b.store.EndSession(ctx, agent, id, now); err != nil {
		return err
	}
	b.notify(agent)
	b.emit(Event{Kind: "session", Type: "ended", Agent: agent, Session: id})
	// Tell whoever sent what it hadn't taken yet: it waits for the session to
	// come back, and isn't given to another one.
	b.noticeHeld(ctx, false)
	return nil
}

func (b *Bus) holdFor() time.Duration {
	if b.HoldFor > 0 {
		return b.HoldFor
	}
	return DefaultHoldFor
}

// days says d in whole days when it is some, else as Go writes it.
func days(d time.Duration) string {
	if d >= 24*time.Hour && d%(24*time.Hour) == 0 {
		n := int(d / (24 * time.Hour))
		if n == 1 {
			return "1 day"
		}
		return fmt.Sprintf("%d days", n)
	}
	return d.String()
}

// woke starts listening again after a sleep: sessions were last heard
// before it, up to last, when the bus last ran.
func (b *Bus) woke(last, now time.Time) {
	b.heard.Store(last.UnixMilli())
	b.listening.Store(now.UnixMilli())
	b.lastExpire = time.Time{}
}

// quiet reports whether the bus has listened for too short a time, since it
// started or the machine woke, to tell that a session it hasn't heard from
// has stopped.
func (b *Bus) quiet(now time.Time) bool {
	q := b.QuietFor
	if q == 0 {
		q = SessionTTL
	}
	return q > 0 && now.UnixMilli()-b.listening.Load() < q.Milliseconds()
}

// noticeHeld tells the senders of messages held for a session that isn't
// running, once each, that they are held. Only the sweep, which notices a
// wake before it looks, passes lapsed: then sessions that went silent count
// too, not only those that closed.
func (b *Bus) noticeHeld(ctx context.Context, lapsed bool) {
	now := time.Now()
	b.settleHeld(ctx, HoldQuery{Lapsed: lapsed && !b.quiet(now), LiveSince: now.Add(-SessionTTL), Now: now, Limit: inboxBatch})
}

// expireHeld expires messages held longer than HoldFor for a session that
// isn't running, and tells their senders.
func (b *Bus) expireHeld(ctx context.Context) {
	now := time.Now()
	if b.quiet(now) {
		return
	}
	b.settleHeld(ctx, HoldQuery{Expire: true, Lapsed: true, LiveSince: now.Add(-SessionTTL), Before: now.Add(-b.holdFor()), Now: now, Limit: inboxBatch})
}

func (b *Bus) settleHeld(ctx context.Context, q HoldQuery) {
	agents := map[string]Agent{}
	if list, err := b.store.ListAgents(ctx); err == nil {
		for _, a := range list {
			agents[a.Name] = a
		}
	}
	for {
		held, notices, err := b.store.SettleHeld(ctx, q, func(held []HeldMessage) []Message {
			return heldNotices(held, q.Expire, agents, b.holdFor(), q.Now)
		})
		if err != nil {
			if ctx.Err() == nil {
				b.log.Error("settle held messages", "expire", q.Expire, "err", err)
			}
			return
		}
		if q.Expire {
			for _, m := range held {
				b.emitMessage(StatusExpired, m.Message, "", "")
			}
		}
		for _, n := range notices {
			b.emitMessage(StatusQueued, n, "", "")
			b.notify(n.To)
		}
		if len(notices) > 0 {
			select {
			case b.kick <- struct{}{}:
			default:
			}
		}
		if len(held) < q.Limit {
			return
		}
	}
}

// plural is one for a single message, else many.
func plural(n int, one, many string) string {
	if n > 1 {
		return many
	}
	return one
}

// heldNotices tells each sender about its messages in held, one notice per
// thread and pair of sessions, naming every message.
func heldNotices(held []HeldMessage, expired bool, agents map[string]Agent, hold time.Duration, now time.Time) []Message {
	type key struct{ thread, from, fromSession, to, toSession string }
	var order []key
	groups := map[key][]HeldMessage{}
	for _, h := range held {
		k := key{h.ThreadID, h.From, h.FromSession, h.To, h.ToSession}
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], h)
	}
	kind := "held"
	if expired {
		kind = "expired"
	}
	var out []Message
	for _, k := range order {
		g := groups[k]
		ids := make([]string, len(g))
		for i, h := range g {
			ids[i] = h.ID
		}
		to, ok := agents[k.to]
		if !ok {
			to = Agent{Name: k.to}
		}
		label := to.DisplayName() + " · " + k.toSession
		if g[0].SessionName != "" {
			label = to.DisplayName() + " · " + g[0].SessionName
		}
		subject := plural(len(ids), "your message ", "your messages ") + strings.Join(ids, ", ")
		verb := plural(len(ids), "is", "are")
		var text string
		if expired {
			text = fmt.Sprintf("%s: %s to %s expired. That session stopped and didn't come back within %s, so %s never delivered, and no other session got %s. To reach a session that is running, send a new message to its address (list_agents shows them).",
				RecipientSessionEnded, subject, label, days(hold), plural(len(ids), "it was", "they were"), plural(len(ids), "it", "them"))
		} else {
			text = fmt.Sprintf("%s: %s to %s %s held. That session isn't running, so %s for that session to come back, for up to %s from when it stopped or you sent %s, whichever is later; after that %s and you'll be told. No other session gets %s. To reach a session that is running, send a new message to its address (list_agents shows them).",
				RecipientSessionEnded, subject, label, verb, plural(len(ids), "it waits", "they wait"), days(hold), plural(len(ids), "it", "them"), plural(len(ids), "it expires", "they expire"), plural(len(ids), "it", "them"))
		}
		out = append(out, Message{
			ID:        "msg_" + randHex(12),
			ThreadID:  k.thread,
			InReplyTo: ids[0],
			From:      SystemSender,
			To:        k.from,
			ToSession: k.fromSession,
			Content:   text,
			Meta:      map[string]string{noticeKey: kind, "messages": strings.Join(ids, ",")},
			Status:    StatusQueued,
			CreatedAt: now.UTC(),
		})
	}
	return out
}

// Send queues a message from the authenticated agent from. Replies inherit
// the parent's thread (and its other participant when To is empty), and only
// participants may post into an existing thread.
func (b *Bus) Send(ctx context.Context, from string, in SendInput) (Message, error) {
	if strings.TrimSpace(in.Content) == "" {
		return Message{}, fmt.Errorf("%w: content is required", ErrInvalid)
	}

	thread := in.ThreadID
	joins := in.InReplyTo != "" // it answers, or follows up in, a conversation that exists
	var pinned string           // the session the thread says should get this
	var parent Message
	if in.InReplyTo != "" {
		var err error
		parent, err = b.store.GetMessage(ctx, in.InReplyTo)
		if errors.Is(err, ErrNotFound) {
			return Message{}, fmt.Errorf("%w: unknown message %q", ErrNotFound, in.InReplyTo)
		} else if err != nil {
			return Message{}, err
		}
		if parent.From != from && parent.To != from {
			return Message{}, fmt.Errorf("%w: not a participant in message %q", ErrForbidden, in.InReplyTo)
		}
		if thread != "" && thread != parent.ThreadID {
			return Message{}, fmt.Errorf("%w: thread_id does not match in_reply_to", ErrInvalid)
		}
		thread = parent.ThreadID
		if in.To == "" {
			in.To = parent.From
			if parent.From == from {
				in.To = parent.To
			}
		}
		// An answer goes back to the session that asked.
		if in.To == parent.From {
			pinned = parent.FromSession
		} else if in.To == parent.To {
			pinned = parent.ToSession
		}
	} else if thread != "" {
		exists, member, err := b.store.ThreadAccess(ctx, thread, from)
		if err != nil {
			return Message{}, err
		}
		if exists && !member {
			return Message{}, fmt.Errorf("%w: not a participant in thread %q", ErrForbidden, thread)
		}
		joins = exists
	}
	if in.To == "" {
		return Message{}, fmt.Errorf("%w: to is required unless in_reply_to is set", ErrInvalid)
	}
	if IsSystemSender(in.To) {
		return Message{}, fmt.Errorf("%w: %s notices can't be answered", ErrInvalid, SystemSender)
	}
	addr, err := ParseAddress(in.To)
	if err != nil {
		return Message{}, err
	}
	if addr.Node != "" {
		return Message{}, fmt.Errorf("%w: can't reach %q: birdie only connects agents on this machine for now", ErrInvalid, in.To)
	}
	to, err := b.store.GetAgent(ctx, addr.Agent)
	if errors.Is(err, ErrNotFound) {
		return Message{}, fmt.Errorf("%w: unknown agent %q", ErrNotFound, addr.Agent)
	} else if err != nil {
		return Message{}, err
	}
	if err := b.admit(ctx, from, to.Name); err != nil {
		return Message{}, err
	}
	toSession := pinned
	var others []SessionRef // running sessions with the name that were passed over
	name := addr.Session
	if addr.Session != "" {
		s, more, err := b.findSession(ctx, to, addr)
		if err != nil {
			return Message{}, err
		}
		toSession, others = s.ID, more
	} else if in.InReplyTo == "" && thread != "" {
		// A follow-up in a thread goes where an answer would.
		if toSession, err = b.store.ThreadSession(ctx, thread, from, to.Name); err != nil {
			return Message{}, err
		}
	}
	// A new conversation must name a session. One that answers or follows
	// up a conversation the recipient is in, with no session of it there
	// (asked by `birdie send`, or by an agent with no sessions), waits for
	// whoever waits on its thread or reads without being a session; no
	// session is picked for it.
	if joins {
		if _, member, err := b.store.ThreadAccess(ctx, thread, to.Name); err != nil {
			return Message{}, err
		} else if !member {
			joins = false
		}
	}
	if toSession == "" && !joins && to.Delivery == DeliveryPull && !PicksSession(to) {
		s, more, err := b.chosenSession(ctx, to)
		if err != nil {
			return Message{}, err
		}
		toSession, others, name = s.ID, more, s.Name
	}

	id := randHex(12)
	if thread == "" {
		thread = "thr_" + id
	}
	m := Message{
		ID:          "msg_" + id,
		ThreadID:    thread,
		InReplyTo:   in.InReplyTo,
		From:        from,
		To:          to.Name,
		FromSession: in.FromSession,
		ToSession:   toSession,
		Content:     in.Content,
		Meta:        in.Meta,
		Status:      StatusQueued,
		CreatedAt:   time.Now().UTC(),
	}
	// Stored with the message, so a notice can't also tell the sender.
	if m.ToSession != "" && to.Delivery == DeliveryPull && b.stopped(ctx, to.Name, m.ToSession) {
		m.Held = RecipientSessionEnded
	}
	if err := b.store.InsertMessage(ctx, m); err != nil {
		return Message{}, err
	}
	b.emitMessage(StatusQueued, m, "", "")
	if parent.ID != "" {
		if err := b.store.MarkReplied(ctx, parent.ID, from); err != nil {
			b.log.Error("mark replied", "id", parent.ID, "err", err)
		} else if parent.To == from && parent.Status != StatusFailed {
			b.emitMessage(StatusReplied, parent, "", "")
		}
	}
	m.Note = b.deliveryNote(ctx, to, m)
	if len(others) > 0 {
		m.OtherSessions = others
		m.Note += "; " + othersNote(to, name, others)
	}
	if to.Delivery == DeliveryPull {
		b.notify(to.Name)
	} else {
		if _, ok := b.deliverers[to.Delivery]; !ok {
			b.log.Warn("no deliverer registered; message stays queued", "kind", to.Delivery, "to", to.Name)
		}
		select {
		case b.kick <- struct{}{}:
		default:
		}
	}
	return m, nil
}

// Receive claims queued messages for agent, long-polling up to wait when the
// inbox is empty.
func (b *Bus) Receive(ctx context.Context, agent, threadID string, wait time.Duration) ([]Message, error) {
	return b.ReceiveFor(ctx, ReceiveInput{Agent: agent, Thread: threadID, Wait: wait})
}

// ReceiveFor is Receive with session scoping and leases.
func (b *Bus) ReceiveFor(ctx context.Context, in ReceiveInput) ([]Message, error) {
	deadline := time.Now().Add(min(in.Wait, MaxWait))
	for {
		// Subscribe before claiming so a send between the two isn't missed.
		ch := b.subscribe(in.Agent)
		f, err := b.claimFilter(ctx, in)
		var msgs []Message
		if err == nil {
			msgs, err = b.store.ClaimInbox(ctx, f, time.Now())
		}
		if err != nil || len(msgs) > 0 {
			b.unsubscribe(in.Agent, ch)
			if err == nil {
				for _, m := range msgs {
					b.emitMessage(m.Status, m, in.Session, "")
				}
			}
			return msgs, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			b.unsubscribe(in.Agent, ch)
			return []Message{}, nil
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ch:
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			b.unsubscribe(in.Agent, ch)
			return nil, ctx.Err()
		}
		timer.Stop()
		b.unsubscribe(in.Agent, ch)
	}
}

// Unread counts what the receiver would get from ReceiveFor right now.
func (b *Bus) Unread(ctx context.Context, in ReceiveInput) (int, error) {
	f, err := b.claimFilter(ctx, in)
	if err != nil {
		return 0, err
	}
	return b.store.CountInbox(ctx, f, time.Now())
}

func (b *Bus) claimFilter(ctx context.Context, in ReceiveInput) (ClaimFilter, error) {
	now := time.Now()
	f := ClaimFilter{Agent: in.Agent, Thread: in.Thread, Session: in.Session, LiveSince: now.Add(-SessionTTL), Limit: inboxBatch}
	if in.Lease > 0 {
		f.LeaseUntil = now.Add(in.Lease)
	}
	if in.Session == "" {
		// Not a session: it takes only mail that isn't for a running one
		// (see the store), and on a thread it named, mail for a session
		// that has gone.
		f.Orphans = in.Thread != ""
		return f, nil
	}
	// Waiting on a thread names what it wants, so it doesn't need to be the
	// default session, and it may collect an answer whose session has gone.
	if in.Thread != "" {
		f.Unpinned, f.Orphans = true, true
		return f, nil
	}
	// Otherwise only the default takes new conversations. It doesn't take
	// answers meant for a session that has gone: what they answer was said in
	// that session, so they wait for it to come back.
	a, err := b.store.GetAgent(ctx, in.Agent)
	if err != nil {
		return f, err
	}
	live, err := b.store.LiveSessions(ctx, in.Agent, f.LiveSince)
	if err != nil {
		return f, err
	}
	d := defaultSession(live, a)
	f.Unpinned = d != nil && d.ID == in.Session
	return f, nil
}

// Ack confirms the agent got a claimed message.
func (b *Bus) Ack(ctx context.Context, agent, id string) error {
	changed, err := b.store.Settle(ctx, id, agent, true, time.Now())
	if changed {
		if m, err := b.store.GetMessage(ctx, id); err == nil {
			b.emitMessage(StatusDelivered, m, "", "")
		}
	}
	return err
}

// Nack returns a claimed message to the queue because it couldn't be shown.
func (b *Bus) Nack(ctx context.Context, agent, id string) error {
	changed, err := b.store.Settle(ctx, id, agent, false, time.Now())
	if changed {
		b.notify(agent)
		// Given back by a session that has since closed (a /clear while it
		// held the message): it is held now, and its sender is told.
		if m, err := b.store.GetMessage(ctx, id); err == nil && m.ToSession != "" && !b.running(ctx, agent, m.ToSession) {
			b.noticeHeld(ctx, false)
		}
	}
	return err
}

// Complete finishes a message a Deliverer accepted with ErrAccepted.
func (b *Bus) Complete(ctx context.Context, agent, id string) error {
	return b.Ack(ctx, agent, id)
}

// Notice tells the sender of m something went wrong with it. kind keeps it to
// one notice of that kind per thread.
func (b *Bus) Notice(ctx context.Context, m Message, kind, text string) error {
	if IsSystemSender(m.From) {
		return nil
	}
	if msgs, err := b.store.Thread(ctx, m.ThreadID, m.From); err == nil {
		for _, old := range msgs {
			if IsSystemSender(old.From) && old.Meta[noticeKey] == kind {
				return nil
			}
		}
	}
	id := randHex(12)
	n := Message{
		ID:        "msg_" + id,
		ThreadID:  m.ThreadID,
		InReplyTo: m.ID,
		From:      SystemSender,
		To:        m.From,
		ToSession: m.FromSession,
		Content:   text,
		Meta:      map[string]string{noticeKey: kind},
		Status:    StatusQueued,
		CreatedAt: time.Now().UTC(),
	}
	if err := b.store.InsertMessage(ctx, n); err != nil {
		return err
	}
	b.emitMessage(StatusQueued, n, "", "")
	b.notify(m.From)
	select {
	case b.kick <- struct{}{}:
	default:
	}
	return nil
}

// admit decides whether from may message to right now. Rules about who may
// reach whom belong here, including any later ones for other machines.
func (b *Bus) admit(ctx context.Context, from, to string) error {
	limit := b.LoopLimit
	if limit == 0 {
		limit = DefaultLoopLimit
	}
	if limit < 0 {
		return nil
	}
	n, err := b.store.CountSent(ctx, from, to, time.Now().Add(-LoopWindow))
	if err != nil {
		return err
	}
	if n >= limit {
		return fmt.Errorf("%w: %s has sent %s %d messages in the last %v, which looks like a loop; check with your user before continuing",
			ErrLoop, from, to, n, LoopWindow)
	}
	return nil
}

// stopped reports whether agent's session id has stopped: it closed, or,
// once the bus has listened long enough to tell (see quiet), it went silent
// or was never heard from.
func (b *Bus) stopped(ctx context.Context, agent, id string) bool {
	now := time.Now()
	recent, err := b.store.RecentSessions(ctx, agent, now.Add(-SessionKeep))
	if err != nil {
		return false // don't claim it stopped when it can't be told
	}
	for _, s := range recent {
		if s.ID == id {
			return !s.EndedAt.IsZero() || (!s.Live(now) && !b.quiet(now))
		}
	}
	return !b.quiet(now)
}

// running reports whether agent's session id is running.
func (b *Bus) running(ctx context.Context, agent, id string) bool {
	live, err := b.store.LiveSessions(ctx, agent, time.Now().Add(-SessionTTL))
	if err != nil {
		return true // don't claim it closed when it can't be told
	}
	for _, s := range live {
		if s.ID == id {
			return true
		}
	}
	return false
}

// deliveryNote tells the sender when the recipient will see m, naming it by
// its label.
func (b *Bus) deliveryNote(ctx context.Context, to Agent, m Message) string {
	switch to.Delivery {
	case DeliveryPull:
	case "webhook":
		return fmt.Sprintf("queued: being sent to %s's webhook; it answers in its own time", to.DisplayName())
	case "acp":
		return fmt.Sprintf("queued: birdie will start %s for this and post its answer on this thread", to.DisplayName())
	default:
		return "queued"
	}
	live, err := b.Sessions(ctx, to)
	if err != nil {
		return "queued"
	}
	var target *Session
	for i := range live {
		if (m.ToSession != "" && live[i].ID == m.ToSession) || (m.ToSession == "" && live[i].Default) {
			target = &live[i]
		}
	}
	if target == nil {
		if m.ToSession != "" {
			return fmt.Sprintf("queued: %s is no longer open, so this waits for that session to come back, for up to %s; other sessions don't get it unless one waits on this thread. To reach a %s session that is open now, send a new message to its address (list_agents shows them)",
				b.sessionLabel(ctx, to, m.ToSession), days(b.holdFor()), to.DisplayName())
		}
		return fmt.Sprintf("queued: %s has no open session; it gets this when it next checks or starts", to.DisplayName())
	}
	name := to.DisplayName()
	if target.Name != "" {
		name += " · " + target.Name
	}
	switch target.Tier {
	case TierPush:
		return fmt.Sprintf("queued: %s gets this right away", name)
	case TierTurn:
		return fmt.Sprintf("queued: %s sees this at its next tool call or end of turn; if it's idle, it waits for its user", name)
	}
	return fmt.Sprintf("queued: %s only sees this when it checks its inbox", name)
}

// sessionLabel names one of to's sessions as people see it: by its name if
// birdie still knows it, else by its id.
func (b *Bus) sessionLabel(ctx context.Context, to Agent, id string) string {
	label := to.DisplayName() + " · " + id
	if recent, err := b.store.RecentSessions(ctx, to.Name, time.Now().Add(-SessionRetention)); err == nil {
		for _, s := range recent {
			if s.ID == id && s.Name != "" {
				label = to.DisplayName() + " · " + s.Name
			}
		}
	}
	return label
}

func (b *Bus) Thread(ctx context.Context, threadID, agent string) ([]Message, error) {
	msgs, err := b.store.Thread(ctx, threadID, agent)
	if err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("%w: thread %q", ErrNotFound, threadID)
	}
	return msgs, nil
}

func (b *Bus) subscribe(agent string) chan struct{} {
	ch := make(chan struct{}, 1)
	b.mu.Lock()
	if b.waiters[agent] == nil {
		b.waiters[agent] = make(map[chan struct{}]struct{})
	}
	b.waiters[agent][ch] = struct{}{}
	b.mu.Unlock()
	return ch
}

func (b *Bus) unsubscribe(agent string, ch chan struct{}) {
	b.mu.Lock()
	delete(b.waiters[agent], ch)
	if len(b.waiters[agent]) == 0 {
		delete(b.waiters, agent)
	}
	b.mu.Unlock()
}

func (b *Bus) notify(agent string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for ch := range b.waiters[agent] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// RunDelivery pushes queued messages to non-pull agents until ctx ends,
// retrying failures with exponential backoff.
func (b *Bus) RunDelivery(ctx context.Context) {
	if err := b.store.ReleaseOutbound(ctx); err != nil {
		b.log.Error("release unfinished deliveries", "err", err)
	}
	t := time.NewTicker(sweepInterval)
	defer t.Stop()
	// After a sleep, sessions may not have reported since, so the bus listens
	// again before taking silence as stopping.
	last := time.Now()
	awake := func(waiting bool) {
		now := time.Now()
		if asleep(time.Duration(now.UnixMilli()-last.UnixMilli())*time.Millisecond, now.Sub(last), waiting) {
			b.woke(last, now)
		}
		last = now
	}
	for {
		b.deliverDue(ctx)
		awake(false)
		if now := time.Now(); !b.quiet(now) && now.Sub(b.lastExpire) >= expireEvery {
			b.lastExpire = now
			// Expiring first spares a message that is already too old a
			// notice that it is held.
			b.expireHeld(ctx)
			b.noticeHeld(ctx, true)
		}
		last = time.Now()
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		case <-b.kick:
		}
		awake(true)
	}
}

// asleep reports whether the machine slept, or the bus was stopped, during a
// stretch that took wall on the wall clock and mono on the monotonic clock.
// The monotonic clock stops while the machine sleeps (on macOS and Linux) and
// the wall clock doesn't. Delivering can be slow, so only that difference
// counts; waiting for the next tick never takes long, so any long wait does.
func asleep(wall, mono time.Duration, waiting bool) bool {
	return wall-mono > SessionBeat || (waiting && wall > SessionBeat)
}

func (b *Bus) deliverDue(ctx context.Context) {
	due, err := b.store.DueOutbound(ctx, time.Now(), inboxBatch)
	if err != nil {
		if ctx.Err() == nil {
			b.log.Error("load outbound", "err", err)
		}
		return
	}
	for _, o := range due {
		err := fmt.Errorf("no deliverer for kind %q", o.Agent.Delivery)
		if d, ok := b.deliverers[o.Agent.Delivery]; ok {
			dctx, cancel := context.WithTimeout(ctx, deliverTimeout)
			err = d.Deliver(dctx, o.Agent, o.Message)
			cancel()
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, ErrAccepted) {
			if err := b.store.MarkClaimed(ctx, o.Message.ID); err != nil {
				b.log.Error("mark claimed", "id", o.Message.ID, "err", err)
			} else {
				b.emitMessage(StatusClaimed, o.Message, "", "")
			}
			continue
		}
		if err == nil {
			if err := b.store.MarkDelivered(ctx, o.Message.ID, time.Now()); err != nil {
				b.log.Error("mark delivered", "id", o.Message.ID, "err", err)
			} else {
				b.emitMessage(StatusDelivered, o.Message, "", "")
			}
			continue
		}
		attempts := o.Message.Attempts + 1
		failed := attempts >= maxAttempts
		next := time.Now().Add(min(time.Duration(1<<attempts)*time.Second, 5*time.Minute))
		b.log.Warn("delivery failed", "id", o.Message.ID, "to", o.Agent.Name, "attempt", attempts, "gave_up", failed, "err", err)
		why := err.Error()
		if err := b.store.RecordFailure(ctx, o.Message.ID, why, next, failed); err != nil {
			b.log.Error("record failure", "id", o.Message.ID, "err", err)
		} else if failed {
			b.emitMessage(StatusFailed, o.Message, "", why)
		} else {
			b.emitMessage("retrying", o.Message, "", why)
		}
		if failed {
			text := fmt.Sprintf("Your message %s to %s could not be delivered after %d attempts: %v", o.Message.ID, o.Agent.Name, attempts, err)
			if err := b.Notice(ctx, o.Message, "delivery_failed", text); err != nil {
				b.log.Error("send notice", "id", o.Message.ID, "err", err)
			}
		}
	}
}
