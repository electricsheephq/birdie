package bus

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
)

// Codes of AddressError.
const (
	// RecipientNotFound: no running session has that id or name.
	RecipientNotFound = "recipient_not_found"
	// RecipientSessionEnded: the session it names has closed or stopped
	// reporting. A message for it isn't given to another one.
	RecipientSessionEnded = "recipient_session_ended"
	// RecipientAmbiguous: a name with the start of an id matches more than
	// one running session.
	RecipientAmbiguous = "recipient_ambiguous"
	// SessionRequired: the agent doesn't pick one of its sessions for a
	// message addressed to it alone (see PicksSession).
	SessionRequired = "session_required"
)

// SessionRef names a running session for a sender to address.
type SessionRef struct {
	// Address reaches exactly this session, as agent/name#id or agent/id.
	Address  string    `json:"address"`
	ID       string    `json:"id"`
	Name     string    `json:"name,omitempty"`
	State    string    `json:"state,omitempty"`
	ActiveAt time.Time `json:"active_at"`
}

// AddressError is a recipient birdie won't resolve to a session. Live lists
// the agent's running sessions, so the sender can address one.
type AddressError struct {
	Code string
	Msg  string
	Live []SessionRef
	kind error
}

func (e *AddressError) Error() string {
	s := e.Code + ": " + e.Msg
	if len(e.Live) > 0 {
		refs := make([]string, len(e.Live))
		for i, r := range e.Live {
			refs[i] = describe(r, time.Now())
		}
		s += ". Running sessions: " + strings.Join(refs, ", ")
	}
	return s
}

func (e *AddressError) Unwrap() error { return e.kind }

// refs lists sessions as a sender can address them, most recently active
// first.
func refs(a Agent, live []Session) []SessionRef {
	live = slices.Clone(live)
	slices.SortStableFunc(live, func(x, y Session) int { return y.ActiveAt.Compare(x.ActiveAt) })
	out := make([]SessionRef, len(live))
	for i, s := range live {
		out[i] = SessionRef{Address: addressOf(a, s), ID: s.ID, Name: s.Name, State: s.State, ActiveAt: s.ActiveAt}
	}
	return out
}

// addressOf is an address that reaches only s: its name with its whole id,
// or its id when it has no name. A shortened id could later match another
// session.
func addressOf(a Agent, s Session) string {
	if s.Name == "" {
		return a.Name + "/" + s.ID
	}
	return a.Name + "/" + s.Name + "#" + s.ID
}

func describe(r SessionRef, now time.Time) string {
	s := r.Address + " ("
	if r.State != "" {
		s += r.State + ", "
	}
	return s + "active " + ago(r.ActiveAt, now) + ")"
}

func ago(t, now time.Time) string {
	d := now.Sub(t)
	switch {
	case t.IsZero():
		return "at an unknown time"
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d/time.Minute))
	case d < 48*time.Hour:
		return fmt.Sprintf("%dh ago", int(d/time.Hour))
	}
	return fmt.Sprintf("%dd ago", int(d/(24*time.Hour)))
}

// findSession resolves addr, an address naming one of to's sessions, to a
// running session. An id wins over a name. A name with an id (name#id) must
// match both. A name that several running sessions share goes to the most
// recently active one, and the others come back so the sender can choose.
// A session that isn't running is never swapped for another.
func (b *Bus) findSession(ctx context.Context, to Agent, addr Address) (Session, []SessionRef, error) {
	now := time.Now()
	recent, err := b.store.RecentSessions(ctx, to.Name, now.Add(-SessionKeep))
	if err != nil {
		return Session{}, nil, err
	}
	var live []Session
	for _, s := range recent {
		if s.Live(now) {
			live = append(live, s)
		}
	}
	fail := func(code, msg string) (Session, []SessionRef, error) {
		return Session{}, nil, &AddressError{Code: code, Msg: msg, Live: refs(to, live), kind: ErrNotFound}
	}
	ended := func(s Session) (Session, []SessionRef, error) {
		return fail(RecipientSessionEnded, fmt.Sprintf("%s's session %s stopped %s; send to a running one", to.DisplayName(), addressOf(to, s), ago(s.StoppedAt(), now)))
	}
	matches := func(s Session) bool {
		if addr.ID != "" {
			return s.Name == addr.Session && strings.HasPrefix(s.ID, addr.ID)
		}
		return s.Name == addr.Session
	}
	// A whole id, alone or after a name, names one session. A session that
	// isn't running is never swapped for one whose id merely starts the same.
	whole := addr.Session
	if addr.ID != "" {
		whole = addr.ID
	}
	for _, s := range recent {
		if s.ID != whole {
			continue
		}
		if addr.ID != "" && s.Name != addr.Session {
			return fail(RecipientNotFound, fmt.Sprintf("%s has no session %s: %s is the id of %s", to.DisplayName(), addr, addr.ID, addressOf(to, s)))
		}
		if s.Live(now) {
			return s, nil, nil
		}
		return ended(s)
	}
	var found []Session
	for _, s := range live {
		if matches(s) {
			found = append(found, s)
		}
	}
	if addr.ID != "" && len(found) > 0 {
		// The start of an id that also starts one that has stopped could
		// mean either.
		for _, s := range recent {
			if !s.Live(now) && matches(s) {
				return Session{}, nil, &AddressError{Code: RecipientAmbiguous, Msg: fmt.Sprintf("%s also starts the id of %s, which stopped %s; give the whole id", addr, addressOf(to, s), ago(s.StoppedAt(), now)), Live: refs(to, live), kind: ErrInvalid}
			}
		}
	}
	switch {
	case len(found) == 1:
		return found[0], nil, nil
	case len(found) > 1 && addr.ID != "":
		return Session{}, nil, &AddressError{Code: RecipientAmbiguous, Msg: fmt.Sprintf("%d of %s's running sessions match %s; give more of the id", len(found), to.DisplayName(), addr), Live: refs(to, found), kind: ErrInvalid}
	case len(found) > 1:
		all := refs(to, found)
		for _, s := range found {
			if s.ID == all[0].ID {
				return s, all[1:], nil
			}
		}
	}
	// None running. One that was says why, unless a running one took its
	// name since.
	for _, s := range recent {
		if !s.Live(now) && matches(s) {
			return ended(s)
		}
	}
	return fail(RecipientNotFound, fmt.Sprintf("%s has no running session %s", to.DisplayName(), addr))
}

// othersNote tells the sender that name reached one of several sessions.
func othersNote(to Agent, name string, others []SessionRef) string {
	now := time.Now()
	list := make([]string, len(others))
	for i, r := range others {
		list[i] = describe(r, now)
	}
	return fmt.Sprintf("%d other running %s sessions are named %s and didn't get it: %s. To reach one of them, send to its address", len(others), to.DisplayName(), name, strings.Join(list, ", "))
}

// chosenSession is the session the user chose to take messages addressed to
// agent to alone, which doesn't pick one itself (see PicksSession), and the
// other running sessions with its name. The message is pinned to it, so it
// waits for that session even if another with its name opens later. Without
// one running, the message is refused, with the sessions that are, so the
// sender can address one.
func (b *Bus) chosenSession(ctx context.Context, to Agent) (Session, []SessionRef, error) {
	live, err := b.store.LiveSessions(ctx, to.Name, time.Now().Add(-SessionTTL))
	if err != nil {
		return Session{}, nil, err
	}
	if d := defaultSession(live, to); d != nil {
		var same []Session
		for _, s := range live {
			if s.Name == d.Name && s.ID != d.ID {
				same = append(same, s)
			}
		}
		return *d, refs(to, same), nil
	}
	msg := fmt.Sprintf("%s doesn't pick one of its sessions for a message addressed to %s alone; send it to one of them", to.DisplayName(), to.Name)
	if len(live) == 0 {
		msg = fmt.Sprintf("%s has no running session; send to one by its address once it is running", to.DisplayName())
	}
	return Session{}, nil, &AddressError{Code: SessionRequired, Msg: msg, Live: refs(to, live), kind: ErrInvalid}
}

// AsAddressError reports err's AddressError, if it is one.
func AsAddressError(err error) (*AddressError, bool) {
	var e *AddressError
	ok := errors.As(err, &e)
	return e, ok
}
