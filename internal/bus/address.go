package bus

import (
	"fmt"
	"regexp"
	"strings"
)

var sessionPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

// Address is where a message goes: agent, agent/session, agent/name#id, or
// agent@node. Session is a session's id or its name; ID, when set, is the
// start of the id of the session with that name.
//
// Node is reserved for other machines. Agent names can't contain / or @, so
// the plain names in use today stay valid when that arrives.
type Address struct {
	Agent, Session, ID, Node string
}

// ParseAddress splits a recipient such as "claude", "claude/api",
// "claude/api#3f2a" or "claude/api@laptop".
func ParseAddress(s string) (Address, error) {
	var a Address
	rest, node, hasNode := strings.Cut(s, "@")
	agent, session, hasSession := strings.Cut(rest, "/")
	session, id, hasID := strings.Cut(session, "#")
	a.Agent, a.Session, a.ID, a.Node = agent, session, id, node
	if a.Agent == "" || (hasNode && node == "") || (hasSession && !sessionPattern.MatchString(session)) ||
		(hasID && (!hasSession || !sessionPattern.MatchString(id))) {
		return Address{}, fmt.Errorf("%w: bad address %q (want agent, agent/session or agent/name#id)", ErrInvalid, s)
	}
	return a, nil
}

func (a Address) String() string {
	s := a.Agent
	if a.Session != "" {
		s += "/" + a.Session
	}
	if a.ID != "" {
		s += "#" + a.ID
	}
	if a.Node != "" {
		s += "@" + a.Node
	}
	return s
}
