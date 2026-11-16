package bus

import "strings"

// ClarifyMarker starts a reply that asks the sender questions before the
// receiver acts on a new request (the clarify-back protocol). Agents also use
// it as an everyday header on questions.
const ClarifyMarker = "(question / not a steer)"

// ThreadAwaitingClarification is the status of a thread whose latest message
// asks the agent that started it questions before acting.
const ThreadAwaitingClarification = "awaiting_clarification"

// AsksFirst reports whether m is a reply that starts with ClarifyMarker: it
// asks the other agent questions. Only the whole thread tells whether those
// hold up a request (see ThreadStatus).
func AsksFirst(m Message) bool {
	return m.InReplyTo != "" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(m.Content)), ClarifyMarker)
}

// ThreadStatus is the status of a thread from all its messages, oldest first:
// ThreadAwaitingClarification when its latest message asks the agent that
// started the thread questions (AsksFirst), so it has yet to answer; otherwise
// empty. The starter's own questions under the same header don't count, nor do
// birdie's notices. Without the thread's first message it is empty.
func ThreadStatus(msgs []Message) string {
	var root *Message
	for i := range msgs {
		if msgs[i].InReplyTo == "" && !IsSystemSender(msgs[i].From) {
			root = &msgs[i]
			break
		}
	}
	if root == nil {
		return ""
	}
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if IsSystemSender(m.From) {
			continue
		}
		if AsksFirst(m) && !sameAuthor(m, *root) {
			return ThreadAwaitingClarification
		}
		return ""
	}
	return ""
}

// sameAuthor reports whether a and b come from the same agent and, when both
// name one, the same session.
func sameAuthor(a, b Message) bool {
	return a.From == b.From && (a.FromSession == "" || b.FromSession == "" || a.FromSession == b.FromSession)
}
