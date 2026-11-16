package bus

import "strings"

// ClarifyMarker starts a message that asks questions: in the clarify-back
// protocol, a reply asking the sender questions before acting on its request.
// Agents also use it as an everyday header on questions.
const ClarifyMarker = "(question / not a steer)"

// ThreadAwaitingClarification is the status of a thread whose request is held
// up by questions to the agent that started it.
const ThreadAwaitingClarification = "awaiting_clarification"

// AsksQuestions reports whether m starts with ClarifyMarker. Only the whole
// thread tells whether its questions hold up a request (see ThreadStatus).
func AsksQuestions(m Message) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(m.Content)), ClarifyMarker)
}

// ThreadStatus is the status of a thread from its messages, oldest first, as
// one of its agents sees them: ThreadAwaitingClarification while the other
// side has only asked questions (AsksQuestions) since the thread's first
// message, and the latest of those, addressed to the starter, hasn't been
// answered; otherwise empty.
//
// The thread's first message, as the reader sees it, names its starter. Once
// the other side has said anything that isn't a question, such as an answer
// or "done", later questions don't count: the request isn't held up. The
// starter's own questions don't count either, nor do birdie's notices.
func ThreadStatus(msgs []Message) string {
	var root, last *Message
	for i := range msgs {
		if IsSystemSender(msgs[i].From) {
			continue
		}
		if root == nil {
			root = &msgs[i]
		}
		last = &msgs[i]
	}
	if root == nil || last == root || sameAuthor(*last, *root) || last.To != root.From {
		return ""
	}
	for _, m := range msgs {
		if IsSystemSender(m.From) || sameAuthor(m, *root) {
			continue
		}
		if !AsksQuestions(m) {
			return ""
		}
	}
	return ThreadAwaitingClarification
}

// sameAuthor reports whether a and b come from the same agent and, when both
// name one, the same session.
func sameAuthor(a, b Message) bool {
	return a.From == b.From && (a.FromSession == "" || b.FromSession == "" || a.FromSession == b.FromSession)
}
