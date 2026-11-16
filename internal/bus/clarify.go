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
// one of its agents sees them: ThreadAwaitingClarification when the latest
// message asks the thread's starter questions (AsksQuestions) and its author
// has sent nothing but questions in the thread; otherwise empty.
//
// The first message the reader sees names the starter. It must start the
// thread, or join it by its id; when the reader sees only replies, because
// threads are read per agent, the status is empty rather than guessed. Once
// the asker has said anything that isn't a question, such as an answer or
// "done", its later questions don't hold up a request; what other agents in
// the thread say doesn't change that. The starter's own questions don't count,
// nor do birdie's notices.
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
	if root == nil || root.InReplyTo != "" || last == root || !AsksQuestions(*last) || sameAuthor(*last, *root) || last.To != root.From {
		return ""
	}
	for _, m := range msgs {
		if sameAuthor(m, *last) && !AsksQuestions(m) {
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
