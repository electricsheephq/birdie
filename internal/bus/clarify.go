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
// one of its agents sees them: ThreadAwaitingClarification when the agent that
// got the thread's first message has sent nothing but questions
// (AsksQuestions) back to the agent that sent it, and the latest message is
// one of them; otherwise empty.
//
// That is the clarify-back protocol: the receiver of a new request asks
// before acting. Questions from anyone else in the thread don't set it, nor do
// the starter's own questions or birdie's notices. Once the receiver has said
// anything that isn't a question, such as an answer or "done", its later
// questions don't hold up the request.
//
// The first message the reader sees is taken as the thread's first. It must
// start the thread, or join it by its id; when the reader sees only replies,
// because threads are read per agent, the status is empty rather than guessed.
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
	if root == nil || root.InReplyTo != "" || last == root || !AsksQuestions(*last) || sameAuthor(*last, *root) ||
		last.From != root.To || last.To != root.From {
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
