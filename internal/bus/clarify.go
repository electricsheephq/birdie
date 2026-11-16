package bus

import "strings"

// ClarifyMarker starts a reply that asks the sender questions before the
// receiver acts on a new request (the clarify-back protocol).
const ClarifyMarker = "(question / not a steer)"

// ThreadAwaitingClarification is the status of a thread whose latest message
// asks its sender questions before the receiver acts.
const ThreadAwaitingClarification = "awaiting_clarification"

// ThreadStatus is the status of a thread from its messages, oldest first:
// ThreadAwaitingClarification, or empty. birdie's own notices don't count.
func ThreadStatus(msgs []Message) string {
	for i := len(msgs) - 1; i >= 0; i-- {
		m := msgs[i]
		if IsSystemSender(m.From) {
			continue
		}
		if m.InReplyTo != "" && strings.HasPrefix(strings.ToLower(strings.TrimSpace(m.Content)), ClarifyMarker) {
			return ThreadAwaitingClarification
		}
		return ""
	}
	return ""
}
