package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"time"

	"birdie/internal/bus"
)

type schema = map[string]any

func obj(required []string, props schema) schema {
	s := schema{"type": "object", "properties": props}
	if len(required) > 0 {
		s["required"] = required
	}
	return s
}

func str(desc string) schema { return schema{"type": "string", "description": desc} }

var tools = []schema{
	{
		"name":        "send_message",
		"description": "Message another AI agent or bot (for example Grok Bot, Claude, Codex or Grok): ask it something, hand it a task, or answer it. Returns message_id, thread_id and a note on when the recipient will see it. To answer a message, set in_reply_to to its message_id; to can then be left out.",
		"inputSchema": obj([]string{"content"}, schema{
			"to":          str("Recipient from list_agents (e.g. grok-bot), or one running session by its address from list_agents: agent/name, agent/name#id or agent/id (e.g. claude/api). Claude sessions must be addressed one by one. Optional when in_reply_to is set."),
			"content":     str("The message. For requests, make it self-contained: the recipient has none of your context."),
			"in_reply_to": str("message_id being answered; the reply joins that thread."),
			"thread_id":   str("Continue an existing thread without replying to a specific message."),
			"context":     str("Optional supporting files, links or notes."),
			"wanted":      str("Optional description of the result you want back."),
		}),
	},
	{
		"name":        "check_inbox",
		"description": "Fetch new messages other agents sent you. Each message is returned once.",
		"inputSchema": obj(nil, schema{
			"wait_seconds": schema{"type": "number", "description": "Wait up to this long (max 120) if the inbox is empty. Default 0."},
		}),
	},
	{
		"name":        "wait_for_reply",
		"description": "Wait for the answer to a message you sent: blocks until a message arrives on the thread. Returns the new messages, or nothing on timeout.",
		"inputSchema": obj([]string{"thread_id"}, schema{
			"thread_id":       str("thread_id returned by send_message."),
			"timeout_seconds": schema{"type": "number", "description": "Default 120, max 600."},
		}),
	},
	{
		"name":        "get_thread",
		"description": "Read the full history of a thread you are part of.",
		"inputSchema": obj([]string{"thread_id"}, schema{"thread_id": str("Thread to read.")}),
	},
	{
		"name":        "list_agents",
		"description": "List the other AI agents and bots you can message, with what each one is for and any running sessions. A session's state, when shown, is busy, idle, or waiting (stopped until its user answers).",
		"inputSchema": obj(nil, schema{}),
	},
}

type callParams struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
	Meta      json.RawMessage `json:"_meta"`
}

type toolArgs struct {
	To             string  `json:"to"`
	Content        string  `json:"content"`
	InReplyTo      string  `json:"in_reply_to"`
	ThreadID       string  `json:"thread_id"`
	Context        string  `json:"context"`
	Wanted         string  `json:"wanted"`
	WaitSeconds    float64 `json:"wait_seconds"`
	TimeoutSeconds float64 `json:"timeout_seconds"`
}

func (s *Server) callTool(ctx context.Context, raw json.RawMessage, taken *[]bus.Message) (any, *rpcError) {
	var p callParams
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, &rpcError{-32602, "invalid params"}
	}
	if h := s.currentHost(); h != nil {
		tier := h.Tier()
		if len(p.Meta) > 0 {
			h.Observe(p.Meta)
		}
		// The call may be the first to name the conversation, or the first
		// since it changed (/clear); act for the conversation making it.
		if h.Tier() != tier || h.Session().ID != s.c.Session() {
			// Not cut short if the call is cancelled: the old conversation
			// must still be ended, or nothing will end it.
			s.syncSession(context.WithoutCancel(ctx), h)
		}
	}
	var a toolArgs
	if len(p.Arguments) > 0 {
		if err := json.Unmarshal(p.Arguments, &a); err != nil {
			return toolText("invalid arguments: "+err.Error(), true), nil
		}
	}

	var out any
	var err error
	switch p.Name {
	case "send_message":
		meta := map[string]string{}
		if a.Context != "" {
			meta["context"] = a.Context
		}
		if a.Wanted != "" {
			meta["wanted"] = a.Wanted
		}
		var m bus.Message
		m, err = s.c.Send(ctx, bus.SendInput{To: a.To, Content: a.Content, InReplyTo: a.InReplyTo, ThreadID: a.ThreadID, Meta: meta})
		if err == nil {
			// A later wait_for_reply is for the answer to this, not what came before.
			s.waitMu.Lock()
			delete(s.shown, m.ThreadID)
			s.waitMu.Unlock()
		}
		out = map[string]string{"message_id": m.ID, "thread_id": m.ThreadID, "status": m.Status, "note": m.Note}
	case "check_inbox":
		if h := s.currentHost(); h != nil && s.pushing() {
			// It may have been made pull-only since it last reported.
			s.syncSession(context.WithoutCancel(ctx), h)
		}
		if s.pushing() {
			return toolText("New messages come to you on their own in this session, so there is nothing to fetch.", false), nil
		}
		if h := s.currentHost(); h != nil {
			if _, err := hostConversation(h); errors.Is(err, errUnsure) {
				// Taking messages for the conversation last seen could show
				// another conversation's here.
				return toolText("Birdie can't tell which conversation this is right now (it can't read Claude Code's session registry), so messages wait until it can. Try again shortly.", true), nil
			}
		}
		var msgs []bus.Message
		until := time.Now().Add(seconds(a.WaitSeconds, 0, bus.MaxWait))
		seen := s.newBatch(ctx)
		for {
			msgs, err = s.c.Inbox(ctx, "", max(time.Until(until), 0))
			// What was shown before is confirmed, and the wait goes on.
			msgs = s.confirmSeen(seen, msgs)
			if err != nil || len(msgs) > 0 || time.Until(until) <= 0 {
				break
			}
		}
		if err == nil {
			*taken = msgs
		}
		out = map[string]any{"messages": msgs}
	case "wait_for_reply":
		if a.ThreadID == "" {
			return toolText("thread_id is required", true), nil
		}
		var msgs []bus.Message
		pushing := s.pushing()
		// One decision for how to wait and whether to confirm what comes
		// back: the tier can change while the call waits.
		msgs, *taken, err = s.waitForReply(ctx, a.ThreadID, seconds(a.TimeoutSeconds, 120*time.Second, maxReplyWait), pushing)
		if err == nil && len(msgs) == 0 {
			note := "No reply yet. Check again later with wait_for_reply or check_inbox."
			if s.pushing() {
				note = "No reply yet. It will come to you on its own when it arrives."
			}
			return toolText(note, false), nil
		}
		reply := map[string]any{"messages": msgs}
		if err == nil && slices.ContainsFunc(msgs, bus.AsksQuestions) {
			if status := s.threadStatus(ctx, a.ThreadID); status != "" {
				reply["thread_status"] = status
			}
		}
		out = reply
	case "get_thread":
		var msgs []bus.Message
		msgs, err = s.c.Thread(ctx, a.ThreadID)
		thread := map[string]any{"messages": msgs}
		if status := bus.ThreadStatus(msgs); status != "" {
			thread["status"] = status
		}
		out = thread
	case "list_agents":
		var agents []bus.Agent
		agents, err = s.c.Agents(ctx)
		out = map[string]any{"you": s.name, "agents": agents}
	default:
		return nil, &rpcError{-32602, "unknown tool: " + p.Name}
	}
	if err != nil {
		return toolText(err.Error(), true), nil
	}
	b, _ := json.MarshalIndent(out, "", "  ")
	text := string(b)
	if p.Name != "check_inbox" && p.Name != "wait_for_reply" {
		text += s.unreadNotice(ctx)
	}
	return toolText(text, false), nil
}

// unreadNotice tells a session that can't be pushed to that mail is waiting.
func (s *Server) unreadNotice(ctx context.Context) string {
	if s.currentHost() == nil || s.pushing() {
		return ""
	}
	n, err := s.c.Unread(ctx)
	if err != nil || n == 0 {
		return ""
	}
	return fmt.Sprintf("\n\n(%d unread birdie message(s) waiting; call check_inbox to read them.)", n)
}

func seconds(v float64, def, max time.Duration) time.Duration {
	if v <= 0 {
		return def
	}
	return min(time.Duration(v*float64(time.Second)), max)
}

func toolText(text string, isErr bool) map[string]any {
	return map[string]any{
		"content": []map[string]string{{"type": "text", "text": text}},
		"isError": isErr,
	}
}

// statusWait bounds the extra read of a thread that wait_for_reply makes to
// report its status.
const statusWait = 2 * time.Second

// threadStatus is the status of a thread (see bus.ThreadStatus), read within
// statusWait. Only the whole thread tells whether questions hold up a request.
// The replies wait_for_reply returns are already taken, so a failed or slow
// read leaves the status out rather than failing or holding up the call.
func (s *Server) threadStatus(ctx context.Context, thread string) string {
	ctx, cancel := context.WithTimeout(ctx, statusWait)
	defer cancel()
	msgs, err := s.c.Thread(ctx, thread)
	if err != nil {
		return ""
	}
	return bus.ThreadStatus(msgs)
}
