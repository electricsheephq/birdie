package mcpserver_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestTheAgentToolDescribesWarmColdAndStoppedSessions(t *testing.T) {
	s, cursor := setup(t, false)
	ctx := context.Background()
	now := time.Now()
	for id, active := range map[string]time.Time{"warm": now, "cold": now.Add(-3 * time.Hour), "stopped": now} {
		if _, err := cursor.ReportSession(ctx, id, bus.SessionInput{ActiveAt: active, Tier: bus.TierPush}); err != nil {
			t.Fatal(err)
		}
	}
	toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor/stopped", "content": "update"}}))
	if err := cursor.EndSession(ctx, "stopped"); err != nil {
		t.Fatal(err)
	}

	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "list_agents"}))
	for _, item := range got["agents"].([]any) {
		a := item.(map[string]any)
		if a["name"] != "cursor" {
			continue
		}
		sessions := map[string]map[string]any{}
		for _, item := range a["sessions"].([]any) {
			s := item.(map[string]any)
			sessions[s["id"].(string)] = s
		}
		for id, want := range map[string]string{"warm": "warm", "cold": "cold (idle 3 h)", "stopped": "not running; 1 held, delivered when it comes back"} {
			if desc, _ := sessions[id]["delivery_note"].(string); !strings.Contains(desc, want) {
				t.Errorf("%s: want %q, got %v", id, want, sessions[id])
			}
		}
		return
	}
	t.Fatal("cursor missing from list_agents")
}

func TestTheSendToolReturnsTheDeliveryCost(t *testing.T) {
	s, cursor := setup(t, false)
	if _, err := cursor.ReportSession(context.Background(), "desk", bus.SessionInput{Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	got := toolJSON(t, s.call("tools/call", map[string]any{"name": "send_message", "arguments": map[string]any{"to": "cursor/desk", "content": "update"}}))
	if got["cost"] != "warm" {
		t.Fatalf("send_message = %v", got)
	}
}

func TestTheAgentToolExplainsHeldMailOnStoppedRowsAndWarmth(t *testing.T) {
	s, _ := setup(t, false)
	tools := s.call("tools/list", map[string]any{})["tools"].([]any)
	for _, item := range tools {
		tool := item.(map[string]any)
		if tool["name"] == "list_agents" {
			desc := tool["description"].(string)
			if !strings.Contains(desc, "not running") || !strings.Contains(desc, "held") || !strings.Contains(desc, "warm") || !strings.Contains(desc, "50 minutes") {
				t.Errorf("list_agents description = %q", desc)
			}
			return
		}
	}
	t.Fatal("list_agents is missing")
}
