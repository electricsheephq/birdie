package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

// SettleHeld picks only messages for a pull agent's session that isn't
// running, never mail that isn't pinned, notices, or an outbound agent's mail,
// and counts the hold from when the session stopped.
func TestSettleHeldPicksMessagesHeldForAStoppedSession(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now()
	for _, a := range []bus.Agent{{Name: "bot", Delivery: bus.DeliveryPull}, {Name: "claude", Delivery: bus.DeliveryPull}, {Name: "hook", Delivery: "webhook"}} {
		a.TokenHash, a.CreatedAt = a.Name, now
		if err := st.CreateAgent(ctx, a); err != nil {
			t.Fatal(err)
		}
	}
	session := func(agent, id string, seen time.Time, ended time.Time) {
		if err := st.UpsertSession(ctx, bus.Session{Agent: agent, ID: id, Name: id + "-name", ActiveAt: seen, LastSeen: seen}); err != nil {
			t.Fatal(err)
		}
		if !ended.IsZero() {
			if err := st.EndSession(ctx, agent, id, ended); err != nil {
				t.Fatal(err)
			}
		}
	}
	session("claude", "live", now, time.Time{})
	session("claude", "closed", now.Add(-3*time.Hour), now.Add(-2*time.Hour))
	session("claude", "just-closed", now, now.Add(-time.Minute))
	session("claude", "lapsed", now.Add(-3*time.Hour), time.Time{})
	session("hook", "closed", now.Add(-3*time.Hour), now.Add(-2*time.Hour))
	old := now.Add(-10 * 24 * time.Hour)
	for _, m := range []bus.Message{
		{ID: "live", To: "claude", ToSession: "live"},
		{ID: "closed", To: "claude", ToSession: "closed"},
		{ID: "just-closed", To: "claude", ToSession: "just-closed"},
		{ID: "lapsed", To: "claude", ToSession: "lapsed"},
		{ID: "never-seen", To: "claude", ToSession: "never-seen"},
		{ID: "unpinned", To: "claude"},
		{ID: "outbound", To: "hook", ToSession: "closed"},
		{ID: "notice", From: bus.SystemSender, To: "claude", ToSession: "closed"},
		{ID: "old-notice", From: bus.LegacySystemSender, To: "claude", ToSession: "closed"},
		{ID: "told", To: "claude", ToSession: "closed", Held: bus.RecipientSessionEnded},
		{ID: "delivered", To: "claude", ToSession: "closed", Status: bus.StatusDelivered},
	} {
		if m.From == "" {
			m.From = "bot"
		}
		if m.Status == "" {
			m.Status = bus.StatusQueued
		}
		m.ThreadID, m.Content, m.CreatedAt = "thr_"+m.ID, "x", old
		if err := st.InsertMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	pass := 0
	settle := func(q bus.HoldQuery) []string {
		t.Helper()
		pass++
		q.Now, q.LiveSince, q.Limit = now, now.Add(-bus.SessionTTL), 50
		var built []string
		held, notices, err := st.SettleHeld(ctx, q, func(held []bus.HeldMessage) []bus.Message {
			var out []bus.Message
			for _, h := range held {
				built = append(built, h.ID)
				if h.SessionName != h.ToSession+"-name" && h.ToSession != "never-seen" {
					t.Errorf("%s: session name %q", h.ID, h.SessionName)
				}
				out = append(out, bus.Message{ID: fmt.Sprintf("n%d_%s", pass, h.ID), ThreadID: h.ThreadID, From: bus.SystemSender, To: h.From, Content: "notice", Status: bus.StatusQueued, CreatedAt: now})
			}
			return out
		})
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, h := range held {
			ids = append(ids, h.ID)
		}
		if !slices.Equal(ids, built) || len(notices) != len(ids) {
			t.Fatalf("held %v, built %v, %d notices", ids, built, len(notices))
		}
		for _, n := range notices {
			if _, err := st.GetMessage(ctx, n.ID); err != nil {
				t.Fatalf("notice %s not stored: %v", n.ID, err)
			}
		}
		return ids
	}
	want := func(got []string, want ...string) {
		t.Helper()
		if !slices.Equal(got, want) {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	// Told they're held: closed sessions, and with Lapsed, silent ones. Once,
	// and not if the send said so.
	want(settle(bus.HoldQuery{}), "closed", "just-closed")
	want(settle(bus.HoldQuery{}))
	want(settle(bus.HoldQuery{Lapsed: true}), "lapsed", "never-seen")
	// Expired: held longer than an hour since the session stopped.
	want(settle(bus.HoldQuery{Expire: true, Lapsed: true, Before: now.Add(-time.Hour)}), "closed", "lapsed", "never-seen", "told")
	for id, status := range map[string]string{"closed": bus.StatusExpired, "just-closed": bus.StatusQueued, "live": bus.StatusQueued,
		"unpinned": bus.StatusQueued, "outbound": bus.StatusQueued, "notice": bus.StatusQueued, "old-notice": bus.StatusQueued,
		"delivered": bus.StatusDelivered} {
		if m, err := st.GetMessage(ctx, id); err != nil || m.Status != status {
			t.Fatalf("%s: status %q, %v; want %s", id, m.Status, err, status)
		}
	}
	// Only closed sessions without Lapsed.
	want(settle(bus.HoldQuery{Expire: true, Before: now}), "just-closed")
}
