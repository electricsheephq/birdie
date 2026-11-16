package store_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

func TestHeldMessagesReadOnlyAPreviewAndLeaveTheOriginalIntact(t *testing.T) {
	for _, expire := range []bool{false, true} {
		name := "held"
		if expire {
			name = "expired"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			now := time.Now()
			if err := st.CreateAgent(ctx, bus.Agent{Name: "bob", TokenHash: "test-hash", Delivery: bus.DeliveryPull, CreatedAt: now}); err != nil {
				t.Fatal(err)
			}
			// Include NUL and four-byte runes so byte and character cuts differ.
			content := "a\x00" + strings.Repeat("🙂界", 1<<19)
			m := bus.Message{ID: "held", ThreadID: "thread", From: "alice", FromSession: "sender",
				To: "bob", ToSession: "stopped", Content: content, Meta: map[string]string{"large": content},
				Status: bus.StatusQueued, CreatedAt: now.Add(-2 * time.Hour)}
			if err := st.InsertMessage(ctx, m); err != nil {
				t.Fatal(err)
			}
			check := func(held []bus.HeldMessage) {
				t.Helper()
				if len(held) != 1 {
					t.Fatalf("got %d held messages, want 1", len(held))
				}
				h := held[0]
				if len(h.Content) > 800 || !strings.HasPrefix(content, h.Content) || len([]rune(h.Content)) < 200 {
					t.Errorf("held content has %d bytes; want a prefix covering 200 runes in at most 800 bytes", len(h.Content))
				}
				if len(h.Meta) != 0 {
					t.Error("held messages read caller-supplied metadata")
				}
				if h.ID != m.ID || h.ThreadID != m.ThreadID || h.FromSession != m.FromSession || h.ToSession != m.ToSession {
					t.Error("held message lost its routing details")
				}
			}
			held, _, err := st.SettleHeld(ctx, bus.HoldQuery{Expire: expire, Lapsed: true,
				Now: now, Before: now.Add(-time.Hour), LiveSince: now, Limit: 50}, func(held []bus.HeldMessage) []bus.Message {
				check(held)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			check(held)
			original, err := st.GetMessage(ctx, m.ID)
			if err != nil || original.Content != content || original.Meta["large"] != content {
				t.Fatalf("settling held messages changed the stored content or metadata: %v", err)
			}
		})
	}
}
