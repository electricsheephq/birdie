package bus_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

func TestLegacyBareMailExpiresOrReachesTheChosenSession(t *testing.T) {
	for _, choice := range []string{"none", "default", "fallback"} {
		t.Run(choice, func(t *testing.T) {
			ctx := context.Background()
			b, st := newBus(t)
			addAgent(t, st, "bot", nil)
			addAgent(t, st, "claude", nil)
			if err := st.SetAgentConfig(ctx, "claude", "fallback", "off"); err != nil {
				t.Fatal(err)
			}
			report(t, b, "claude", "s1", "api", bus.TierPush, time.Now())
			if choice == "default" {
				if err := st.SetAgentConfig(ctx, "claude", "default_session", "api"); err != nil {
					t.Fatal(err)
				}
			} else if choice == "fallback" {
				picksSession(t, st, "claude")
			}
			// Insert directly because new bare sends already require a session.
			for _, id := range []string{"old", "recent"} {
				created := time.Now()
				if id == "old" {
					created = created.Add(-2 * time.Hour)
				}
				if err := st.InsertMessage(ctx, bus.Message{
					ID: id, ThreadID: "legacy", From: "bot", FromSession: "sender",
					To: "claude", Content: "before the upgrade", Status: bus.StatusQueued, CreatedAt: created,
				}); err != nil {
					t.Fatal(err)
				}
			}
			b.HoldFor, b.QuietFor = time.Hour, -1
			// Its expiry notice proves the sweep ran before a chosen session
			// takes the legacy mail, so delivery cannot hide an expiry bug.
			if err := st.InsertMessage(ctx, bus.Message{
				ID: "sweep", ThreadID: "sweep", From: "bot", To: "claude", ToSession: "gone",
				Content: "for a stopped session", Status: bus.StatusQueued, CreatedAt: time.Now().Add(-2 * time.Hour),
			}); err != nil {
				t.Fatal(err)
			}
			run, cancel := context.WithCancel(ctx)
			done := make(chan struct{})
			go func() {
				defer close(done)
				b.RunDelivery(run)
			}()
			t.Cleanup(func() { cancel(); <-done })
			if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: "sweep", Wait: time.Second}); err != nil || len(got) != 1 || got[0].Meta["notice"] != "expired" {
				t.Fatalf("sweep notice: %+v, %v", got, err)
			}
			if choice == "none" {
				if got := take(t, b, "claude", "s1"); len(got) != 0 {
					t.Fatalf("no default but a session took %+v", got)
				}
				seen := map[string]bool{}
				for len(seen) < 2 {
					got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Session: "sender", Thread: "legacy", Wait: time.Second})
					if err != nil || len(got) == 0 {
						t.Fatalf("legacy mail was stranded: notices %v, err %v", seen, err)
					}
					for _, n := range got {
						kind := n.Meta["notice"]
						wantID := "recent"
						if kind == "expired" {
							wantID = "old"
						}
						if (kind != "held" && kind != "expired") || seen[kind] || n.From != bus.SystemSender || n.ToSession != "sender" || n.InReplyTo != wantID || n.Meta["messages"] != wantID || !strings.Contains(n.Content, bus.SessionRequired) || !strings.Contains(n.Content, "birdie session default") {
							t.Fatalf("legacy notice: %+v", n)
						}
						seen[kind] = true
					}
				}
				for id, want := range map[string]string{"old": bus.StatusExpired, "recent": bus.StatusQueued} {
					if m, err := st.GetMessage(ctx, id); err != nil || m.Status != want {
						t.Fatalf("%s status = %s, err %v; want %s", id, m.Status, err, want)
					}
				}
				return
			}
			if got := take(t, b, "claude", "s1"); len(got) != 2 {
				t.Fatalf("chosen session took %+v; want both legacy messages", got)
			}
			if got, err := b.ReceiveFor(ctx, bus.ReceiveInput{Agent: "bot", Thread: "legacy"}); err != nil || len(got) != 0 {
				t.Fatalf("deliverable mail got notices: %+v, %v", got, err)
			}
		})
	}
}
