package main

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

// `birdie session pull-only ADDRESS on|off` flips one running session, and
// the session list shows it.
func TestSessionPullOnly(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	b := bus.New(st, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err := st.CreateAgent(ctx, bus.Agent{Name: "claude", Delivery: bus.DeliveryPull, CreatedAt: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := b.ReportSession(ctx, "claude", "s1", bus.SessionInput{Name: "api", Tier: bus.TierPush}); err != nil {
		t.Fatal(err)
	}
	list := func() string {
		agents, err := b.AgentsWithRecentSessions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		var out bytes.Buffer
		printSessions(&out, agents, false, time.Now())
		return out.String()
	}
	for _, args := range [][]string{{"pull-only"}, {"pull-only", "claude/api"}, {"pull-only", "claude/api", "yes"}} {
		if err := sessionCmd(ctx, b, nil, nil, args); err == nil || !strings.Contains(err.Error(), "usage") {
			t.Fatalf("%v: %v", args, err)
		}
	}
	if err := sessionCmd(ctx, b, nil, nil, []string{"pull-only", "claude/api", "on"}); err != nil {
		t.Fatal(err)
	}
	if on, _ := st.SessionPullOnly(ctx, "claude", "s1"); !on || !strings.Contains(list(), "pull-only") {
		t.Fatalf("on: %v\n%s", on, list())
	}
	if err := sessionCmd(ctx, b, nil, nil, []string{"pull-only", "claude/s1", "off"}); err != nil {
		t.Fatal(err)
	}
	if on, _ := st.SessionPullOnly(ctx, "claude", "s1"); on || strings.Contains(list(), "pull-only") {
		t.Fatalf("off: %v\n%s", on, list())
	}
	if err := sessionCmd(ctx, b, nil, nil, []string{"pull-only", "claude/gone", "on"}); err == nil {
		t.Fatal("a session that isn't running was accepted")
	}
}
