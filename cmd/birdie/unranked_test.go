package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"birdie/internal/bus"
	"birdie/internal/store"
)

func TestSessionUnrankedRejectsBadValuesAndClearsTheSetting(t *testing.T) {
	ctx := context.Background()
	db := filepath.Join(t.TempDir(), "bus.db")
	t.Setenv("BIRDIE_DB", db)
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	if err := st.CreateAgent(ctx, bus.Agent{Name: "claude", Delivery: bus.DeliveryPull}); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"unranked", "claude", "p0"},
		{"unranked", "claude/name#", "p3"},
		{"unranked", "claude/name@remote", "p3"},
	} {
		if err := session(ctx, args); err == nil || !strings.Contains(err.Error(), "usage") {
			t.Errorf("%v: want a usage error, got %v", args, err)
		}
	}
	for _, tc := range []struct{ target, key string }{{"claude", "unranked_as"}, {"claude/api", "unranked_as:api"}} {
		for _, value := range []string{"p3", ""} {
			args := []string{"unranked", tc.target}
			if value != "" {
				args = append(args, value)
			}
			if err := session(ctx, args); err != nil {
				t.Fatal(err)
			}
			a, err := st.GetAgent(ctx, "claude")
			if err != nil {
				t.Fatal(err)
			}
			if got, present := a.Config[tc.key]; got != value || present != (value != "") {
				t.Errorf("%s = %q (present %t), want %q", tc.key, got, present, value)
			}
		}
	}
}

func TestSessionUnrankedSavesTheNameWhenGivenAnIDOrListedAddress(t *testing.T) {
	for _, target := range []string{"claude/a1b2", "claude/lcm-x#a1b2", "claude/lcm-x"} {
		t.Run(target, func(t *testing.T) {
			st, run := unrankedSessions(t)
			out, err := run(target)
			if err != nil {
				t.Fatal(err)
			}
			a, err := st.GetAgent(context.Background(), "claude")
			if err != nil || a.UnrankedAs("lcm-x") != "p3" || a.Config["unranked_as:a1b2"] != "" {
				t.Errorf("%s did not set only the name: %v, %v", target, a.Config, err)
			}
			if !strings.Contains(out, "covers: claude/lcm-x#a1b2 (running), claude/lcm-x#c3d4 (stopped)") {
				t.Errorf("confirmation = %q", out)
			}
		})
	}
}

func TestSessionUnrankedWarnsAndSavesAnExactNameForAFutureSession(t *testing.T) {
	st, run := unrankedSessions(t)
	out, err := run("claude/lcm")
	if err != nil || !strings.Contains(out, "no session has this name yet") {
		t.Errorf("confirmation = %q, %v", out, err)
	}
	a, err := st.GetAgent(context.Background(), "claude")
	if err != nil || a.UnrankedAs("lcm") != "p3" || a.UnrankedAs("lcm-x") != "immediate" {
		t.Errorf("future setting = %v, %v", a.Config, err)
	}
}

func TestSessionUnrankedRejectsAnUnknownIDWithoutWritingASetting(t *testing.T) {
	st, run := unrankedSessions(t)
	for _, target := range []string{"claude/lcm-x#missing", "claude/a6e25a19-0389-456b-8ee7-ba3699c3157a", "claude/other#a1b2"} {
		out, err := run(target)
		if err == nil || !strings.Contains(err.Error(), "no recent session matches") || out != "" {
			t.Errorf("%s: confirmation = %q, error = %v", target, out, err)
		}
	}
	a, err := st.GetAgent(context.Background(), "claude")
	if err != nil || len(a.Config) != 0 {
		t.Errorf("agent config = %v, %v", a.Config, err)
	}
}

func TestBirdieHelpShowsHowToSetTheUnrankedPriority(t *testing.T) {
	if !strings.Contains(usage, "birdie session unranked AGENT[/NAME] [immediate|p1|p3]") {
		t.Error("birdie help omits the session unranked command")
	}
}

func unrankedSessions(t *testing.T) (*store.SQLite, func(string) (string, error)) {
	t.Helper()
	ctx := context.Background()
	dir := t.TempDir()
	db := filepath.Join(dir, "bus.db")
	t.Setenv("BIRDIE_DB", db)
	st, err := store.Open(db)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	if err := st.CreateAgent(ctx, bus.Agent{Name: "claude", Delivery: bus.DeliveryPull}); err != nil {
		t.Fatal(err)
	}
	b := bus.New(st, nil)
	for _, id := range []string{"a1b2", "c3d4"} {
		if err := b.ReportSession(ctx, "claude", id, bus.SessionInput{Name: "lcm-x"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.EndSession(ctx, "claude", "c3d4"); err != nil {
		t.Fatal(err)
	}
	return st, func(target string) (string, error) {
		t.Helper()
		f, err := os.CreateTemp(dir, "stdout")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		stdout := os.Stdout
		defer func() { os.Stdout = stdout }()
		os.Stdout = f
		runErr := session(ctx, []string{"unranked", target, "p3"})
		out, err := os.ReadFile(f.Name())
		if err != nil {
			t.Fatal(err)
		}
		return string(out), runErr
	}
}
