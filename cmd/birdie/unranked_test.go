package main

import (
	"context"
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
		{"unranked", "claude/name#id", "p3"},
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
