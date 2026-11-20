package store_test

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

// One claim returns no more content than MaxBatchBytes, but always at least
// one message.
func TestAClaimStaysWithinItsByteBudget(t *testing.T) {
	ctx := context.Background()
	st, err := store.Open(filepath.Join(t.TempDir(), "bus.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	now := time.Now()
	sizes := []int{bus.MaxBatchBytes + 1, bus.MaxBatchBytes / 2, bus.MaxBatchBytes / 2, 10, 10}
	for i, n := range sizes {
		m := bus.Message{ID: fmt.Sprintf("msg_%d", i), ThreadID: "thr", From: "a", To: "b", Content: strings.Repeat("x", n), Status: bus.StatusQueued, CreatedAt: now}
		if err := st.InsertMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	claim := func() []string {
		t.Helper()
		got, _, err := st.ClaimInbox(ctx, bus.ClaimFilter{Agent: "b", Unpinned: true, Limit: 50}, now)
		if err != nil {
			t.Fatal(err)
		}
		var ids []string
		for _, m := range got {
			ids = append(ids, m.ID)
		}
		return ids
	}
	for _, want := range []string{"[msg_0]", "[msg_1 msg_2]", "[msg_3 msg_4]", "[]"} {
		if got := fmt.Sprint(claim()); got != want {
			t.Fatalf("claim = %s, want %s", got, want)
		}
	}
}
