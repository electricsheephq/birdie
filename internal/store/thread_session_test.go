package store_test

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/store"
)

// mainThreadSession is ThreadSession as it was before #29, which only looked
// at messages between the two.
const mainThreadSession = `SELECT CASE WHEN m.sender = ? THEN m.from_session WHEN m.taken_by != '' AND m.status != 'queued' THEN m.taken_by ELSE m.to_session END
	 FROM messages m WHERE m.thread_id = ?
	   AND ((m.sender = ? AND m.recipient = ? AND ((m.taken_by != '' AND m.status != 'queued') OR m.to_session != ''))
	     OR (m.sender = ? AND m.recipient = ? AND m.from_session != ''))
	 ORDER BY m.seq DESC LIMIT 1`

// Wherever the two have exchanged a message in the thread, ThreadSession
// answers exactly as before #29. Only a follow-up between two that never
// exchanged one there takes the new fallback.
func TestThreadSessionIsUnchangedForAPairThatExchangedMessages(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "bus.db")
	st, err := store.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	type pair struct{ from, to string }
	pairs := []pair{{"f", "a"}, {"a", "f"}, {"x", "a"}, {"a", "x"}, {"f", "x"}, {"x", "f"}, {"a", "a"}}
	sessions := []string{"", "", "s1", "s2"}
	statuses := []string{bus.StatusQueued, bus.StatusClaimed, bus.StatusDelivered, bus.StatusReplied, bus.StatusExpired, bus.StatusFailed}
	rng := rand.New(rand.NewSource(29))
	compared, fellBack := 0, 0
	for i := 0; i < 400; i++ {
		thread := fmt.Sprintf("thr_%d", i)
		exchanged := false
		for j, n := 0, 1+rng.Intn(6); j < n; j++ {
			p := pairs[rng.Intn(len(pairs))]
			exchanged = exchanged || (p.from == "f" && p.to == "a") || (p.from == "a" && p.to == "f")
			id := fmt.Sprintf("msg_%d_%d", i, j)
			m := bus.Message{ID: id, ThreadID: thread, From: p.from, To: p.to, FromSession: sessions[rng.Intn(len(sessions))],
				ToSession: sessions[rng.Intn(len(sessions))], Content: "x", Status: statuses[rng.Intn(len(statuses))], CreatedAt: time.Now()}
			if err := st.InsertMessage(ctx, m); err != nil {
				t.Fatal(err)
			}
			if taken := sessions[rng.Intn(len(sessions))]; taken != "" && m.Status != bus.StatusQueued || rng.Intn(4) == 0 {
				if _, err := raw.ExecContext(ctx, `UPDATE messages SET taken_by = ? WHERE id = ?`, taken, id); err != nil {
					t.Fatal(err)
				}
			}
		}
		got, err := st.ThreadSession(ctx, thread, "f", "a")
		if err != nil {
			t.Fatal(err)
		}
		var before string
		if err := raw.QueryRowContext(ctx, mainThreadSession, "a", thread, "f", "a", "a", "f").Scan(&before); err != nil && err != sql.ErrNoRows {
			t.Fatal(err)
		}
		if !exchanged {
			if before != "" {
				t.Fatalf("%s: before #29 %q without an exchange", thread, before)
			}
			if got != "" {
				fellBack++
			}
			continue
		}
		compared++
		if got != before {
			rows, _ := raw.QueryContext(ctx, `SELECT seq, sender, recipient, from_session, to_session, taken_by, status FROM messages WHERE thread_id = ? ORDER BY seq`, thread)
			for rows.Next() {
				var seq int64
				var a, b, c, d, e, f string
				_ = rows.Scan(&seq, &a, &b, &c, &d, &e, &f)
				t.Logf("  %d %s->%s from_session=%q to_session=%q taken_by=%q %s", seq, a, b, c, d, e, f)
			}
			rows.Close()
			t.Fatalf("%s: got %q, before #29 %q", thread, got, before)
		}
	}
	// Both kinds of thread were exercised.
	if compared < 100 || fellBack < 20 {
		t.Fatalf("compared %d threads with an exchange, %d fell back; want more", compared, fellBack)
	}
	t.Logf("compared %d threads with an exchange; %d without one took the fallback", compared, fellBack)
}
