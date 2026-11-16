package bus_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

type noticeStore struct {
	bus.Store
	threadReads int
}

func (s *noticeStore) Thread(ctx context.Context, thread, agent string) ([]bus.Message, error) {
	s.threadReads++
	return s.Store.Thread(ctx, thread, agent)
}

func TestNoticesCheckForDuplicatesWithoutReadingTheThread(t *testing.T) {
	ctx := context.Background()
	_, st := newBus(t)
	s := &noticeStore{Store: st}
	b := bus.New(s, nil)
	content := strings.Repeat("界", 1<<20)
	original := bus.Message{ID: "original", ThreadID: "thread", From: "alice", To: "bob",
		Content: content, Status: bus.StatusQueued, CreatedAt: time.Now()}
	if err := st.InsertMessage(ctx, original); err != nil {
		t.Fatal(err)
	}
	// Neither another recipient nor a user-supplied notice marker is a
	// duplicate. A notice from the legacy sender, and a system message with no
	// kind (kind ""), are: the check that read the whole thread treated them so.
	for i, m := range []bus.Message{
		{From: bus.SystemSender, To: "bob", Meta: map[string]string{"notice": "failed"}},
		{From: "bob", To: "alice", Meta: map[string]string{"notice": "failed"}},
		{From: bus.LegacySystemSender, To: "alice", Meta: map[string]string{"notice": "legacy"}},
		{From: bus.SystemSender, To: "alice"},
	} {
		m.ID, m.ThreadID, m.Content, m.CreatedAt = fmt.Sprint(i), original.ThreadID, content, original.CreatedAt
		if err := st.InsertMessage(ctx, m); err != nil {
			t.Fatal(err)
		}
	}
	for _, kind := range []string{"failed", "failed", "legacy", "", "different"} {
		if err := b.Notice(ctx, original, kind, "send a new message"); err != nil {
			t.Fatal(err)
		}
	}
	if s.threadReads != 0 {
		t.Errorf("notices read the full thread %d times", s.threadReads)
	}
	thread, err := b.Thread(ctx, original.ThreadID, "alice")
	if err != nil {
		t.Fatal(err)
	}
	sent := map[string]int{}
	for _, m := range thread {
		if strings.HasPrefix(m.ID, "msg_") {
			if m.From != bus.SystemSender || m.To != "alice" || m.InReplyTo != original.ID {
				t.Errorf("unexpected new message %+v", m.ID)
			}
			sent[m.Meta["notice"]]++
		}
	}
	if len(sent) != 2 || sent["failed"] != 1 || sent["different"] != 1 {
		t.Fatalf("new notices by kind = %v, want failed and different once each", sent)
	}
	if len(thread) != 6 {
		t.Fatalf("thread has %d messages, want 6", len(thread))
	}
	if thread[0].Content != content {
		t.Fatal("reading a thread no longer returns the full content")
	}
}
