package bus

import (
	"runtime"
	"strings"
	"testing"
)

func TestEventPreviewsStopAfterTwoHundredRunes(t *testing.T) {
	prefix := strings.Repeat("界🙂", 100)
	for _, content := range []string{"", "hello", prefix, prefix + "tail", "a\x00b", "\xff\xfehello"} {
		b := &Bus{}
		events, stop := b.Watch()
		b.emitMessage(StatusQueued, Message{Content: content}, "", "")
		stop()
		want := []rune(content)
		if len(want) > 200 {
			want = want[:200]
		}
		if got := (<-events).Text; got != string(want) {
			t.Fatalf("preview = %q, want %q", got, string(want))
		}
	}
}

func TestEventPreviewsAllocateOnlyForThePrefix(t *testing.T) {
	content := strings.Repeat("x", 4<<20)
	b := &Bus{}
	events, stop := b.Watch()
	defer stop()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	b.emitMessage(StatusQueued, Message{Content: content}, "", "")
	runtime.ReadMemStats(&after)
	if got := (<-events).Text; got != strings.Repeat("x", 200) {
		t.Fatalf("preview length = %d, want 200", len(got))
	}
	// Leave room for runtime bookkeeping, but never for the full message.
	if after.TotalAlloc-before.TotalAlloc > 1<<20 {
		t.Fatal("making an event preview allocated more than 1 MiB")
	}
}
