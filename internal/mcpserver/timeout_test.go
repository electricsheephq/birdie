package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"birdie/internal/bus"
	"birdie/internal/client"
)

func TestAStalledDaemonDoesntHoldCheckInboxOrTheSessionLockForever(t *testing.T) {
	for _, method := range []string{http.MethodPut, http.MethodDelete} {
		t.Run(method, func(t *testing.T) {
			t.Parallel() // each waits out the real ten-second deadline
			entered := make(chan struct{}, 1)
			release := make(chan struct{})
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				io.Copy(io.Discard, r.Body)
				if r.Method == method {
					entered <- struct{}{}
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				io.WriteString(w, `{}`)
			}))
			defer api.Close()
			defer close(release)
			c := client.New(api.URL, "test-token")
			if method == http.MethodDelete {
				c.SetSession("old")
			} else {
				c.SetSession("s1")
			}
			s := New(c, "claude", "test", false, log.New(io.Discard, "", 0))
			s.host = &pullOnlyHost{}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			done := make(chan struct{})
			go func() {
				defer close(done)
				var taken []bus.Message
				s.callTool(ctx, json.RawMessage(`{"name":"check_inbox","arguments":{}}`), &taken, new(string))
			}()
			// Cancellation must still let the conversation change reach the
			// daemon, even though the daemon can no longer hold it forever.
			select {
			case <-entered:
			case <-time.After(2 * time.Second):
				t.Fatal("caller cancellation cut off session synchronization")
			}
			select {
			case <-done:
			case <-time.After(12 * time.Second):
				t.Fatal("stalled session request held check_inbox beyond its deadline")
			}
			if !s.syncMu.TryLock() {
				t.Fatal("check_inbox returned with session synchronization still locked")
			}
			s.syncMu.Unlock()
		})
	}
}
