package client

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"birdie/internal/bus"
)

// A daemon from before pull-only answers a report with no body; one that
// drops the connection is an error, not a report that went through.
func TestReportSessionAnswers(t *testing.T) {
	old := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	defer old.Close()
	if on, err := New(old.URL, "t").ReportSession(context.Background(), "s1", bus.SessionInput{}); err != nil || on {
		t.Fatalf("old daemon: %v, %v", on, err)
	}
	drop := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}))
	defer drop.Close()
	if _, err := New(drop.URL, "t").ReportSession(context.Background(), "s1", bus.SessionInput{}); err == nil {
		t.Fatal("a dropped connection counted as reported")
	}
}
