package client

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"birdie/internal/bus"
)

type deadlineTransport func(*http.Request) (*http.Response, error)

func (f deadlineTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestSessionAndConfirmationCallsHaveADeadline(t *testing.T) {
	c := New("http://daemon", "test-token")
	c.HTTP.Transport = deadlineTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) <= 0 || time.Until(deadline) > 10*time.Second {
			t.Errorf("%s %s has no deadline within ten seconds", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	if _, err := c.ReportSession(context.Background(), "s1", bus.SessionInput{}); err != nil {
		t.Fatal(err)
	}
	if err := c.EndSession(context.Background(), "s1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(context.Background(), "m1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Nack(context.Background(), "m1"); err != nil {
		t.Fatal(err)
	}
}

func TestInboxDeadlinesLeaveTimeForTheWholeLongPoll(t *testing.T) {
	c := New("http://daemon", "test-token")
	wait := 120 * time.Second
	c.HTTP.Transport = deadlineTransport(func(r *http.Request) (*http.Response, error) {
		deadline, ok := r.Context().Deadline()
		if !ok || time.Until(deadline) < wait || time.Until(deadline) > wait+30*time.Second {
			t.Error("inbox deadline must allow the requested wait plus a bounded margin")
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{"messages":[]}`))}, nil
	})
	if _, err := c.Inbox(context.Background(), "", wait); err != nil {
		t.Fatal(err)
	}
	if _, err := c.PushInbox(context.Background(), wait); err != nil {
		t.Fatal(err)
	}
}

func TestBoundedCallsKeepAShorterCallerDeadline(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	want, _ := ctx.Deadline()
	c := New("http://daemon", "test-token")
	c.HTTP.Transport = deadlineTransport(func(r *http.Request) (*http.Response, error) {
		if got, ok := r.Context().Deadline(); !ok || !got.Equal(want) {
			t.Errorf("%s %s replaced the caller's earlier deadline", r.Method, r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(`{}`))}, nil
	})
	if _, err := c.ReportSession(ctx, "s1", bus.SessionInput{}); err != nil {
		t.Fatal(err)
	}
	if err := c.EndSession(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Ack(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
	if err := c.Nack(ctx, "m1"); err != nil {
		t.Fatal(err)
	}
}
