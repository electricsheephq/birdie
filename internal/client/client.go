// Package client is a Go client for the birdie HTTP API.
package client

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"birdie/internal/bus"
)

// Lease is how long a session keeps messages it claimed before confirming them.
const Lease = 2 * time.Minute

// callTimeout bounds the short calls a bridge makes even after its own
// request has ended: session reports, which can outlive the tool call that
// found a conversation change, and confirmations, which outlive the delivery.
// A stalled daemon must not hold them up forever.
const callTimeout = 10 * time.Second

type Client struct {
	BaseURL string
	Token   string
	HTTP    *http.Client
	// Leased claims messages under a lease even without a session, so each
	// must be confirmed with Ack or given back with Nack. Set it before use.
	Leased bool

	mu      sync.RWMutex
	session string
}

// SetSession makes the client act for one session: its inbox is scoped to that
// session, what it sends can be answered there, and messages it claims must be
// confirmed with Ack.
func (c *Client) SetSession(id string) {
	c.mu.Lock()
	c.session = id
	c.mu.Unlock()
}

// Session returns the session the client acts for.
func (c *Client) Session() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.session
}

func New(baseURL, token string) *Client {
	return &Client{BaseURL: strings.TrimRight(baseURL, "/"), Token: token, HTTP: &http.Client{}}
}

type APIError struct {
	Status  int
	Message string
	// Code, for a recipient that names no running session, says why (see
	// bus.AddressError), and Sessions lists the running ones.
	Code     string
	Sessions []bus.SessionRef
}

func (e *APIError) Error() string { return fmt.Sprintf("%s (HTTP %d)", e.Message, e.Status) }

func (c *Client) do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.BaseURL+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+c.Token)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			return fmt.Errorf("birdie daemon not reachable at %s (is `birdie serve` running?): %w", c.BaseURL, err)
		}
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		var e struct {
			Error    string
			Code     string
			Sessions []bus.SessionRef
		}
		if err := json.NewDecoder(io.LimitReader(resp.Body, 64<<10)).Decode(&e); err != nil {
			// A stall while the reason comes in isn't a refusal, whatever
			// the status said.
			var netErr net.Error
			if ctx.Err() != nil || errors.As(err, &netErr) {
				return fmt.Errorf("%s, and its reason didn't arrive: %w", resp.Status, err)
			}
		}
		if e.Error == "" {
			e.Error = resp.Status
		}
		return &APIError{Status: resp.StatusCode, Message: e.Error, Code: e.Code, Sessions: e.Sessions}
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

func (c *Client) Me(ctx context.Context) (bus.Agent, error) {
	var a bus.Agent
	return a, c.do(ctx, http.MethodGet, "/v1/me", nil, &a)
}

func (c *Client) Agents(ctx context.Context) ([]bus.Agent, error) {
	var out struct{ Agents []bus.Agent }
	return out.Agents, c.do(ctx, http.MethodGet, "/v1/agents", nil, &out)
}

func (c *Client) Send(ctx context.Context, in bus.SendInput) (bus.Message, error) {
	if in.FromSession == "" {
		in.FromSession = c.Session()
	}
	var m bus.Message
	return m, c.do(ctx, http.MethodPost, "/v1/messages", in, &m)
}

// Inbox claims queued messages, long-polling up to wait (the server caps it).
func (c *Client) Inbox(ctx context.Context, threadID string, wait time.Duration) ([]bus.Message, error) {
	return c.inbox(ctx, threadID, wait, false, "")
}

// PushInbox is Inbox for a delivery that puts what it gets into the
// conversation on its own. A pull-only session gets nothing from it.
func (c *Client) PushInbox(ctx context.Context, wait time.Duration) ([]bus.Message, error) {
	return c.PushInboxAt(ctx, wait, "")
}

// PushInboxAt supplies the recipient turn position when the host knows it.
func (c *Client) PushInboxAt(ctx context.Context, wait time.Duration, at string) ([]bus.Message, error) {
	return c.inbox(ctx, "", wait, true, at)
}

func (c *Client) inbox(ctx context.Context, threadID string, wait time.Duration, push bool, at string) ([]bus.Message, error) {
	ctx, cancel := context.WithTimeout(ctx, wait+30*time.Second)
	defer cancel()
	q := url.Values{"wait": {strconv.FormatFloat(wait.Seconds(), 'f', -1, 64)}}
	if push {
		q.Set("push", "1")
		if at != "" {
			q.Set("at", at)
		}
	}
	if threadID != "" {
		q.Set("thread_id", threadID)
	}
	id := c.Session()
	if id != "" {
		q.Set("session", id)
	}
	if id != "" || c.Leased {
		q.Set("lease", strconv.FormatFloat(Lease.Seconds(), 'f', -1, 64))
	}
	var out struct{ Messages []bus.Message }
	return out.Messages, c.do(ctx, http.MethodGet, "/v1/inbox?"+q.Encode(), nil, &out)
}

func (c *Client) Thread(ctx context.Context, threadID string) ([]bus.Message, error) {
	var out struct{ Messages []bus.Message }
	return out.Messages, c.do(ctx, http.MethodGet, "/v1/threads/"+url.PathEscape(threadID), nil, &out)
}

// Ack confirms the agent got a claimed message.
func (c *Client) Ack(ctx context.Context, id string, lease ...int64) error {
	return c.settle(ctx, id, "ack", lease)
}

// Nack returns a claimed message to the queue.
func (c *Client) Nack(ctx context.Context, id string, lease ...int64) error {
	return c.settle(ctx, id, "nack", lease)
}

func (c *Client) settle(ctx context.Context, id, verb string, lease []int64) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	path := "/v1/messages/" + url.PathEscape(id) + "/" + verb
	if len(lease) > 0 && lease[0] > 0 {
		path += "?lease_until=" + strconv.FormatInt(lease[0], 10)
	}
	return c.do(ctx, http.MethodPost, path, nil, nil)
}

// Unread counts messages waiting for this client's session.
func (c *Client) Unread(ctx context.Context) (int, error) {
	var out struct{ Unread int }
	path := "/v1/unread"
	if id := c.Session(); id != "" {
		path += "?session=" + url.QueryEscape(id)
	}
	return out.Unread, c.do(ctx, http.MethodGet, path, nil, &out)
}

// ReportSession tells the bus this session is running.
func (c *Client) ReportSession(ctx context.Context, id string, in bus.SessionInput) (pullOnly bool, err error) {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	var out struct {
		PullOnly bool `json:"pull_only"`
	}
	err = c.do(ctx, http.MethodPut, "/v1/sessions/"+url.PathEscape(id), in, &out)
	if err == io.EOF {
		// A daemon from before pull-only answers with no body. Compared
		// exactly: a dropped connection is an EOF wrapped in a *url.Error.
		err = nil
	}
	return out.PullOnly, err
}

// EndSession tells the bus this session has stopped.
func (c *Client) EndSession(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	return c.do(ctx, http.MethodDelete, "/v1/sessions/"+url.PathEscape(id), nil, nil)
}
