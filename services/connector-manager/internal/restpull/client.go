package restpull

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidConfig    = errors.New("invalid REST pull configuration")
	ErrResponseTooLarge = errors.New("REST pull response exceeds configured limit")
)

// Config contains only non-secret connector settings. Authentication material
// remains behind the connector's SecretRef and must be attached by a caller
// which resolves that reference.
type Config struct {
	Endpoint          string
	CursorParameter   string
	RequestsPerSecond int
	MaxResponseBytes  int64
}

func (c Config) validate() (*url.URL, error) {
	u, err := url.Parse(c.Endpoint)
	if err != nil || u.Scheme == "" || u.Host == "" || (u.Scheme != "https" && u.Scheme != "http") ||
		c.CursorParameter == "" || c.RequestsPerSecond <= 0 || c.MaxResponseBytes <= 0 {
		return nil, ErrInvalidConfig
	}
	return u, nil
}

// HTTPError keeps retry policy separate from transport code. RetryAfter is
// populated for a valid Retry-After response header, so a scheduler can prefer
// the source's requested pause over exponential backoff.
type HTTPError struct {
	StatusCode int
	RetryAfter time.Duration
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("REST pull returned HTTP %d", e.StatusCode)
}

// Response is deliberately opaque to this transport package: a connector
// adapter validates and emits its records before Progress is marked successful.
// NextCursor comes from X-Next-Cursor, avoiding assumptions about vendor JSON.
type Response struct {
	Body       []byte
	NextCursor string
}

// Client executes cursor-based GET pulls. It is safe for concurrent callers;
// rate slots are reserved before a request is started, which prevents a burst
// after a slow upstream response.
type Client struct {
	endpoint *url.URL
	config   Config
	http     *http.Client
	now      func() time.Time
	wait     func(context.Context, time.Duration) error

	// Authorization, when set, is sent as the Authorization header. A caller resolves it from the connector's
	// SecretRef at run time; it is never stored in Config.
	Authorization string

	mu          sync.Mutex
	nextRequest time.Time
}

func NewClient(config Config, httpClient *http.Client) (*Client, error) {
	endpoint, err := config.validate()
	if err != nil {
		return nil, err
	}
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		endpoint: endpoint,
		config:   config,
		http:     httpClient,
		now:      time.Now,
		wait:     waitContext,
	}, nil
}

// Pull requests one incremental page. The supplied cursor is sent only as a
// URL query parameter; tenant identity and credentials are never inferred from
// response data or caller-controlled headers.
func (c *Client) Pull(ctx context.Context, cursor string) (Response, error) {
	if err := c.reserve(ctx); err != nil {
		return Response{}, err
	}
	u := *c.endpoint
	query := u.Query()
	if cursor != "" {
		query.Set(c.config.CursorParameter, cursor)
	}
	u.RawQuery = query.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return Response{}, err
	}
	if c.Authorization != "" {
		req.Header.Set("Authorization", c.Authorization)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Response{}, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return Response{}, &HTTPError{StatusCode: resp.StatusCode, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), c.now())}
	}
	body, err := readBounded(resp.Body, c.config.MaxResponseBytes)
	if err != nil {
		return Response{}, err
	}
	return Response{Body: body, NextCursor: resp.Header.Get("X-Next-Cursor")}, nil
}

func (c *Client) reserve(ctx context.Context) error {
	c.mu.Lock()
	now := c.now()
	at := now
	if c.nextRequest.After(at) {
		at = c.nextRequest
	}
	// Use integer division only after validation; at least one nanosecond keeps
	// an accidentally huge configured rate from disabling the limiter.
	interval := time.Second / time.Duration(c.config.RequestsPerSecond)
	if interval < time.Nanosecond {
		interval = time.Nanosecond
	}
	c.nextRequest = at.Add(interval)
	c.mu.Unlock()

	// Use the same clock for both reservation and delay calculation. Besides
	// keeping tests deterministic, this avoids bypassing the limiter when a
	// caller supplies a monotonic/test clock whose epoch differs from time.Now.
	return c.wait(ctx, at.Sub(now))
}

func waitContext(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readBounded(r io.Reader, maximum int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(r, maximum+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > maximum {
		return nil, ErrResponseTooLarge
	}
	return body, nil
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil && at.After(now) {
		return at.Sub(now)
	}
	return 0
}
