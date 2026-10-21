// Package scanner provides bounded, fail-closed adapters for malware scanners.
// Provider responses are normalized here; raw engine output is never returned.
package scanner

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"
)

type Status string

const (
	Clean     Status = "clean"
	Infected  Status = "infected"
	Encrypted Status = "encrypted"
	ScanError Status = "error"
)

type Verdict struct {
	Status     Status        `json:"status"`
	Provider   string        `json:"provider"`
	ReasonCode string        `json:"reason_code,omitempty"`
	Retryable  bool          `json:"retryable,omitempty"`
	Duration   time.Duration `json:"-"`
}

type File struct {
	Path string
	Name string
	Size int64
}

type Provider interface {
	Name() string
	Scan(context.Context, File) Verdict
}

var ErrCircuitOpen = errors.New("scanner: circuit open")

// CircuitBreaker turns provider outages into bounded retryable verdicts.
type CircuitBreaker struct {
	Provider Provider
	Failures int
	Cooldown time.Duration
	n        int
	opened   time.Time
	mu       sync.Mutex
}

// Name preserves the wrapped provider identity for aggregated verdicts.
func (c *CircuitBreaker) Name() string {
	if c == nil || c.Provider == nil {
		return "unknown"
	}
	return c.Provider.Name()
}

func (c *CircuitBreaker) Scan(ctx context.Context, f File) Verdict {
	if c.Provider == nil {
		return Verdict{Status: ScanError, Provider: "unknown", Retryable: false, ReasonCode: "not_configured"}
	}
	if c.Failures <= 0 {
		c.Failures = 3
	}
	if c.Cooldown <= 0 {
		c.Cooldown = 30 * time.Second
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.guarded(ctx, f)
}

// guarded is serialized by a package-local lock in production callers; this
// method keeps the API value-based and avoids exposing provider internals.
func (c *CircuitBreaker) guarded(ctx context.Context, f File) Verdict {
	now := time.Now()
	if c.opened.After(now) {
		return Verdict{Status: ScanError, Provider: c.Provider.Name(), Retryable: true, ReasonCode: "circuit_open"}
	}
	v := c.Provider.Scan(ctx, f)
	if v.Status == ScanError && v.Retryable {
		c.n++
		if c.n >= c.Failures {
			c.opened = now.Add(c.Cooldown)
			c.n = 0
		}
	} else if v.Status != ScanError {
		c.n = 0
	}
	return v
}

func clientOrDefault(c *http.Client, timeout time.Duration) *http.Client {
	if c != nil {
		return c
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &http.Client{Timeout: timeout}
}
