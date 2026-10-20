package main

import (
	"context"
	"sync"
	"time"
)

// Limiter paces callers to a steady rate of units per second. It is a
// schedule-based pacer (no burst credit beyond one wait): each WaitN reserves
// the next slot of n/rate seconds, so the long-run average equals the rate even
// with many concurrent callers. rate <= 0 means unlimited.
type Limiter struct {
	mu    sync.Mutex
	rate  float64
	next  time.Time
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) error
}

// NewLimiter returns a real-clock limiter.
func NewLimiter(rate float64) *Limiter {
	return &Limiter{rate: rate, now: time.Now, sleep: ctxSleep}
}

func ctxSleep(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// reserve books n units and returns how long the caller must wait.
func (l *Limiter) reserve(n int) time.Duration {
	if l.rate <= 0 || n <= 0 {
		return 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if l.next.Before(now) {
		l.next = now // idle time is not banked
	}
	wait := l.next.Sub(now)
	l.next = l.next.Add(time.Duration(float64(n) / l.rate * float64(time.Second)))
	return wait
}

// WaitN blocks until n units may be sent, or ctx is done.
func (l *Limiter) WaitN(ctx context.Context, n int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if d := l.reserve(n); d > 0 {
		return l.sleep(ctx, d)
	}
	return nil
}
