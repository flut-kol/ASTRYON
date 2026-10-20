package api

import (
	"bytes"
	"io"
	"math"
	"strconv"
	"sync"
	"time"
)

func fmtInt(i int) string { return strconv.Itoa(i) }

// RateLimiter is a simple per-key (tenant) token bucket.
type RateLimiter struct {
	mu     sync.Mutex
	rps    float64
	burst  float64
	now    func() time.Time
	b      map[string]*bucket
	lastGC time.Time
}

type bucket struct {
	tokens float64
	at     time.Time
}

func NewRateLimiter(rps float64, burst int) *RateLimiter {
	return &RateLimiter{rps: rps, burst: float64(burst), now: time.Now, b: map[string]*bucket{}}
}

// Allow consumes a token; if denied, retryAfter is whole seconds (>=1).
func (l *RateLimiter) Allow(key string) (ok bool, retryAfter int) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.now()
	if now.Sub(l.lastGC) > time.Minute {
		for k, v := range l.b {
			if now.Sub(v.at) > time.Minute {
				delete(l.b, k)
			}
		}
		l.lastGC = now
	}
	b := l.b[key]
	if b == nil {
		b = &bucket{tokens: l.burst, at: now}
		l.b[key] = b
	}
	b.tokens = math.Min(l.burst, b.tokens+now.Sub(b.at).Seconds()*l.rps)
	b.at = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0
	}
	return false, int(math.Max(1, math.Ceil((1-b.tokens)/l.rps)))
}

func bytesReader(b []byte) io.Reader { return bytes.NewReader(b) }
