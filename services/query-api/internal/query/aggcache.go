package query

import (
	"context"
	"net/url"
	"sync"
	"time"
)

// DefaultAggCacheMax bounds the number of cached aggregate results per process.
const DefaultAggCacheMax = 1000

// aggCache is an opt-in, per-process cache of aggregate results (DPP-050 cache strategy). Entries expire after ttl, so
// a result may be up to ttl old; identical concurrent requests share one ClickHouse query. The key always starts with
// the tenant id taken from the Principal, so a result is never served across tenants. Errors are never cached.
type aggCache struct {
	ttl time.Duration
	max int
	now func() time.Time

	mu       sync.Mutex
	entries  map[string]aggEntry
	inflight map[string]*aggCall
}

type aggEntry struct {
	rows    []map[string]any
	expires time.Time
}

type aggCall struct {
	done chan struct{}
	rows []map[string]any
	err  error
}

func newAggCache(ttl time.Duration, max int) *aggCache {
	if max <= 0 {
		max = DefaultAggCacheMax
	}
	return &aggCache{ttl: ttl, max: max, now: time.Now, entries: map[string]aggEntry{}, inflight: map[string]*aggCall{}}
}

func aggKey(tenant, sql string, params url.Values) string {
	return tenant + "\x00" + sql + "\x00" + params.Encode() // Encode sorts keys, so equal params give equal keys
}

// get returns the cached rows or runs fn once for all concurrent callers of the same key. A caller whose context ends
// stops waiting; the shared query keeps running under the context of the caller that started it.
func (c *aggCache) get(ctx context.Context, key string, fn func() ([]map[string]any, error)) ([]map[string]any, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		if c.now().Before(e.expires) {
			c.mu.Unlock()
			return e.rows, nil
		}
		delete(c.entries, key)
	}
	if call, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-call.done:
			if call.err != nil && ctx.Err() == nil {
				return fn() // the starter failed (maybe its own cancelled context): do not inherit that error
			}
			return call.rows, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	call := &aggCall{done: make(chan struct{})}
	c.inflight[key] = call
	c.mu.Unlock()

	call.rows, call.err = fn()

	c.mu.Lock()
	delete(c.inflight, key)
	if call.err == nil {
		if len(c.entries) >= c.max {
			c.evictLocked()
		}
		c.entries[key] = aggEntry{rows: call.rows, expires: c.now().Add(c.ttl)}
	}
	c.mu.Unlock()
	close(call.done)
	return call.rows, call.err
}

// evictLocked drops expired entries and, if still full, one arbitrary entry.
func (c *aggCache) evictLocked() {
	now := c.now()
	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)
		}
	}
	if len(c.entries) >= c.max {
		for k := range c.entries {
			delete(c.entries, k)
			break
		}
	}
}
