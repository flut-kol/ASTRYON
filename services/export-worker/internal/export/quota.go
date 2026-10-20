package export

import (
	"errors"
	"sync"
	"time"
)

var (
	ErrQuotaExceeded     = errors.New("daily export quota exceeded")
	ErrTooManyConcurrent = errors.New("too many concurrent exports")
)

// QuotaChecker is the per-tenant rate/quota guard. Acquire reserves one export slot and returns a release
// func that must be called when the export finishes. PGQuota (pg.go) is the durable, replica-safe
// implementation; the in-memory one is per-process only.
type QuotaChecker interface {
	Acquire(tenantID string) (release func(), err error)
}

// MemoryQuota allows MaxPerDay exports per tenant per UTC day and MaxConcurrent in flight.
type MemoryQuota struct {
	MaxPerDay     int
	MaxConcurrent int
	Now           func() time.Time

	mu     sync.Mutex
	day    string
	perDay map[string]int
	active map[string]int
}

func (q *MemoryQuota) Acquire(tenantID string) (func(), error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now
	if q.Now != nil {
		now = q.Now
	}
	d := now().UTC().Format("2006-01-02")
	if q.perDay == nil || q.day != d {
		q.day, q.perDay = d, map[string]int{}
	}
	if q.active == nil {
		q.active = map[string]int{}
	}
	if q.MaxPerDay > 0 && q.perDay[tenantID] >= q.MaxPerDay {
		return nil, ErrQuotaExceeded
	}
	if q.MaxConcurrent > 0 && q.active[tenantID] >= q.MaxConcurrent {
		return nil, ErrTooManyConcurrent
	}
	q.perDay[tenantID]++
	q.active[tenantID]++
	var once sync.Once
	return func() {
		once.Do(func() {
			q.mu.Lock()
			q.active[tenantID]--
			q.mu.Unlock()
		})
	}, nil
}
