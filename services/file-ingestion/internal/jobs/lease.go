package jobs

import (
	"errors"
	"time"
)

var ErrLeaseOwner = errors.New("lease is not owned by worker")

type Lease struct {
	Owner     string
	ExpiresAt time.Time
}

// Acquire permits an empty or expired lease to be claimed. A live lease cannot
// be stolen by another worker, preventing concurrent file processing.
func (l Lease) Acquire(worker string, now time.Time, ttl time.Duration) (Lease, error) {
	if worker == "" || ttl <= 0 {
		return Lease{}, ErrLeaseOwner
	}
	if l.Owner != "" && l.Owner != worker && now.Before(l.ExpiresAt) {
		return Lease{}, ErrLeaseOwner
	}
	return Lease{Owner: worker, ExpiresAt: now.Add(ttl)}, nil
}

// Heartbeat extends only the owner's still-live lease. Persistence must use a
// compare-and-set on owner and expiry so this decision remains atomic.
func (l Lease) Heartbeat(worker string, now time.Time, ttl time.Duration) (Lease, error) {
	if worker == "" || worker != l.Owner || !now.Before(l.ExpiresAt) || ttl <= 0 {
		return Lease{}, ErrLeaseOwner
	}
	return Lease{Owner: worker, ExpiresAt: now.Add(ttl)}, nil
}
