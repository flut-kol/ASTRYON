package retry

import (
	"context"
	"errors"
	"time"
)

// DueClaimer is the persistence boundary used by Scheduler.
type DueClaimer interface {
	ClaimDue(context.Context, int) (int64, error)
}

// Scheduler repeatedly releases due retries. It is safe to run in multiple
// replicas when its store uses SKIP LOCKED (PostgresStore does).
type Scheduler struct {
	Store    DueClaimer
	Interval time.Duration
	Batch    int
	OnError  func(error)
	OnPoll   func(released int64, err error)
}

func (s Scheduler) Validate() error {
	if s.Store == nil || s.Interval <= 0 || s.Batch <= 0 {
		return errors.New("retry: invalid scheduler")
	}
	return nil
}

// Run blocks until ctx is cancelled. A failed poll is reported and retried on
// the next tick; one unavailable database must not stop the controller.
func (s Scheduler) Run(ctx context.Context) error {
	if err := s.Validate(); err != nil {
		return err
	}
	poll := func() {
		released, err := s.Store.ClaimDue(ctx, s.Batch)
		if s.OnPoll != nil {
			s.OnPoll(released, err)
		}
		if err != nil && s.OnError != nil && ctx.Err() == nil {
			s.OnError(err)
		}
	}
	poll()
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			poll()
		}
	}
}
