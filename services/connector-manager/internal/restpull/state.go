package restpull

import (
	"errors"
	"math"
	"time"
)

var ErrInvalid = errors.New("invalid pull state")

type Checkpoint struct {
	Cursor    string
	UpdatedAt time.Time
}

// NextDelay provides bounded exponential retry delay. Jitter is deliberately
// supplied by the caller so it can be deterministic in tests and observable.
func NextDelay(attempt int, base, max time.Duration, jitter float64) (time.Duration, error) {
	if attempt < 0 || base <= 0 || max < base || jitter < 0 || jitter > 1 {
		return 0, ErrInvalid
	}
	exp := float64(base) * math.Pow(2, float64(attempt))
	if exp > float64(max) {
		exp = float64(max)
	}
	return time.Duration(exp * (1 + jitter)), nil
}

func (c Checkpoint) Advance(cursor string, now time.Time) (Checkpoint, error) {
	if cursor == "" || now.Before(c.UpdatedAt) {
		return Checkpoint{}, ErrInvalid
	}
	return Checkpoint{Cursor: cursor, UpdatedAt: now}, nil
}
