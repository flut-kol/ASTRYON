// Package retry contains deterministic, bounded retry scheduling policy.
package retry

import (
	"errors"
	"math/rand/v2"
	"time"
)

// Class identifies whether an error may be retried.
type Class string

const (
	Transient Class = "transient"
	Permanent Class = "permanent"
)

// Error lets callers classify failures without parsing error text.
type Error interface {
	error
	RetryClass() Class
}

// Policy bounds exponential retry delay and randomizes it to avoid retry storms.
type Policy struct {
	BaseDelay   time.Duration
	MaxDelay    time.Duration
	Jitter      float64 // 0..1, symmetric fraction of delay
	MaxAttempts int
}

// Validate checks that the policy cannot schedule an unbounded or negative delay.
func (p Policy) Validate() error {
	if p.BaseDelay <= 0 || p.MaxDelay < p.BaseDelay || p.MaxAttempts <= 0 || p.Jitter < 0 || p.Jitter > 1 {
		return errors.New("retry: invalid policy")
	}
	return nil
}

// Next returns a retry delay for the next one-based attempt, or false when the
// error is permanent or the attempt budget is exhausted.
func (p Policy) Next(err error, nextAttempt int, random func() float64) (time.Duration, bool) {
	if p.Validate() != nil || nextAttempt < 1 || nextAttempt > p.MaxAttempts || Classify(err) != Transient {
		return 0, false
	}
	delay := p.BaseDelay
	for i := 1; i < nextAttempt && delay < p.MaxDelay; i++ {
		if delay > p.MaxDelay/2 {
			delay = p.MaxDelay
			break
		}
		delay *= 2
	}
	if random == nil {
		random = rand.Float64
	}
	if p.Jitter > 0 {
		delay = time.Duration(float64(delay) * (1 - p.Jitter + 2*p.Jitter*random()))
	}
	return delay, true
}

// Classify defaults unknown errors to permanent: callers must explicitly mark
// a failure transient before a job can be rescheduled.
func Classify(err error) Class {
	var classified Error
	if errors.As(err, &classified) && classified.RetryClass() == Transient {
		return Transient
	}
	return Permanent
}
