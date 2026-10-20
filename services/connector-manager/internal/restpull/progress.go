package restpull

import "time"

// Progress is the persistable scheduling state for one connector. Callers
// persist it atomically with any emitted records. In particular, Succeed must
// be called only after the page has been accepted downstream.
type Progress struct {
	Checkpoint          Checkpoint
	ConsecutiveFailures int
	NextAttemptAt       time.Time
}

// Succeed advances the checkpoint only for a successful, fully handled page
// and clears any retry schedule. A source without more data may omit its next
// cursor, leaving the previous checkpoint intact.
func (p Progress) Succeed(nextCursor string, now time.Time) (Progress, error) {
	if nextCursor != "" {
		checkpoint, err := p.Checkpoint.Advance(nextCursor, now)
		if err != nil {
			return Progress{}, err
		}
		p.Checkpoint = checkpoint
	}
	p.ConsecutiveFailures = 0
	p.NextAttemptAt = time.Time{}
	return p, nil
}

// Fail creates a bounded retry schedule. retryAfter, when supplied by the
// source, is a minimum delay rather than a reason to bypass local backoff.
func (p Progress) Fail(now time.Time, base, max time.Duration, jitter float64, retryAfter time.Duration) (Progress, error) {
	delay, err := NextDelay(p.ConsecutiveFailures, base, max, jitter)
	if err != nil {
		return Progress{}, err
	}
	if retryAfter > delay {
		delay = retryAfter
	}
	p.ConsecutiveFailures++
	p.NextAttemptAt = now.Add(delay)
	return p, nil
}
