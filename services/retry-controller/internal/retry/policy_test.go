package retry

import (
	"errors"
	"testing"
	"time"
)

type transientError struct{}

func (transientError) Error() string     { return "temporary" }
func (transientError) RetryClass() Class { return Transient }

func TestNextBoundsExponentialBackoff(t *testing.T) {
	p := Policy{BaseDelay: time.Second, MaxDelay: 8 * time.Second, MaxAttempts: 4}
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 4: 8 * time.Second} {
		got, ok := p.Next(transientError{}, attempt, func() float64 { return .5 })
		if !ok || got != want {
			t.Fatalf("attempt %d: %v %v", attempt, got, ok)
		}
	}
	if _, ok := p.Next(transientError{}, 5, nil); ok {
		t.Fatal("must stop at max attempts")
	}
}
func TestNextRejectsUnknownError(t *testing.T) {
	if _, ok := (Policy{BaseDelay: time.Second, MaxDelay: time.Second, MaxAttempts: 1}).Next(errors.New("timeout"), 1, nil); ok {
		t.Fatal("unknown errors must be permanent")
	}
}
