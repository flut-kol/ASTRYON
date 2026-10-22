package retrybridge

import (
	"errors"
	"testing"
	"time"
)

func TestDelay(t *testing.T) {
	b, err := New(10*time.Second, 5*time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	b.Random = func() float64 { return .5 } // no jitter shift
	cause := errors.New("x")
	for _, c := range []struct {
		name, code string
		failures   int
		retryAfter time.Duration
		want       time.Duration
	}{
		{"first transient", "http_503", 0, 0, 10 * time.Second},
		{"second doubles", "pull_error", 1, 0, 20 * time.Second},
		{"exhausted parks at max", "emit_failed", 3, 0, 5 * time.Minute},
		{"permanent parks at max", "bad_config", 0, 0, 5 * time.Minute},
		{"4xx permanent", "http_404", 0, 0, 5 * time.Minute},
		{"429 transient", "http_429", 0, 0, 10 * time.Second},
		{"retry-after is a minimum", "http_429", 0, time.Minute, time.Minute},
		{"retry-after smaller ignored", "http_503", 1, time.Second, 20 * time.Second},
	} {
		got, err := b.Delay(c.code, c.failures, cause, c.retryAfter)
		if err != nil || got != c.want {
			t.Errorf("%s: got %v %v want %v", c.name, got, err, c.want)
		}
	}
}

func TestNewRejectsInvalid(t *testing.T) {
	if _, err := New(0, time.Second, 1); err == nil {
		t.Fatal("zero base must be rejected")
	}
	if _, err := New(time.Minute, time.Second, 1); err == nil {
		t.Fatal("max < base must be rejected")
	}
}
