// Package retrybridge is the opt-in (CONNECTOR_RETRY_MODE=controller) backoff for failed REST pulls. It reuses the
// retry-controller's public classification and policy (pkg/retryclient: Transientf/PermanentErr, Policy.Next) so a
// connector backs off exactly like the other workers.
//
// What it does NOT do: retryclient.Client.Fail and the dispatcher operate on the PostgreSQL `jobs` table and
// `scheduled_retries(job_id -> jobs)`. A pull connector is not a job (state lives in connector_pull_state, fenced
// lease in pullstore), so nothing is written to scheduled_retries and the controller does not dispatch these
// retries; pullstore.Fail still stores next_attempt_at under the fence. Permanent failures cannot be dead-lettered
// (there is no job to mark FAILED); they are parked at the policy's MaxDelay until an operator fixes or pauses
// the connector. Transient failures past MaxAttempts are parked the same way.
package retrybridge

import (
	"errors"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/astryon/retry-controller/pkg/retryclient"
)

// Backoff computes the next delay from retryclient.Policy.
type Backoff struct {
	Policy retryclient.Policy
	Random func() float64 // optional, tests
}

// Transient reports whether a runner failure code is worth retrying with backoff.
func Transient(code string) bool {
	switch code {
	case "pull_error", "emit_failed", "secret_unavailable", "http_408", "http_425", "http_429":
		return true
	}
	return strings.HasPrefix(code, "http_5")
}

// Delay returns the wait before the next pull. failures is the number of consecutive failures before this one.
// retryAfter (from the source) is a minimum.
func (b Backoff) Delay(code string, failures int, cause error, retryAfter time.Duration) (time.Duration, error) {
	if err := b.Policy.Validate(); err != nil {
		return 0, err
	}
	if cause == nil {
		cause = errors.New(code)
	}
	if Transient(code) {
		cause = retryclient.Transientf(cause)
	} else {
		cause = retryclient.PermanentErr(cause)
	}
	d, ok := b.Policy.Next(cause, failures+1, b.Random)
	if !ok {
		d = b.Policy.MaxDelay // permanent or exhausted: parked, never dead-lettered
	}
	if retryAfter > d {
		d = retryAfter
	}
	return d, nil
}

// New returns a Backoff for the given bounds.
func New(base, max time.Duration, attempts int) (*Backoff, error) {
	b := &Backoff{Policy: retryclient.Policy{BaseDelay: base, MaxDelay: max, Jitter: 0.2, MaxAttempts: attempts}, Random: rand.Float64}
	return b, b.Policy.Validate()
}
