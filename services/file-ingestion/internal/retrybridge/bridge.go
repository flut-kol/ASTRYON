// Package retrybridge is the opt-in (FILE_RETRY_MODE=controller) path that hands failed file jobs to the
// retry-controller's worker client instead of the job store's built-in RETRYING/backoff.
//
// Only Fail is replaced; claim, heartbeat, checkpoint, quarantine and the lease fence stay in jobs.Store, so a
// retried job still resumes from payload.checkpoint. Transient failures (anything the worker marks retryable:
// S3/ClickHouse/PostgreSQL outages) become retryclient.Transientf; permanent ones (malformed file, policy block,
// source changed, row limit) become retryclient.PermanentErr and are dead-lettered (jobs.status FAILED).
// Content-policy QUARANTINED is a separate state written by jobs.Store.Quarantine and never reaches this path.
//
// Attempt accounting differs: jobs.Store increments attempt_count at claim, retryclient at failure. Before calling
// the client the bridge rewinds attempt_count by one (fenced) so a failed attempt is counted exactly once.
package retrybridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/retry-controller/pkg/retryclient"
)

// Failer is the subset of *retryclient.Client used here.
type Failer interface {
	Fail(ctx context.Context, jobID string, cause error) (retryclient.Decision, error)
}

// Ledger does the two file-store-specific writes around the client call.
type Ledger interface {
	// Rewind lowers jobs.attempt_count by one while the job is still RUNNING under this worker's fence.
	Rewind(ctx context.Context, j jobs.Job) error
	// End closes the job_attempts row of this attempt.
	End(ctx context.Context, j jobs.Job, status, code string) error
}

// Inner is the regular job store (jobs.Store satisfies it; so does worker.JobStore).
type Inner interface {
	Claim(ctx context.Context) (jobs.Job, error)
	Heartbeat(ctx context.Context, j jobs.Job) error
	SaveCheckpoint(ctx context.Context, j jobs.Job, checkpoint json.RawMessage, processed, failed int64, labels classify.Labels) error
	SetLabels(ctx context.Context, j jobs.Job, labels classify.Labels) error
	Quarantine(ctx context.Context, j jobs.Job, code string, labels classify.Labels) error
	Succeed(ctx context.Context, j jobs.Job) error
	Fail(ctx context.Context, j jobs.Job, code string, retryable bool) (jobs.State, error)
}

// Store wraps the regular job store; only Fail differs.
type Store struct {
	Inner  // every method except Fail is delegated unchanged
	Ledger Ledger
	// NewClient builds the (per tenant) client; the tenant comes from the claimed job row, never from a request.
	NewClient func(tenantID string) (Failer, error)

	mu      sync.Mutex
	clients map[string]Failer
}

func (s *Store) client(tenant string) (Failer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c, ok := s.clients[tenant]; ok {
		return c, nil
	}
	c, err := s.NewClient(tenant)
	if err != nil {
		return nil, err
	}
	if s.clients == nil {
		s.clients = map[string]Failer{}
	}
	s.clients[tenant] = c
	return c, nil
}

// Fail has the jobs.Store.Fail contract. If the retry client cannot be built the failure falls back to the
// built-in store logic, so a misconfigured controller path never strands a job in RUNNING.
func (s *Store) Fail(ctx context.Context, j jobs.Job, code string, retryable bool) (jobs.State, error) {
	c, err := s.client(j.TenantID)
	if err != nil {
		return s.Inner.Fail(ctx, j, code, retryable)
	}
	cause := errors.New(code)
	if retryable {
		cause = retryclient.Transientf(cause)
	} else {
		cause = retryclient.PermanentErr(cause)
	}
	if err := s.Ledger.Rewind(ctx, j); err != nil {
		return "", err
	}
	d, err := c.Fail(ctx, j.ID, cause)
	if errors.Is(err, retryclient.ErrLeaseLost) {
		return "", jobs.ErrFenced
	}
	if d.Action == "" { // nothing was decided (db error): the lease will expire and Sweep recovers the job
		return "", err
	}
	state, status := jobs.Failed, "FAILED"
	if d.Action == retryclient.ActionRetry || d.Action == retryclient.ActionAlready {
		state, status = jobs.Retrying, "RETRYING"
	}
	if eerr := s.Ledger.End(ctx, j, status, code); eerr != nil {
		err = errors.Join(err, fmt.Errorf("end attempt: %w", eerr))
	}
	return state, err
}
