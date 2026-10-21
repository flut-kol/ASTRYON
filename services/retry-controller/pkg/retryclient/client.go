// Package retryclient is the worker-facing API of the retry-controller.
//
// Worker side: Client.Fail classifies a job failure, and either schedules a
// bounded, jittered retry (transient, attempts left) or dead-letters the job
// (permanent, or attempts exhausted). It is idempotent per job id and fenced by
// the worker's lease (jobs.locked_by). The tenant comes only from Config, which
// the caller must build from validated token claims or trusted service config,
// never from a request header or body.
//
// Controller side: Dispatcher claims due retries with a fenced lease and
// publishes them through a Publisher (Kafka or callback), see dispatch.go.
package retryclient

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"time"

	"github.com/astryon/retry-controller/internal/retry"
)

// Re-exported classification so workers need not import internal packages.
type (
	Class  = retry.Class
	Policy = retry.Policy
)

const (
	Transient = retry.Transient
	Permanent = retry.Permanent
)

// Transientf wraps err as a transient (retryable) failure. Unclassified errors are permanent.
func Transientf(err error) error { return classified{err, Transient} }

// PermanentErr marks err permanent explicitly.
func PermanentErr(err error) error { return classified{err, Permanent} }

type classified struct {
	error
	c Class
}

func (c classified) RetryClass() Class { return c.c }
func (c classified) Unwrap() error     { return c.error }

// Action is the outcome of Client.Fail.
type Action string

const (
	ActionRetry       Action = "retry"       // retry scheduled by this call
	ActionAlready     Action = "already"     // an earlier call already scheduled it (idempotent replay)
	ActionDeadLetter  Action = "dead_letter" // job moved to FAILED (dead-letter) by this call
	ActionDeadAlready Action = "dead_letter_already"
)

// Decision describes what Fail did.
type Decision struct {
	Action  Action
	Attempt int           // attempt number that just failed
	Delay   time.Duration // zero unless ActionRetry
	Reason  string
}

var (
	// ErrLeaseLost: the job is not RUNNING under this worker's lease; the failure must not be recorded.
	ErrLeaseLost = errors.New("retryclient: job not running under this worker's lease")
	ErrNilError  = errors.New("retryclient: nil error")
)

// DeadLetterSink is notified after a job was durably dead-lettered (e.g. publish to a DLQ topic).
// Failures are returned from Fail but the dead-letter state is already committed.
type DeadLetterSink interface {
	DeadLetter(ctx context.Context, tenantID, jobID string, attempts int, reason string) error
}

// Config configures a worker-side client.
type Config struct {
	TenantID string // from validated claims / trusted config only
	WorkerID string // must equal jobs.locked_by of the leased job (fencing)
	Policy   Policy
	Sink     DeadLetterSink // optional
	Random   func() float64 // optional, tests
}

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Client schedules retries for one tenant.
type Client struct {
	db  *sql.DB
	cfg Config
}

// New validates cfg. db must connect as a non-superuser role so tenant RLS applies.
func New(db *sql.DB, cfg Config) (*Client, error) {
	if db == nil {
		return nil, errors.New("retryclient: db required")
	}
	if !uuidRe.MatchString(cfg.TenantID) {
		return nil, errors.New("retryclient: TenantID must be a uuid")
	}
	if cfg.WorkerID == "" {
		return nil, errors.New("retryclient: WorkerID required")
	}
	if err := cfg.Policy.Validate(); err != nil {
		return nil, err
	}
	return &Client{db: db, cfg: cfg}, nil
}

// Fail records a failed attempt of jobID. Safe to call again for the same job and failure.
func (c *Client) Fail(ctx context.Context, jobID string, cause error) (Decision, error) {
	if cause == nil {
		return Decision{}, ErrNilError
	}
	if !uuidRe.MatchString(jobID) {
		return Decision{}, errors.New("retryclient: jobID must be a uuid")
	}
	reason := truncate(cause.Error(), 500)
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return Decision{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.tenant_id', $1, true)`, c.cfg.TenantID); err != nil {
		return Decision{}, err
	}
	var status string
	var attempts, maxAttempts int
	var lockedBy sql.NullString
	err = tx.QueryRowContext(ctx, `SELECT status::text, attempt_count, max_attempts, locked_by FROM jobs WHERE tenant_id = $1 AND id = $2 FOR UPDATE`, c.cfg.TenantID, jobID).Scan(&status, &attempts, &maxAttempts, &lockedBy)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Decision{}, fmt.Errorf("%w: job not found", ErrLeaseLost)
		}
		return Decision{}, err
	}
	switch status {
	case "RETRYING":
		return Decision{Action: ActionAlready, Attempt: attempts, Reason: reason}, nil
	case "FAILED":
		return Decision{Action: ActionDeadAlready, Attempt: attempts, Reason: reason}, nil
	case "RUNNING":
		if !lockedBy.Valid || lockedBy.String != c.cfg.WorkerID {
			return Decision{}, ErrLeaseLost
		}
	default:
		return Decision{}, ErrLeaseLost
	}
	next := attempts + 1
	pol := c.cfg.Policy
	if maxAttempts < pol.MaxAttempts {
		pol.MaxAttempts = maxAttempts
	}
	delay, ok := pol.Next(cause, next, c.cfg.Random)
	if !ok {
		why := "permanent: " + reason
		if retry.Classify(cause) == Transient {
			why = "max attempts exhausted: " + reason
		}
		if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status = 'FAILED', attempt_count = $3, finished_at = now(), locked_by = NULL, locked_until = NULL, last_error = $4 WHERE tenant_id = $1 AND id = $2`, c.cfg.TenantID, jobID, next, "dead-letter: "+why); err != nil {
			return Decision{}, err
		}
		if err := tx.Commit(); err != nil {
			return Decision{}, err
		}
		d := Decision{Action: ActionDeadLetter, Attempt: next, Reason: why}
		if c.cfg.Sink != nil {
			if err := c.cfg.Sink.DeadLetter(ctx, c.cfg.TenantID, jobID, next, why); err != nil {
				return d, fmt.Errorf("retryclient: dead-letter committed, sink failed: %w", err)
			}
		}
		return d, nil
	}
	secs := delay.Seconds()
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET status = 'RETRYING', attempt_count = $3, run_after = now() + make_interval(secs => $4), locked_by = NULL, locked_until = NULL, last_error = $5 WHERE tenant_id = $1 AND id = $2`, c.cfg.TenantID, jobID, next, secs, reason); err != nil {
		return Decision{}, err
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO scheduled_retries (tenant_id, job_id, retry_at, reason, backoff_ms) VALUES ($1, $2, now() + make_interval(secs => $3), $4, $5)`, c.cfg.TenantID, jobID, secs, reason, delay.Milliseconds()); err != nil {
		return Decision{}, err
	}
	if err := tx.Commit(); err != nil {
		return Decision{}, err
	}
	return Decision{Action: ActionRetry, Attempt: next, Delay: delay, Reason: reason}, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
