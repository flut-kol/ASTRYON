package retryclient

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Message is the wake-up published when a retry is due. It is at-least-once:
// consumers must dedupe on (TenantID, JobID, Attempt) and still claim the job
// (QUEUED -> RUNNING) themselves. TenantID is read from the job row, not input.
type Message struct {
	TenantID string `json:"tenant_id"`
	JobID    string `json:"job_id"`
	Kind     string `json:"kind"`
	Attempt  int    `json:"attempt"` // attempts already failed
}

// Publisher is the dispatcher output port. Publish returns nil only when durably accepted.
type Publisher interface {
	Publish(ctx context.Context, m Message) error
}

// PublisherFunc adapts a callback to Publisher.
type PublisherFunc func(ctx context.Context, m Message) error

func (f PublisherFunc) Publish(ctx context.Context, m Message) error { return f(ctx, m) }

// Claim is a due retry held under a fenced lease. Token changes on every claim, so a
// stale holder (lease expired, re-claimed elsewhere) cannot Complete or Release.
type Claim struct {
	Message
	Token string
}

// ErrFenced means the lease was lost (expired and possibly re-claimed).
var ErrFenced = errors.New("retryclient: claim fenced out")

// DispatchStore is the persistence side of the dispatcher.
type DispatchStore interface {
	Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Claim, error)
	Complete(ctx context.Context, c Claim) error // mark fired, job -> QUEUED; requires live lease
	Release(ctx context.Context, c Claim) error  // give the lease back after a publish failure
}

// Dispatcher claims due retries, publishes them and then completes them.
// Crash between claim and publish (or publish and complete) only delays/duplicates
// the wake-up: the lease expires and another claim re-publishes.
type Dispatcher struct {
	Store     DispatchStore
	Publisher Publisher
	Owner     string
	Batch     int
	Lease     time.Duration
	Interval  time.Duration
	OnError   func(error)
}

func (d Dispatcher) validate() error {
	if d.Store == nil || d.Publisher == nil || d.Owner == "" || d.Batch <= 0 || d.Lease <= 0 {
		return errors.New("retryclient: invalid dispatcher")
	}
	return nil
}

// Once runs a single claim/publish/complete cycle and returns the number completed.
func (d Dispatcher) Once(ctx context.Context) (int, error) {
	if err := d.validate(); err != nil {
		return 0, err
	}
	claims, err := d.Store.Claim(ctx, d.Owner, d.Batch, d.Lease)
	if err != nil {
		return 0, err
	}
	done := 0
	var first error
	for _, c := range claims {
		if err := d.Publisher.Publish(ctx, c.Message); err != nil {
			_ = d.Store.Release(context.WithoutCancel(ctx), c)
			if first == nil {
				first = err
			}
			continue
		}
		if err := d.Store.Complete(ctx, c); err != nil {
			if first == nil && !errors.Is(err, ErrFenced) {
				first = err
			}
			continue
		}
		done++
	}
	return done, first
}

// Run polls until ctx is cancelled.
func (d Dispatcher) Run(ctx context.Context) error {
	if err := d.validate(); err != nil {
		return err
	}
	iv := d.Interval
	if iv <= 0 {
		iv = 2 * time.Second
	}
	t := time.NewTicker(iv)
	defer t.Stop()
	for {
		if _, err := d.Once(ctx); err != nil && d.OnError != nil && ctx.Err() == nil {
			d.OnError(err)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
		}
	}
}

// PGDispatchStore is the PostgreSQL DispatchStore. It runs as a trusted platform
// worker (astryon.platform_admin) because it serves all tenants; tenant values it
// emits come from job rows. Do not run it together with retry.Scheduler.
type PGDispatchStore struct{ DB *sql.DB }

func (s PGDispatchStore) Claim(ctx context.Context, owner string, limit int, lease time.Duration) ([]Claim, error) {
	if limit <= 0 {
		return nil, nil
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.platform_admin', 'on', true)`); err != nil {
		return nil, err
	}
	rows, err := tx.QueryContext(ctx, `
WITH due AS (
  SELECT j.tenant_id, j.id FROM scheduled_retries sr
  JOIN jobs j ON j.tenant_id = sr.tenant_id AND j.id = sr.job_id
  WHERE sr.retry_at <= now() AND sr.fired_at IS NULL AND sr.cancelled_at IS NULL
    AND j.status = 'RETRYING' AND (j.locked_until IS NULL OR j.locked_until < now())
  ORDER BY sr.retry_at LIMIT $1 FOR UPDATE OF j SKIP LOCKED
)
UPDATE jobs j SET locked_by = $2 || '#' || gen_random_uuid()::text, locked_until = now() + make_interval(secs => $3)
FROM due WHERE j.tenant_id = due.tenant_id AND j.id = due.id
RETURNING j.tenant_id::text, j.id::text, j.kind, j.attempt_count, j.locked_by`, limit, owner, lease.Seconds())
	if err != nil {
		return nil, err
	}
	var out []Claim
	for rows.Next() {
		var c Claim
		if err := rows.Scan(&c.TenantID, &c.JobID, &c.Kind, &c.Attempt, &c.Token); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, c)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, tx.Commit()
}

func (s PGDispatchStore) Complete(ctx context.Context, c Claim) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.platform_admin', 'on', true)`); err != nil {
		return err
	}
	res, err := tx.ExecContext(ctx, `UPDATE jobs SET status = 'QUEUED', run_after = now(), locked_by = NULL, locked_until = NULL
WHERE tenant_id = $1 AND id = $2 AND status = 'RETRYING' AND locked_by = $3 AND locked_until > now()`, c.TenantID, c.JobID, c.Token)
	if err != nil {
		return err
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return ErrFenced
	}
	if _, err := tx.ExecContext(ctx, `UPDATE scheduled_retries SET fired_at = now() WHERE tenant_id = $1 AND job_id = $2 AND fired_at IS NULL AND cancelled_at IS NULL`, c.TenantID, c.JobID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s PGDispatchStore) Release(ctx context.Context, c Claim) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT set_config('astryon.platform_admin', 'on', true)`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jobs SET locked_by = NULL, locked_until = NULL WHERE tenant_id = $1 AND id = $2 AND status = 'RETRYING' AND locked_by = $3`, c.TenantID, c.JobID, c.Token); err != nil {
		return err
	}
	return tx.Commit()
}

// EncodeMessage is the wire format used by KafkaPublisher.
func EncodeMessage(m Message) ([]byte, error) { return json.Marshal(m) }
