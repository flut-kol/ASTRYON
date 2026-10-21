// Package worker drives claimed file jobs: range-read the immutable object from the last checkpoint, parse
// NDJSON in bounded memory, hand records to an idempotent sink, then advance the checkpoint.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
)

// JobStore is the subset of *jobs.Store the worker needs.
type JobStore interface {
	Claim(ctx context.Context) (jobs.Job, error)
	Heartbeat(ctx context.Context, j jobs.Job) error
	SaveCheckpoint(ctx context.Context, j jobs.Job, checkpoint json.RawMessage, processed, failed int64) error
	Succeed(ctx context.Context, j jobs.Job) error
	Fail(ctx context.Context, j jobs.Job, code string, retryable bool) (jobs.State, error)
}

// Source resolves a job's file object. Fingerprint identifies immutable content (the verified SHA-256), Open
// range-reads from a byte offset.
type Source interface {
	Open(ctx context.Context, j jobs.Job, offset int64) (rc io.ReadCloser, fingerprint string, err error)
}

// Item is one parsed record with its idempotency key (parse.Record.DedupKey).
type Item struct {
	Record   parse.Record
	DedupKey string
}

// Sink must be idempotent on Item.DedupKey: a crash between Accept and SaveCheckpoint replays the batch.
// Valid records go to normal processing, Invalid (malformed/oversized) to quarantine; both must be durably
// accepted before Accept returns.
type Sink interface {
	Accept(ctx context.Context, j jobs.Job, valid, invalid []Item) error
}

type permanent struct{ code string }

func (p permanent) Error() string { return "permanent failure: " + p.code }

// Permanent marks an error that must not be retried (the job goes straight to FAILED with code).
func Permanent(code string) error { return permanent{code} }

// Config tunes the worker.
type Config struct {
	Store          JobStore
	Source         Source
	Sink           Sink
	BatchSize      int           // records per sink call and checkpoint, default 500
	MaxRecordBytes int           // default 1 MiB
	Poll           time.Duration // idle wait, default 2s
	Heartbeat      time.Duration // default 15s; must be well below the store lease
}

type Worker struct{ Config }

func New(c Config) *Worker {
	if c.BatchSize <= 0 {
		c.BatchSize = 500
	}
	if c.MaxRecordBytes <= 0 {
		c.MaxRecordBytes = 1 << 20
	}
	if c.Poll <= 0 {
		c.Poll = 2 * time.Second
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 15 * time.Second
	}
	return &Worker{c}
}

// Run claims and processes jobs until ctx ends.
func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		ok, err := w.RunOnce(ctx)
		if err != nil {
			log.Printf("file worker: %v", err)
		}
		if !ok || err != nil {
			select {
			case <-ctx.Done():
			case <-time.After(w.Poll):
			}
		}
	}
	return ctx.Err()
}

// RunOnce claims at most one job and processes it. ok=false means nothing was due.
func (w *Worker) RunOnce(ctx context.Context) (bool, error) {
	j, err := w.Store.Claim(ctx)
	if errors.Is(err, jobs.ErrNoJob) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, w.process(ctx, j)
}

func (w *Worker) process(parent context.Context, j jobs.Job) error {
	ctx, cancel := context.WithCancelCause(parent)
	defer cancel(nil)
	go func() { // lease keeper: losing the fence stops the work immediately
		t := time.NewTicker(w.Heartbeat)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := w.Store.Heartbeat(ctx, j); errors.Is(err, jobs.ErrFenced) {
					cancel(jobs.ErrFenced)
					return
				}
			}
		}
	}()

	err := w.run(ctx, j)
	if cause := context.Cause(ctx); errors.Is(cause, jobs.ErrFenced) || errors.Is(err, jobs.ErrFenced) {
		return jobs.ErrFenced // someone else owns the job now; write nothing
	}
	// terminal writes use the parent context: a shutdown still records the outcome
	fctx := context.WithoutCancel(parent)
	if err == nil {
		return w.Store.Succeed(fctx, j)
	}
	if parent.Err() != nil {
		return err // shutting down: leave the lease to expire, Sweep re-queues from the checkpoint
	}
	var p permanent
	code, retry := "worker_error", true
	if errors.As(err, &p) {
		code, retry = p.code, false
	}
	if _, ferr := w.Store.Fail(fctx, j, code, retry); ferr != nil {
		return errors.Join(err, ferr)
	}
	return err
}

func (w *Worker) run(ctx context.Context, j jobs.Job) error {
	var cp parse.Checkpoint
	haveCP := len(j.Checkpoint) > 0
	if haveCP {
		if err := json.Unmarshal(j.Checkpoint, &cp); err != nil {
			return Permanent("bad_checkpoint")
		}
	}
	rc, fp, err := w.Source.Open(ctx, j, cp.NextOffset)
	if err != nil {
		return err
	}
	defer rc.Close()
	if !haveCP {
		if cp, err = parse.NewCheckpoint(fp); err != nil {
			return Permanent("no_fingerprint")
		}
	} else if err := cp.ValidateSource(fp); err != nil {
		return Permanent("source_changed")
	}

	var valid, invalid []Item
	var pending []parse.Record
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := w.Sink.Accept(ctx, j, valid, invalid); err != nil {
			return err
		}
		next := cp
		for _, r := range pending {
			var aerr error
			if next, aerr = next.Advance(r); aerr != nil {
				return Permanent("checkpoint_gap")
			}
		}
		raw, _ := json.Marshal(next)
		if err := w.Store.SaveCheckpoint(ctx, j, raw, int64(len(valid)), int64(len(invalid))); err != nil {
			return err
		}
		cp, valid, invalid, pending = next, nil, nil, nil
		return nil
	}
	startOffset, startNumber := cp.NextOffset, cp.NextRecordNumber
	err = parse.NDJSONFrom(rc, w.MaxRecordBytes, startOffset, startNumber, func(r parse.Record) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		key, kerr := r.DedupKey(fp)
		if kerr != nil {
			return Permanent("dedup_key")
		}
		it := Item{Record: r, DedupKey: key}
		if r.Err != nil {
			invalid = append(invalid, it)
		} else {
			valid = append(valid, it)
		}
		pending = append(pending, r)
		if len(pending) >= w.BatchSize {
			return flush()
		}
		return nil
	})
	if err != nil {
		return err
	}
	return flush()
}
