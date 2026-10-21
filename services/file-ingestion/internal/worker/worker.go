// Package worker drives claimed file jobs: range-read the immutable object from the last checkpoint, parse
// NDJSON in bounded memory, hand records to an idempotent sink, then advance the checkpoint.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/parse"
	"github.com/astryon/file-ingestion/internal/profile"
)

// JobStore is the subset of *jobs.Store the worker needs.
type JobStore interface {
	Claim(ctx context.Context) (jobs.Job, error)
	Heartbeat(ctx context.Context, j jobs.Job) error
	SaveCheckpoint(ctx context.Context, j jobs.Job, checkpoint json.RawMessage, processed, failed int64, labels classify.Labels) error
	SetLabels(ctx context.Context, j jobs.Job, labels classify.Labels) error
	Quarantine(ctx context.Context, j jobs.Job, code string, labels classify.Labels) error
	Succeed(ctx context.Context, j jobs.Job) error
	Fail(ctx context.Context, j jobs.Job, code string, retryable bool) (jobs.State, error)
}

// Source resolves a job's file object. Fingerprint identifies immutable content (the verified SHA-256), Open
// range-reads from a byte offset.
type Source interface {
	Open(ctx context.Context, j jobs.Job, offset int64) (rc io.ReadCloser, fingerprint string, err error)
}

// RandomSource is implemented by sources that can serve random-access reads. Parquet needs it: the footer sits at
// the end of the object and row groups are read independently. size is the verified object size.
type RandomSource interface {
	OpenAt(ctx context.Context, j jobs.Job) (ra io.ReaderAt, size int64, fingerprint string, err error)
}

// Outputter is optionally implemented by sinks that write an object per batch; its key lands in the batch lineage.
type Outputter interface {
	OutputKey(j jobs.Job, valid []Item) string
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

// quarantined is returned by run when a content policy holds the file back before anything reached the sink.
type quarantined struct {
	code   string
	labels classify.Labels
}

func (q quarantined) Error() string { return "quarantined: " + q.code }

type permanent struct {
	code    string
	blocked bool // content policy block (metrics)
}

func (p permanent) Error() string { return "permanent failure: " + p.code }

// Permanent marks an error that must not be retried (the job goes straight to FAILED with code).
func Permanent(code string) error { return permanent{code: code} }

// Config tunes the worker.
type Config struct {
	Store          JobStore
	Source         Source
	Sink           Sink
	BatchSize      int           // records per sink call and checkpoint, default 500
	MaxRecordBytes int           // default 1 MiB
	Poll           time.Duration // idle wait, default 2s
	Heartbeat      time.Duration // default 15s; must be well below the store lease
	// Policies is the credentials policy (env default plus tenant/source overrides); the zero value quarantines.
	// A source-type profile's own policy sits between an explicit override and the default (profile.EffectiveCredentialsPolicy).
	Policies classify.PolicySet
	// Lineage, when set, records source file -> batch -> (output) -> pipeline version for every batch the sink accepted.
	Lineage jobs.LineageRecorder
	// PipelineVersion labels lineage and normalized output; default "file-ingestion/1".
	PipelineVersion string
	// LineageDestination is the destination recorded for sinks that are not Outputters; default CLICKHOUSE_BULK.
	LineageDestination jobs.Destination
	// Parquet bounds the Parquet reader (MaxRecordBytes is taken from the profile/worker setting).
	Parquet parse.ParquetLimits
}

// Metrics are plain atomic counters; Prometheus text via WritePrometheus.
type Metrics struct {
	Claimed, Succeeded, Failed, Retried, Fenced, Batches, Records, InvalidRecords, SinkErrors atomic.Int64

	mu      sync.Mutex
	labels  map[string]int64 // content label -> jobs it was found in (no values, ever)
	actions map[string]int64 // quarantine|block
}

func (m *Metrics) incLabels(l classify.Labels) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.labels == nil {
		m.labels = map[string]int64{}
	}
	for _, n := range l.Names() {
		m.labels[n]++
	}
}

func (m *Metrics) incAction(a string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.actions == nil {
		m.actions = map[string]int64{}
	}
	m.actions[a]++
}

func (m *Metrics) WritePrometheus(w io.Writer) {
	m.mu.Lock()
	fmt.Fprint(w, "# HELP astryon_file_worker_content_labels_total Jobs in which a content label was detected\n# TYPE astryon_file_worker_content_labels_total counter\n")
	for _, l := range classify.AllLabels {
		fmt.Fprintf(w, "astryon_file_worker_content_labels_total{label=%q} %d\n", string(l), m.labels[string(l)])
	}
	fmt.Fprint(w, "# HELP astryon_file_worker_policy_actions_total Jobs held back by a content policy\n# TYPE astryon_file_worker_policy_actions_total counter\n")
	for _, a := range []string{"quarantine", "block"} {
		fmt.Fprintf(w, "astryon_file_worker_policy_actions_total{action=%q} %d\n", a, m.actions[a])
	}
	m.mu.Unlock()
	for _, c := range []struct {
		name, help string
		v          int64
	}{
		{"jobs_claimed_total", "Jobs claimed", m.Claimed.Load()},
		{"jobs_succeeded_total", "Jobs finished SUCCEEDED", m.Succeeded.Load()},
		{"jobs_failed_total", "Jobs finished FAILED (permanent or retries exhausted)", m.Failed.Load()},
		{"jobs_retried_total", "Attempts ended with a retry scheduled", m.Retried.Load()},
		{"jobs_fenced_total", "Jobs abandoned because the lease was lost", m.Fenced.Load()},
		{"batches_total", "Batches accepted by the sink and checkpointed", m.Batches.Load()},
		{"records_total", "Valid records written", m.Records.Load()},
		{"invalid_records_total", "Records routed to processing_errors", m.InvalidRecords.Load()},
		{"sink_errors_total", "Sink failures", m.SinkErrors.Load()},
	} {
		fmt.Fprintf(w, "# HELP astryon_file_worker_%s %s\n# TYPE astryon_file_worker_%s counter\nastryon_file_worker_%s %d\n", c.name, c.help, c.name, c.name, c.v)
	}
}

type Worker struct {
	Config
	M Metrics
}

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
	if c.PipelineVersion == "" {
		c.PipelineVersion = "file-ingestion/1"
	}
	if c.LineageDestination == "" {
		c.LineageDestination = jobs.ClickHouseBulk
	}
	return &Worker{Config: c}
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
	w.M.Claimed.Add(1)
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

	seen := classify.Labels{}
	err := w.run(ctx, j, seen)
	if cause := context.Cause(ctx); errors.Is(cause, jobs.ErrFenced) || errors.Is(err, jobs.ErrFenced) {
		w.M.Fenced.Add(1)
		return jobs.ErrFenced // someone else owns the job now; write nothing
	}
	// terminal writes use the parent context: a shutdown still records the outcome
	fctx := context.WithoutCancel(parent)
	var q quarantined
	if errors.As(err, &q) {
		w.M.incLabels(q.labels)
		w.M.incAction("quarantine")
		if qerr := w.Store.Quarantine(fctx, j, q.code, q.labels); qerr != nil {
			return qerr
		}
		return nil
	}
	if err == nil {
		w.M.incLabels(seen)
		w.M.Succeeded.Add(1)
		return w.Store.Succeed(fctx, j)
	}
	if parent.Err() != nil {
		return err // shutting down: leave the lease to expire, Sweep re-queues from the checkpoint
	}
	var p permanent
	code, retry := "worker_error", true
	if errors.As(err, &p) {
		code, retry = p.code, false
		if p.blocked {
			w.M.incAction("block")
		}
	}
	st, ferr := w.Store.Fail(fctx, j, code, retry)
	if ferr != nil {
		return errors.Join(err, ferr)
	}
	if st == jobs.Retrying {
		w.M.Retried.Add(1)
	} else {
		w.M.Failed.Add(1)
	}
	return err
}

// prescan classifies the whole object in a streaming pass (bounded memory) before any record reaches the sink, so
// quarantine/block really mean "nothing was written". It returns the final labels.
func (w *Worker) prescan(ctx context.Context, j jobs.Job, prof profile.Profile) (classify.Labels, error) {
	sc := prof.NewScanner()
	st := &stream{f: formatOf(j)}
	var err error
	emit := func(row row) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if row.Header {
			return nil
		}
		r := row.Rec
		if prof.OnInvalid == profile.FailFile && (r.Err != nil || prof.CheckRecord(r.Raw) != nil) {
			return permanent{code: profile.CodeSchema, blocked: true}
		}
		if prof.MaxRecords > 0 && st.f.dataNumber(r) > prof.MaxRecords {
			return Permanent(profile.CodeRowLimit)
		}
		if r.Err == nil {
			st.scan(sc, row)
		}
		return nil
	}
	if st.f == formatParquet {
		var ra io.ReaderAt
		var size int64
		if ra, size, _, err = w.openParquet(ctx, j); err == nil {
			err = mapParquetErr(st.runParquet(ra, size, w.parquetLimits(prof), 0, 0, emit))
		}
		return sc.Take(), err
	}
	rc, _, err := w.Source.Open(ctx, j, 0)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	err = st.run(rc, prof.MaxRecord(w.MaxRecordBytes), 0, 1, emit)
	return sc.Take(), err
}

func (w *Worker) run(ctx context.Context, j jobs.Job, seen classify.Labels) error {
	prof := profile.Select(j.SourceType)
	if prof.Parser != "ndjson" && prof.Parser != "csv" {
		return Permanent("parser_unavailable")
	}
	pol := prof.EffectiveCredentialsPolicy(w.Policies, j.TenantID, j.SourceID)
	var scanner *classify.Scanner
	if !prof.Gates(pol) {
		scanner = prof.NewScanner() // nothing can hold the file back: classify while processing
	} else if len(j.Checkpoint) == 0 {
		labels, err := w.prescan(ctx, j, prof)
		if err != nil {
			return err
		}
		seen.Merge(labels)
		if err := w.Store.SetLabels(ctx, j, labels); err != nil {
			return err
		}
		switch out := prof.Evaluate(pol, labels); out.Action {
		case profile.Quarantine:
			return quarantined{code: out.Code, labels: labels}
		case profile.Block:
			return permanent{code: out.Code, blocked: true}
		}
		if prof.NoSink {
			return nil // classified and labelled; this profile never writes records to the sink
		}
	}

	if formatOf(j) == formatParquet {
		return w.runParquet(ctx, j, prof, scanner, seen)
	}
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

	st := &stream{f: formatOf(j)}
	if len(cp.Header) > 0 {
		st.setHeader(cp.Header)
	}
	var valid, invalid []Item
	var pending []parse.Record
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		if err := w.accept(ctx, j, fp, valid, invalid, pending[0].Number, pending[len(pending)-1].Number); err != nil {
			return err
		}
		next := cp
		for _, r := range pending {
			var aerr error
			if next, aerr = next.Advance(r); aerr != nil {
				return Permanent("checkpoint_gap")
			}
		}
		if st.f == formatCSV {
			next.Header = st.header
		}
		raw, _ := json.Marshal(next)
		var delta classify.Labels
		if scanner != nil {
			delta = scanner.Take()
			seen.Merge(delta)
		}
		if err := w.Store.SaveCheckpoint(ctx, j, raw, int64(len(valid)), int64(len(invalid)), delta); err != nil {
			return err
		}
		w.M.Batches.Add(1)
		w.M.Records.Add(int64(len(valid)))
		w.M.InvalidRecords.Add(int64(len(invalid)))
		cp, valid, invalid, pending = next, nil, nil, nil
		return nil
	}
	startOffset, startNumber := cp.NextOffset, cp.NextRecordNumber
	err = st.run(rc, prof.MaxRecord(w.MaxRecordBytes), startOffset, startNumber, func(rw row) error {
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		r := rw.Rec
		if rw.Header {
			pending = append(pending, r)
			return nil
		}
		if prof.MaxRecords > 0 && st.f.dataNumber(r) > prof.MaxRecords {
			return Permanent(profile.CodeRowLimit)
		}
		if r.Err == nil {
			if cerr := prof.CheckRecord(r.Raw); cerr != nil {
				r.Err = cerr // drop_row behaviour: routed to processing_errors, the file continues
			}
		}
		if scanner != nil && r.Err == nil {
			rw.Rec = r
			st.scan(scanner, rw)
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

// accept hands a batch to the sink, then records its lineage. Both must succeed before the caller checkpoints.
func (w *Worker) accept(ctx context.Context, j jobs.Job, fp string, valid, invalid []Item, first, last int) error {
	if len(valid) == 0 && len(invalid) == 0 {
		return nil
	}
	if err := w.Sink.Accept(ctx, j, valid, invalid); err != nil {
		w.M.SinkErrors.Add(1)
		return err
	}
	if w.Lineage == nil {
		return nil
	}
	b := jobs.BatchLineage{TenantID: j.TenantID, JobID: j.ID, SourceFileID: j.FileObjectID, SourceChecksum: fp,
		PipelineVersion: w.PipelineVersion, Destination: w.LineageDestination, FirstRow: int64(first), LastRow: int64(last),
		Valid: len(valid), Invalid: len(invalid)}
	if o, ok := w.Sink.(Outputter); ok {
		if b.OutputKey = o.OutputKey(j, valid); b.OutputKey != "" {
			b.Destination = jobs.ParquetS3
		}
	}
	b.BatchID = jobs.NewBatchID(fp, w.PipelineVersion, b.FirstRow, b.LastRow)
	if err := b.Validate(); err != nil {
		return Permanent("lineage_invalid")
	}
	return w.Lineage.Record(ctx, b)
}
