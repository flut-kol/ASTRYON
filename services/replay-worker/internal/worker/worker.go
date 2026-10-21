package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Request statuses used by the worker (enum replay_status, db/postgres/migrations/0005).
const (
	StatusRunning   = "RUNNING"
	StatusCompleted = "COMPLETED"
	StatusFailed    = "FAILED"
)

// ErrNotRunnable: no replay_requests row for (tenant, source, version) in a state the worker may move on
// (missing, CANCELLED, or already COMPLETED). A job without a durable request is not executed.
var ErrNotRunnable = errors.New("replay request not found or not runnable")

// ErrRunningRecently (an ErrNotRunnable): the request is RUNNING and was updated within the running lease, i.e. a live
// worker owns it or a crashed one has not yet timed out. The job must NOT be committed as skipped: after a hard crash
// the redelivered job is the only thing that can finish the replay once the lease expires.
var ErrRunningRecently = fmt.Errorf("%w: RUNNING within its lease", ErrNotRunnable)

// Delivery is one consumed job message.
type Delivery struct {
	Topic     string
	Partition int32
	Offset    int64
	Key       []byte
	Value     []byte
	Ref       any // opaque to the worker; handed back to JobSource.Commit
}

// JobSource is a consumer-group source of jobs. Poll returns ok=false when ctx ended without a message.
type JobSource interface {
	Poll(ctx context.Context) (d Delivery, ok bool, err error)
	// Commit marks d (and everything before it) consumed. Called only after the job is durably handled.
	Commit(ctx context.Context, d Delivery) error
}

// Record is one message to publish.
type Record struct {
	Topic string
	Key   []byte
	Value []byte
}

// Sink publishes records; it returns nil only after durable acknowledgement (acks=all).
type Sink interface {
	Publish(ctx context.Context, recs []Record) error
}

// SourceEvent is one de-duplicated row of events_normalized.
type SourceEvent struct {
	EventID         string
	TenantID        string // as stored; verified against the job, never trusted
	BatchID         string
	PipelineID      string
	PipelineVersion uint32
	EventType       string
	EventTime       time.Time
	IngestedAt      time.Time
	EntityID        string
	Attributes      map[string]string
	Payload         string
	RawRef          string
	QualityFlags    []string
}

// Cursor is the keyset position (event_time, event_id) of the last row of the previous page.
type Cursor struct {
	EventTime time.Time
	EventID   string
}

// PageQuery selects one page of de-duplicated events of one version in [From, To), strictly after After.
type PageQuery struct {
	TenantID, SourceID string
	Version            uint32
	From, To           time.Time
	After              *Cursor
	Limit              int
}

// EventReader is the (read-only) ClickHouse seam.
type EventReader interface {
	// BaseVersion returns the highest version below `below` that has data for tenant+source (0 = none).
	BaseVersion(ctx context.Context, tenantID, sourceID string, below uint32) (uint32, error)
	// Page returns up to q.Limit rows ordered by (event_time, event_id), one row per event_id.
	Page(ctx context.Context, q PageQuery) ([]SourceEvent, error)
	// Existing returns which of ids already exist under version (resume after a partial/previous run).
	Existing(ctx context.Context, tenantID, sourceID string, version uint32, from, to time.Time, ids []string) (map[string]bool, error)
}

// StatusUpdater records progress in replay_requests. The tenant is the job's tenant (verified principal at
// request time). Implementations return ErrNotRunnable when the transition is not allowed.
type StatusUpdater interface {
	SetStatus(ctx context.Context, tenantID, sourceID string, version uint32, status string) error
}

// Worker consumes jobs one at a time.
type Worker struct {
	Source   JobSource
	Reader   EventReader
	Sink     Sink
	Status   StatusUpdater // may be nil (no status tracking)
	OutTopic string        // default processed.events.v1
	PageSize int           // default 200
	Now      func() time.Time
	Metrics  *Metrics // optional; nil = no metrics
	Logf     func(format string, a ...any)
}

// m returns the worker's metrics, or a discarded instance when none are configured.
func (w *Worker) m() *Metrics {
	if w.Metrics != nil {
		return w.Metrics
	}
	return &discard
}

func (w *Worker) logf(f string, a ...any) {
	if w.Logf != nil {
		w.Logf(f, a...)
		return
	}
	log.Printf(f, a...)
}

func (w *Worker) now() time.Time {
	if w.Now != nil {
		return w.Now()
	}
	return time.Now()
}

// Run processes jobs until ctx ends or a transient failure occurs (the offset is then NOT committed, so
// the job is redelivered after a restart; a supervisor restarts the process).
func (w *Worker) Run(ctx context.Context) error {
	for ctx.Err() == nil {
		if _, err := w.Step(ctx); err != nil {
			return err
		}
	}
	return nil
}

// Step handles at most one job. handled=false means nothing was polled.
func (w *Worker) Step(ctx context.Context) (handled bool, err error) {
	d, ok, err := w.Source.Poll(ctx)
	if err != nil || !ok {
		return false, err
	}
	if err := w.handle(ctx, d); err != nil {
		return true, err
	}
	if err := w.Source.Commit(ctx, d); err != nil {
		return true, fmt.Errorf("commit %s/%d@%d: %w", d.Topic, d.Partition, d.Offset, err)
	}
	return true, nil
}

// handle returns nil when the job is durably handled (done, or permanently rejected and recorded).
func (w *Worker) handle(ctx context.Context, d Delivery) error {
	job, err := DecodeJob(d.Value)
	if err != nil {
		w.logf("skip %s/%d@%d: %v", d.Topic, d.Partition, d.Offset, err)
		return nil // poison: cannot even identify the request
	}
	if err := job.Validate(); err != nil {
		w.logf("skip job %q at %s/%d@%d: %v", job.JobID, d.Topic, d.Partition, d.Offset, err)
		w.m().Fail(FailInvalidJob)
		if ec.IsUUID(job.TenantID) && ec.IsUUID(job.SourceID) && job.ProcessingVersion > 0 {
			w.setStatus(ctx, job, StatusFailed)
		}
		return nil
	}
	w.m().add(&w.m().Requested, 1)
	if w.Status != nil {
		if err := w.Status.SetStatus(ctx, job.TenantID, job.SourceID, job.ProcessingVersion, StatusRunning); err != nil {
			if errors.Is(err, ErrRunningRecently) {
				w.m().Fail(FailLease)
				w.logf("job %s v%d not started: %v (offset not committed; redelivered after a restart once the lease expires)", job.JobID, job.ProcessingVersion, err)
				return err
			}
			if errors.Is(err, ErrNotRunnable) {
				w.logf("skip job %s v%d: %v", job.JobID, job.ProcessingVersion, err)
				return nil
			}
			w.m().Fail(FailStatus)
			return fmt.Errorf("set RUNNING: %w", err)
		}
	}
	w.m().add(&w.m().Approved, 1)
	w.m().add(&w.m().Started, 1)
	w.m().add(&w.m().Active, 1)
	began := w.now()
	n, err := w.Execute(ctx, job)
	w.m().add(&w.m().Active, -1)
	w.m().ObserveDuration(w.now().Sub(began))
	w.m().add(&w.m().EventsReplayed, int64(n))
	if err != nil {
		w.m().Fail(FailureCode(err))
		if errors.Is(err, ErrPermanent) {
			w.logf("job %s v%d FAILED permanently after %d events: %v", job.JobID, job.ProcessingVersion, n, err)
			w.setStatus(ctx, job, StatusFailed)
			return nil // recorded; redelivery cannot help
		}
		w.logf("job %s v%d failed after %d events (will be redelivered): %v", job.JobID, job.ProcessingVersion, n, err)
		w.setStatus(ctx, job, StatusFailed)
		return err // do NOT commit
	}
	if w.Status != nil {
		if err := w.Status.SetStatus(ctx, job.TenantID, job.SourceID, job.ProcessingVersion, StatusCompleted); err != nil && !errors.Is(err, ErrNotRunnable) {
			w.m().Fail(FailStatus)
			return fmt.Errorf("set COMPLETED: %w", err) // not committed: redelivery re-runs idempotently
		}
	}
	w.m().add(&w.m().Completed, 1)
	w.logf("job %s v%d completed: %d events published", job.JobID, job.ProcessingVersion, n)
	return nil
}

// setStatus is best effort and detached from a cancelled ctx.
func (w *Worker) setStatus(ctx context.Context, job Job, status string) {
	if w.Status == nil {
		return
	}
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := w.Status.SetStatus(sctx, job.TenantID, job.SourceID, job.ProcessingVersion, status); err != nil {
		w.logf("job %s: set %s: %v", job.JobID, status, err)
	}
}

// Execute copies the previous version's events of the job's ranges forward to the job's version and
// returns the number of envelopes published. The tenant is job.TenantID only.
func (w *Worker) Execute(ctx context.Context, job Job) (int, error) {
	if err := job.Validate(); err != nil {
		return 0, err
	}
	out := w.OutTopic
	if out == "" {
		out = ec.TopicProcessedEvents
	}
	limit := w.PageSize
	if limit <= 0 {
		limit = 200
	}
	base, err := w.Reader.BaseVersion(ctx, job.TenantID, job.SourceID, job.ProcessingVersion)
	if err != nil {
		return 0, fmt.Errorf("base version: %w", stage(FailBaseRead, err))
	}
	if base == 0 {
		return 0, nil // nothing to copy forward
	}
	published := 0
	for _, r := range job.Ranges {
		var after *Cursor
		for {
			rows, err := w.Reader.Page(ctx, PageQuery{TenantID: job.TenantID, SourceID: job.SourceID, Version: base,
				From: r.From, To: r.To, After: after, Limit: limit})
			if err != nil {
				return published, fmt.Errorf("read page: %w", stage(FailBaseRead, err))
			}
			if len(rows) == 0 {
				break
			}
			ids := make([]string, len(rows))
			for i, row := range rows {
				ids[i] = row.EventID
			}
			have, err := w.Reader.Existing(ctx, job.TenantID, job.SourceID, job.ProcessingVersion,
				rows[0].EventTime, rows[len(rows)-1].EventTime, ids)
			if err != nil {
				return published, fmt.Errorf("existing check: %w", stage(FailBaseRead, err))
			}
			recs := make([]Record, 0, len(rows))
			for _, row := range rows {
				if have[strings.ToLower(row.EventID)] {
					continue
				}
				rec, err := BuildRecord(out, job, row, w.now())
				if err != nil {
					return published, err
				}
				recs = append(recs, rec)
			}
			if len(recs) > 0 {
				if err := w.Sink.Publish(ctx, recs); err != nil {
					w.m().add(&w.m().PublishErrors, 1)
					return published, fmt.Errorf("publish: %w", stage(FailPublish, err))
				}
				published += len(recs)
			}
			last := rows[len(rows)-1]
			after = &Cursor{EventTime: last.EventTime, EventID: last.EventID}
			if len(rows) < limit {
				break
			}
		}
	}
	return published, nil
}

// replayEnvelope is the processed.events.v1 value: the contract envelope plus the normalization fields the
// ClickHouse sink maps to events_normalized columns.
type replayEnvelope struct {
	ec.Envelope
	BatchID         string            `json:"batch_id"`
	PipelineID      string            `json:"pipeline_id"`
	PipelineVersion uint32            `json:"pipeline_version"`
	EventType       string            `json:"event_type"`
	Attributes      map[string]string `json:"attributes"`
	QualityFlags    []string          `json:"quality_flags"`
	RawRef          string            `json:"raw_ref"`
}

// BuildRecord is a pure, deterministic function of (job, row): a redelivered job yields byte-identical
// records (same key, same value). Nothing time-dependent is embedded; `now` is used only to validate.
func BuildRecord(topic string, job Job, row SourceEvent, now time.Time) (Record, error) {
	if lower(row.TenantID) != job.TenantID {
		return Record{}, permanent("row of event %s belongs to another tenant than the job", row.EventID)
	}
	et := row.EventTime.UTC()
	schema := row.EventType
	if schema == "" {
		schema = "replay"
	}
	// json.Marshal compacts a RawMessage, so the checksum must be over the compacted bytes or the sink's
	// envelope validation (checksum == sha256(payload)) would reject every replayed event.
	var cp bytes.Buffer
	if err := json.Compact(&cp, []byte(row.Payload)); err != nil {
		return Record{}, permanent("event %s: payload is not JSON: %v", row.EventID, err)
	}
	payload := json.RawMessage(cp.Bytes())
	env := ec.Envelope{
		EventID: strings.ToLower(row.EventID), TenantID: job.TenantID, SourceID: job.SourceID,
		SchemaID: schema, SchemaVersion: 1, EventTime: &et, IngestedAt: row.IngestedAt.UTC(),
		EntityKey: row.EntityID, TraceID: "replay:" + job.JobID, PayloadFormat: ec.FormatJSON,
		Payload: payload, Checksum: ec.ChecksumOf(payload), ProcessingVersion: int(job.ProcessingVersion),
	}
	if err := env.ValidateAt(now); err != nil {
		return Record{}, permanent("event %s: %v", row.EventID, err)
	}
	if row.Attributes == nil {
		row.Attributes = map[string]string{}
	}
	if row.QualityFlags == nil {
		row.QualityFlags = []string{}
	}
	v, err := json.Marshal(replayEnvelope{Envelope: env, BatchID: row.BatchID, PipelineID: row.PipelineID,
		PipelineVersion: row.PipelineVersion, EventType: row.EventType, Attributes: row.Attributes,
		QualityFlags: row.QualityFlags, RawRef: row.RawRef})
	if err != nil {
		return Record{}, err
	}
	return Record{Topic: topic, Key: env.PartitionKey(), Value: v}, nil
}
