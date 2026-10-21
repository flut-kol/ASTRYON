// Package replay implements the replay planning core (DPP-043, DEVELOPMENT_PLAN §5.3):
// a replay always writes a NEW processing_version, so readers that select the active version never
// see doubled metrics.
package replay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"time"
)

const (
	MaxRange        = 31 * 24 * time.Hour
	MaxEstimateRows = 10_000_000 // cap for the bounded count()
)

var uuidRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

// Request is the wire request. It deliberately has NO tenant field; unknown fields are rejected.
type Request struct {
	SourceID        string    `json:"source_id"`
	From            time.Time `json:"from"`
	To              time.Time `json:"to"`
	PipelineVersion uint32    `json:"pipeline_version"`
	DryRun          bool      `json:"dry_run"`
}

// Range is a half-open raw source range [From, To).
type Range struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

type Plan struct {
	SourceID          string  `json:"source_id"`
	PipelineVersion   uint32  `json:"pipeline_version"`
	ProcessingVersion uint32  `json:"processing_version"`
	BasedOnVersion    uint32  `json:"based_on_processing_version"`
	EstimatedEvents   uint64  `json:"estimated_events"`
	EstimateCapped    bool    `json:"estimate_capped"`
	Ranges            []Range `json:"raw_source_ranges"`
	DryRun            bool    `json:"dry_run"`
	JobID             string  `json:"job_id,omitempty"`
}

// ReplayJob is what is handed to the JobPublisher (Kafka publishing is out of scope: TODO).
type ReplayJob struct {
	JobID             string    `json:"job_id"`
	TenantID          string    `json:"tenant_id"`
	SourceID          string    `json:"source_id"`
	PipelineVersion   uint32    `json:"pipeline_version"`
	ProcessingVersion uint32    `json:"processing_version"`
	Ranges            []Range   `json:"raw_source_ranges"`
	CreatedAt         time.Time `json:"created_at"`
}

// VersionAllocator hands out processing versions, monotonic per (tenant, source).
type VersionAllocator interface {
	// Peek returns the highest existing version (0 if none) without side effects.
	Peek(ctx context.Context, tenantID, sourceID string) (uint32, error)
	// Allocate reserves and returns a version strictly greater than every existing/previously allocated one.
	Allocate(ctx context.Context, tenantID, sourceID string) (uint32, error)
}

// EventCounter estimates the number of de-duplicated events of a version in a range.
type EventCounter interface {
	Count(ctx context.Context, tenantID, sourceID string, version uint32, from, to time.Time, limit uint64) (uint64, error)
}

type JobPublisher interface {
	Publish(ctx context.Context, job ReplayJob) error
}

type Planner struct {
	Alloc     VersionAllocator
	Counter   EventCounter
	Publisher JobPublisher
	Now       func() time.Time
}

var ErrInvalid = errors.New("invalid replay request")

// ErrConflict: the (tenant, source, processing_version) the request tried to reserve already exists
// (unique index replay_requests_version_uq, migration 0014). The handler answers 409; retrying allocates the next version.
var ErrConflict = errors.New("replay version conflict")

// Validate checks the bounded explicit scope.
func (r Request) Validate() error {
	switch {
	case !uuidRe.MatchString(r.SourceID):
		return fmt.Errorf("%w: source_id must be a UUID", ErrInvalid)
	case r.From.IsZero() || r.To.IsZero():
		return fmt.Errorf("%w: from and to are required", ErrInvalid)
	case !r.To.After(r.From):
		return fmt.Errorf("%w: to must be after from", ErrInvalid)
	case r.To.Sub(r.From) > MaxRange:
		return fmt.Errorf("%w: range exceeds 31 days", ErrInvalid)
	case r.PipelineVersion == 0:
		return fmt.Errorf("%w: pipeline_version must be >= 1", ErrInvalid)
	}
	return nil
}

// DayRanges splits [from,to) on UTC day boundaries (raw source is partitioned per day).
func DayRanges(from, to time.Time) []Range {
	from, to = from.UTC(), to.UTC()
	var out []Range
	for cur := from; cur.Before(to); {
		next := cur.Truncate(24 * time.Hour).Add(24 * time.Hour)
		if next.After(to) {
			next = to
		}
		out = append(out, Range{cur, next})
		cur = next
	}
	return out
}

// Plan validates and plans a replay for tenantID (taken from the verified principal ONLY).
// dry_run performs only reads: it allocates nothing and publishes nothing.
func (p *Planner) Plan(ctx context.Context, tenantID string, req Request) (Plan, error) {
	return p.PlanWith(ctx, tenantID, req, nil)
}

// PlanWith is Plan with a hook that runs after the version is allocated and BEFORE the job is published (never for a
// dry run). If the hook fails nothing is published, the reserved row is marked FAILED, the version stays burned and the
// hook's error is returned.
func (p *Planner) PlanWith(ctx context.Context, tenantID string, req Request, before func(context.Context, Plan) error) (Plan, error) {
	if !uuidRe.MatchString(tenantID) {
		return Plan{}, fmt.Errorf("%w: tenant", ErrInvalid)
	}
	if err := req.Validate(); err != nil {
		return Plan{}, err
	}
	base, err := p.Alloc.Peek(ctx, tenantID, req.SourceID)
	if err != nil {
		return Plan{}, err
	}
	var n uint64
	if base > 0 {
		if n, err = p.Counter.Count(ctx, tenantID, req.SourceID, base, req.From, req.To, MaxEstimateRows+1); err != nil {
			return Plan{}, err
		}
	}
	capped := n > MaxEstimateRows
	if capped {
		n = MaxEstimateRows
	}
	plan := Plan{SourceID: req.SourceID, PipelineVersion: req.PipelineVersion, BasedOnVersion: base,
		EstimatedEvents: n, EstimateCapped: capped, Ranges: DayRanges(req.From, req.To), DryRun: req.DryRun}
	if req.DryRun {
		plan.ProcessingVersion = base + 1 // informational; NOT reserved
		return plan, nil
	}
	var v uint32
	var reqID string
	rs, durable := p.Alloc.(RequestStore)
	if durable {
		r, err := rs.Reserve(ctx, tenantID, req.SourceID, req.From, req.To)
		if err != nil {
			return Plan{}, err
		}
		v, reqID = r.ProcessingVersion, r.ID
	} else {
		var err error
		if v, err = p.Alloc.Allocate(ctx, tenantID, req.SourceID); err != nil {
			return Plan{}, err
		}
	}
	plan.ProcessingVersion = v
	b := make([]byte, 8)
	if _, err := rand.Read(b); err != nil {
		return Plan{}, err
	}
	plan.JobID = hex.EncodeToString(b)
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	job := ReplayJob{JobID: plan.JobID, TenantID: tenantID, SourceID: req.SourceID, PipelineVersion: req.PipelineVersion,
		ProcessingVersion: v, Ranges: plan.Ranges, CreatedAt: now().UTC()}
	fail := func(err error) (Plan, error) {
		if durable {
			// Best effort, detached from a possibly cancelled request context; the row stays REQUESTED if this fails.
			sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = rs.SetStatus(sctx, tenantID, reqID, StatusFailed)
		}
		return Plan{}, err
	}
	if before != nil {
		if err := before(ctx, plan); err != nil {
			return fail(err)
		}
	}
	// If publishing fails the version stays burned: gaps are fine, reuse is not.
	if err := p.Publisher.Publish(ctx, job); err != nil {
		return fail(err)
	}
	return plan, nil
}
