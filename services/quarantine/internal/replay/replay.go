package replay

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

var ErrInvalidReplayRequest = errors.New("invalid replay request")

const (
	ReplayScope                = "astryon:quarantine-replay:read" // dry-run planning
	ReplayPublishScope         = "astryon:quarantine-replay:write"
	maxReplayRange             = 31 * 24 * time.Hour
	maxReplayCandidates uint64 = 10_000
)

// ReplayDryRunRequest is an already validated, tenant-independent selection.
// Tenant identity is intentionally a separate planner argument so JSON input
// can never override the authenticated claim.
type ReplayDryRunRequest struct {
	SourceID        string
	OccurredFrom    time.Time
	OccurredTo      time.Time
	PipelineVersion uint32
}

type ReplayDryRunPlan struct {
	CandidateCount             uint64
	CandidateCountIsLowerBound bool
}

type ReplayDryRunPlanner interface {
	PlanReplay(context.Context, string, ReplayDryRunRequest) (ReplayDryRunPlan, error)
}

// ReplayJob is a bounded quarantine selection. It intentionally does not use
// replay.jobs.v1: that topic's DPP-043 contract requires a newly allocated
// processing_version and is consumed by replay-worker. A quarantine executor
// may consume this job without accepting a tenant from an HTTP request.
type ReplayJob struct {
	JobID           string    `json:"job_id"`
	TenantID        string    `json:"tenant_id"`
	SourceID        string    `json:"source_id"`
	OccurredFrom    time.Time `json:"occurred_from"`
	OccurredTo      time.Time `json:"occurred_to"`
	PipelineVersion uint32    `json:"pipeline_version"`
	CandidateCount  uint64    `json:"candidate_count"`
	CreatedAt       time.Time `json:"created_at"`
}

func (j ReplayJob) Validate() error {
	request := ReplayDryRunRequest{SourceID: j.SourceID, OccurredFrom: j.OccurredFrom, OccurredTo: j.OccurredTo, PipelineVersion: j.PipelineVersion}
	if j.JobID == "" || !tenantUUID.MatchString(j.TenantID) || request.Validate() != nil || j.CandidateCount > maxReplayCandidates {
		return ErrInvalidReplayRequest
	}
	return nil
}

type ReplayJobPublisher interface {
	PublishReplayJob(context.Context, ReplayJob) error
}

type ReplayAuditEvent struct {
	EventID     string    `json:"event_id"`
	Time        time.Time `json:"ts"`
	TenantID    string    `json:"tenant_id"`
	Subject     string    `json:"subject"`
	Action      string    `json:"action"`
	ReplayID    string    `json:"replay_id"`
	SourceID    string    `json:"source_id"`
	FromVersion *int32    `json:"from_version"`
	ToVersion   *int32    `json:"to_version"`
	Outcome     string    `json:"outcome"`
	Phase       string    `json:"phase,omitempty"`
	RequestID   string    `json:"request_id"`
}

type ReplayAuditSink interface {
	RecordReplayAudit(context.Context, ReplayAuditEvent) error
}

func newReplayID() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("replay id: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}

func (r ReplayDryRunRequest) Validate() error {
	if !tenantUUID.MatchString(r.SourceID) || r.PipelineVersion == 0 ||
		r.OccurredFrom.IsZero() || r.OccurredTo.IsZero() ||
		!r.OccurredFrom.Before(r.OccurredTo) || r.OccurredTo.Sub(r.OccurredFrom) > maxReplayRange {
		return ErrInvalidReplayRequest
	}
	return nil
}
