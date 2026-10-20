// Package worker executes replay jobs (replay.jobs.v1) with the MINIMAL replay model: copy-forward.
//
// It reads the de-duplicated events of the source's previous processing_version from ClickHouse and
// re-publishes them to processed.events.v1 with processing_version set to the job's NEW version. It does
// NOT re-run a changed pipeline; see README.md.
package worker

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	ec "github.com/astryon/event-contracts"
)

// Limits on a job (the controller enforces the same bounds when planning).
const (
	MaxRanges = 32
	MaxSpan   = 31 * 24 * time.Hour
)

// Range is a half-open time range [From, To).
type Range struct {
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// Job is the wire form of services/replay-controller ReplayJob. TenantID is the ONLY source of the tenant
// in this process: the controller sets it from the verified principal; nothing else (message key, headers,
// ClickHouse rows) can override it.
type Job struct {
	JobID             string    `json:"job_id"`
	TenantID          string    `json:"tenant_id"`
	SourceID          string    `json:"source_id"`
	PipelineVersion   uint32    `json:"pipeline_version"`
	ProcessingVersion uint32    `json:"processing_version"`
	Ranges            []Range   `json:"raw_source_ranges"`
	CreatedAt         time.Time `json:"created_at"`
}

// ErrPermanent marks a failure that retrying cannot fix (malformed job, invalid source data). The worker
// marks the request FAILED and commits the offset so the job is not redelivered forever.
var ErrPermanent = errors.New("permanent job failure")

func permanent(format string, a ...any) error {
	return fmt.Errorf("%w: %s", ErrPermanent, fmt.Sprintf(format, a...))
}

// DecodeJob parses a message value. Unknown fields are tolerated (forward compatible producers).
func DecodeJob(b []byte) (Job, error) {
	var j Job
	if err := json.Unmarshal(b, &j); err != nil {
		return Job{}, permanent("undecodable job: %v", err)
	}
	return j, nil
}

// Validate checks the bounded scope and canonicalizes ids to lower case.
func (j *Job) Validate() error {
	if !ec.IsUUID(j.TenantID) {
		return permanent("tenant_id must be a UUID")
	}
	if !ec.IsUUID(j.SourceID) {
		return permanent("source_id must be a UUID")
	}
	if j.ProcessingVersion < 1 {
		return permanent("processing_version must be >= 1")
	}
	if j.JobID == "" || len(j.JobID) > 64 {
		return permanent("job_id required (max 64 chars)")
	}
	if n := len(j.Ranges); n == 0 || n > MaxRanges {
		return permanent("raw_source_ranges must have 1..%d entries", MaxRanges)
	}
	var span time.Duration
	for i, r := range j.Ranges {
		if r.From.IsZero() || !r.To.After(r.From) {
			return permanent("range %d: need from < to", i)
		}
		span += r.To.Sub(r.From)
	}
	if span > MaxSpan {
		return permanent("total range exceeds 31 days")
	}
	j.TenantID, j.SourceID = lower(j.TenantID), lower(j.SourceID)
	return nil
}

func lower(s string) string { c, _ := ec.CanonicalID(s); return c }
