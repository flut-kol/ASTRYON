package replay

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"mime"
	"net/http"
	"strings"
	"time"
)

const maxReplayRequestBytes = 16 << 10

type replayDryRunInput struct {
	SourceID        string `json:"source_id"`
	OccurredFrom    string `json:"occurred_from"`
	OccurredTo      string `json:"occurred_to"`
	PipelineVersion uint32 `json:"pipeline_version"`
}

func (h BrowserHandler) replayDryRun(w http.ResponseWriter, r *http.Request) {
	traceID := browserRequestID(r)
	w.Header().Set("X-Request-ID", traceID)
	if h.Auth == nil || h.ReplayPlanner == nil {
		writeBrowserError(w, http.StatusServiceUnavailable, "REPLAY_UNAVAILABLE", "replay dry-run is not configured", traceID)
		return
	}
	p, ok := h.authorizeReplay(w, r, traceID, ReplayScope)
	if !ok {
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeBrowserError(w, http.StatusUnsupportedMediaType, "INVALID_REQUEST", "content type must be application/json", traceID)
		return
	}
	input, err := decodeReplayDryRunInput(r)
	if err != nil {
		writeBrowserError(w, http.StatusBadRequest, "INVALID_REQUEST", ErrInvalidReplayRequest.Error(), traceID)
		return
	}
	from, fromErr := time.Parse(time.RFC3339, input.OccurredFrom)
	to, toErr := time.Parse(time.RFC3339, input.OccurredTo)
	request := ReplayDryRunRequest{
		SourceID: strings.ToLower(input.SourceID), OccurredFrom: from.UTC(), OccurredTo: to.UTC(),
		PipelineVersion: input.PipelineVersion,
	}
	if fromErr != nil || toErr != nil || request.Validate() != nil {
		writeBrowserError(w, http.StatusBadRequest, "INVALID_REQUEST", ErrInvalidReplayRequest.Error(), traceID)
		return
	}
	plan, err := h.ReplayPlanner.PlanReplay(r.Context(), p.TenantID, request)
	if err != nil {
		Metrics.incPlan("unavailable")
		writeBrowserError(w, http.StatusServiceUnavailable, "REPLAY_UNAVAILABLE", "replay dry-run unavailable", traceID)
		return
	}
	executable := !plan.CandidateCountIsLowerBound
	Metrics.observeCandidates(int64(plan.CandidateCount))
	if executable {
		Metrics.incPlan("executable")
	} else {
		Metrics.incPlan("over_limit")
	}
	response := map[string]any{
		"dry_run": true, "source_id": request.SourceID,
		"occurred_from":                  request.OccurredFrom.Format(time.RFC3339Nano),
		"occurred_to":                    request.OccurredTo.Format(time.RFC3339Nano),
		"pipeline_version":               request.PipelineVersion,
		"candidate_count":                plan.CandidateCount,
		"candidate_count_is_lower_bound": plan.CandidateCountIsLowerBound,
		"executable":                     executable,
	}
	if !executable {
		response["reason"] = "candidate_limit_exceeded"
	}
	writeBrowserJSON(w, http.StatusOK, response)
}

func (h BrowserHandler) replayPublish(w http.ResponseWriter, r *http.Request) {
	outcome := "rejected" // auth, content-type and validation failures
	defer func() { Metrics.incPublish(outcome) }()
	traceID := browserRequestID(r)
	w.Header().Set("X-Request-ID", traceID)
	if h.Auth == nil || h.ReplayPlanner == nil || h.ReplayJobs == nil || h.Audit == nil {
		outcome = "unavailable"
		writeBrowserError(w, http.StatusServiceUnavailable, "REPLAY_UNAVAILABLE", "replay publication is not configured", traceID)
		return
	}
	p, ok := h.authorizeReplay(w, r, traceID, ReplayPublishScope)
	if !ok {
		return
	}
	if !isJSONContentType(r.Header.Get("Content-Type")) {
		writeBrowserError(w, http.StatusUnsupportedMediaType, "INVALID_REQUEST", "content type must be application/json", traceID)
		return
	}
	request, err := parseReplayRequest(r)
	if err != nil {
		writeBrowserError(w, http.StatusBadRequest, "INVALID_REQUEST", ErrInvalidReplayRequest.Error(), traceID)
		return
	}
	plan, err := h.ReplayPlanner.PlanReplay(r.Context(), p.TenantID, request)
	if err != nil {
		outcome = "plan_unavailable"
		writeBrowserError(w, http.StatusServiceUnavailable, "REPLAY_UNAVAILABLE", "replay plan unavailable", traceID)
		return
	}
	Metrics.observeCandidates(int64(plan.CandidateCount))
	if plan.CandidateCountIsLowerBound || plan.CandidateCount > maxReplayCandidates {
		outcome = "scope_too_large"
		writeBrowserError(w, http.StatusUnprocessableEntity, "REPLAY_SCOPE_TOO_LARGE", "replay candidate limit exceeded; narrow the selection", traceID)
		return
	}
	jobID, err := newReplayID()
	if err != nil {
		outcome = "unavailable"
		writeBrowserError(w, http.StatusServiceUnavailable, "REPLAY_UNAVAILABLE", "replay identifier unavailable", traceID)
		return
	}
	now := time.Now
	if h.Now != nil {
		now = h.Now
	}
	job := ReplayJob{
		JobID: jobID, TenantID: p.TenantID, SourceID: request.SourceID,
		OccurredFrom: request.OccurredFrom, OccurredTo: request.OccurredTo,
		PipelineVersion: request.PipelineVersion, CandidateCount: plan.CandidateCount, CreatedAt: now().UTC(),
	}
	prepared := replayAuditEvent(job, p.Subject, traceID, "prepared", "pending", now().UTC())
	if err := h.Audit.RecordReplayAudit(r.Context(), prepared); err != nil {
		outcome = "audit_unavailable"
		writeBrowserError(w, http.StatusServiceUnavailable, "AUDIT_UNAVAILABLE", "audit trail unavailable; replay was not published", traceID)
		return
	}
	if err := h.ReplayJobs.PublishReplayJob(r.Context(), job); err != nil {
		outcome = "publish_failed"
		h.recordReplayOutcome(r.Context(), prepared, "aborted", "failed", now().UTC())
		h.recordMirror(r.Context(), MirrorEvent{TenantID: prepared.TenantID, Subject: prepared.Subject,
			Action: "quarantine.replay.publish", Outcome: "failure", ResourceType: "quarantine_replay",
			ResourceID: prepared.ReplayID, TraceID: traceID, RequestID: traceID})
		writeBrowserError(w, http.StatusBadGateway, "REPLAY_PUBLISH_FAILED", "replay job was not acknowledged", traceID)
		return
	}
	// The prepared record is the fail-closed authorization audit. This outcome
	// is best effort because the broker has already acknowledged the job.
	h.recordReplayOutcome(r.Context(), prepared, "committed", "success", now().UTC())
	h.recordMirror(r.Context(), MirrorEvent{TenantID: prepared.TenantID, Subject: prepared.Subject,
		Action: "quarantine.replay.publish", Outcome: "success", ResourceType: "quarantine_replay",
		ResourceID: prepared.ReplayID, TraceID: traceID, RequestID: traceID})
	outcome = "published"
	writeBrowserJSON(w, http.StatusAccepted, map[string]any{
		"job_id": job.JobID, "status": "queued", "candidate_count": job.CandidateCount,
	})
}

func (h BrowserHandler) authorizeReplay(w http.ResponseWriter, r *http.Request, traceID, scope string) (BrowserPrincipal, bool) {
	p, err := h.Auth.Authenticate(r)
	if err != nil || p.TenantID == "" {
		writeBrowserError(w, http.StatusUnauthorized, "UNAUTHENTICATED", "authentication required", traceID)
		return BrowserPrincipal{}, false
	}
	if !tenantUUID.MatchString(p.TenantID) {
		writeBrowserError(w, http.StatusForbidden, "FORBIDDEN", "principal has no valid tenant", traceID)
		return BrowserPrincipal{}, false
	}
	p.TenantID = strings.ToLower(p.TenantID)
	if !p.Scopes[scope] {
		h.recordMirror(r.Context(), MirrorEvent{TenantID: p.TenantID, Subject: p.Subject, ClientID: p.ClientID,
			ACR: p.ACR, Action: "auth.denied", Outcome: "denied", ResourceType: "quarantine_replay",
			TraceID: traceID, RequestID: traceID})
		writeBrowserError(w, http.StatusForbidden, "FORBIDDEN", "missing scope "+scope, traceID)
		return BrowserPrincipal{}, false
	}
	return p, true
}

func (h BrowserHandler) recordMirror(ctx context.Context, e MirrorEvent) {
	if h.AuditMirror == nil || e.TenantID == "" {
		return
	}
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := h.AuditMirror.RecordMirror(detached, e); err != nil {
		Metrics.incMirror(false)
		log.Printf("quarantine audit mirror failed: %v", err)
		return
	}
	Metrics.incMirror(true)
}

func parseReplayRequest(r *http.Request) (ReplayDryRunRequest, error) {
	input, err := decodeReplayDryRunInput(r)
	if err != nil {
		return ReplayDryRunRequest{}, err
	}
	from, fromErr := time.Parse(time.RFC3339, input.OccurredFrom)
	to, toErr := time.Parse(time.RFC3339, input.OccurredTo)
	request := ReplayDryRunRequest{
		SourceID: strings.ToLower(input.SourceID), OccurredFrom: from.UTC(), OccurredTo: to.UTC(),
		PipelineVersion: input.PipelineVersion,
	}
	if fromErr != nil || toErr != nil || request.Validate() != nil {
		return ReplayDryRunRequest{}, ErrInvalidReplayRequest
	}
	return request, nil
}

func replayAuditEvent(job ReplayJob, subject, requestID, phase, outcome string, at time.Time) ReplayAuditEvent {
	e := ReplayAuditEvent{
		Time: at, TenantID: job.TenantID, Subject: subject, Action: "replay.started",
		ReplayID: job.JobID, SourceID: job.SourceID,
		Outcome: outcome, Phase: phase, RequestID: requestID,
	}
	hash := sha256.Sum256([]byte(strings.Join([]string{e.TenantID, e.ReplayID, e.Action, e.Phase, e.Outcome, e.RequestID}, "\x1f")))
	e.EventID = fmt.Sprintf("%x", hash[:16])
	return e
}

func (h BrowserHandler) recordReplayOutcome(ctx context.Context, prepared ReplayAuditEvent, phase, outcome string, at time.Time) {
	job := ReplayJob{JobID: prepared.ReplayID, TenantID: prepared.TenantID, SourceID: prepared.SourceID}
	e := replayAuditEvent(job, prepared.Subject, prepared.RequestID, phase, outcome, at)
	detached, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	if err := h.Audit.RecordReplayAudit(detached, e); err != nil {
		log.Printf("quarantine replay audit outcome %s not recorded: %v", phase, err)
	}
}

func isJSONContentType(raw string) bool {
	mediaType, _, err := mime.ParseMediaType(raw)
	return err == nil && mediaType == "application/json"
}

func decodeReplayDryRunInput(r *http.Request) (replayDryRunInput, error) {
	var input replayDryRunInput
	limited := http.MaxBytesReader(nil, r.Body, maxReplayRequestBytes)
	decoder := json.NewDecoder(limited)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&input); err != nil {
		return input, err
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return input, ErrInvalidReplayRequest
	}
	return input, nil
}
