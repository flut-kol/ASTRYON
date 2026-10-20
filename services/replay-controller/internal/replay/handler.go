package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"
)

type Handler struct {
	Auth    Authenticator
	Planner *Planner
	// Activator/Audit enable POST /v1/replays/{id}/activate (cutover). Nil Activator = 501 not_configured.
	Activator Activator
	// Audit records plan/start/activate/rollback. nil = no audit. AuditRequired: an activation whose audit event cannot be
	// published is refused with 503 audit_unavailable BEFORE the switch is committed (REPLAY_AUDIT_MODE=required).
	// Plan/start auditing is always best effort (the job is already allocated/published by then; failures are logged).
	Audit         AuditSink
	AuditRequired bool

	denyOnce sync.Once
	denyLim  *denyLimiter
	denySem  chan struct{}
}

// authorize authenticates and checks the write scope. Denials (401/403) are answered and audited best effort as
// auth.denied (rate limited, no token, no headers; subject only if the token verified).
func (h *Handler) authorize(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := h.Auth.Authenticate(r)
	if err != nil {
		h.auditDenied(r, ReasonUnauthenticated, "")
		writeErr(w, 401, "unauthenticated", "authentication required")
		return Principal{}, false
	}
	if !p.HasScope(ScopeReplayWrite) {
		h.auditDenied(r, ReasonMissingScope, p.Subject)
		writeErr(w, 403, "forbidden", "missing scope "+ScopeReplayWrite)
		return Principal{}, false
	}
	return p, true
}

func ptr32(v int32) *int32 { return &v }

// record writes a best-effort audit event; failures are logged, never returned.
func (h *Handler) record(ctx context.Context, e AuditEvent) {
	if h.Audit == nil {
		return
	}
	if e.EventID == "" {
		e.EventID = NewEventID(e)
	}
	if err := h.Audit.Record(ctx, e); err != nil {
		log.Printf("replay audit: %s %s not recorded: %v", e.Action, e.Outcome, err)
	}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/replays", h.create)
	mux.HandleFunc("POST /v1/replays/{id}/activate", h.activate)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.Write([]byte("ok")) })
	return mux
}

func writeErr(w http.ResponseWriter, status int, code, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"code": code, "message": msg, "details": nil, "trace_id": ""})
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	p, ok := h.authorize(w, r)
	if !ok {
		return
	}
	var req Request
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
	dec.DisallowUnknownFields() // a tenant_id in the body is rejected, never honoured
	if err := dec.Decode(&req); err != nil {
		writeErr(w, 400, "invalid_request", "malformed body")
		return
	}
	rid := requestID(r.Header.Get("X-Request-Id"))
	startEv := func(pl Plan, phase, outcome string) AuditEvent {
		return AuditEvent{Time: time.Now().UTC(), TenantID: p.TenantID, Subject: p.Subject, Action: ActionStart, ReplayID: pl.JobID,
			SourceID: pl.SourceID, FromVersion: ptr32(int32(pl.BasedOnVersion)), ToVersion: ptr32(int32(pl.ProcessingVersion)),
			Outcome: outcome, Phase: phase, RequestID: rid}
	}
	// Required mode: replay.started (phase prepared) is published BEFORE the job; if that fails nothing is published
	// (503), the allocated version stays burned and its row is FAILED.
	var before func(context.Context, Plan) error
	var prepared *AuditEvent
	if h.Audit != nil && h.AuditRequired {
		before = func(ctx context.Context, pl Plan) error {
			ev := startEv(pl, PhasePrepared, OutcomePending)
			ev.EventID = NewEventID(ev)
			if err := h.Audit.Record(ctx, ev); err != nil {
				log.Printf("replay start: audit event not published, job not published: %v", err)
				return fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
			}
			prepared = &ev
			return nil
		}
	}
	plan, err := h.Planner.PlanWith(r.Context(), p.TenantID, req, before)
	switch {
	case errors.Is(err, ErrInvalid):
		writeErr(w, 400, "invalid_request", err.Error())
		return
	case errors.Is(err, ErrAuditUnavailable):
		writeErr(w, 503, "audit_unavailable", "audit trail unavailable; replay was not started, retry later")
		return
	case err != nil:
		if prepared != nil { // job publication failed after the prepared event: say so
			ev := *prepared
			ev.Phase, ev.Outcome, ev.Time, ev.EventID = PhaseAborted, OutcomeAborted, time.Now().UTC(), ""
			h.record(context.WithoutCancel(r.Context()), ev)
		}
		writeErr(w, 502, "upstream_error", "replay planning failed")
		return
	}
	switch {
	case plan.DryRun:
		h.record(r.Context(), AuditEvent{Time: time.Now().UTC(), TenantID: p.TenantID, Subject: p.Subject, Action: ActionPlan, ReplayID: plan.JobID,
			SourceID: plan.SourceID, FromVersion: ptr32(int32(plan.BasedOnVersion)), ToVersion: ptr32(int32(plan.ProcessingVersion)),
			Outcome: OutcomeSuccess, RequestID: rid})
	case prepared == nil:
		h.record(r.Context(), startEv(plan, PhaseCommitted, OutcomeSuccess)) // best_effort: after the job is published
	}
	w.Header().Set("Content-Type", "application/json")
	if plan.DryRun {
		w.WriteHeader(200)
	} else {
		w.WriteHeader(202)
	}
	json.NewEncoder(w).Encode(plan)
}

// activate switches readers of the replay's source to the replay's processing_version (see activate.go for the rules).
// Tenant comes only from the principal; the body is ignored; ?rollback=true undoes this replay's activation.
func (h *Handler) activate(w http.ResponseWriter, r *http.Request) {
	p, ok := h.authorize(w, r)
	if !ok {
		return
	}
	if h.Activator == nil {
		writeErr(w, 501, "not_configured", "cutover needs REPLAY_PG_URL")
		return
	}
	rb := false
	switch r.URL.Query().Get("rollback") {
	case "", "false":
	case "true":
		rb = true
	default:
		writeErr(w, 400, "invalid_request", "rollback must be true or false")
		return
	}
	rid := requestID(r.Header.Get("X-Request-Id"))
	replayID := r.PathValue("id")
	event := func(a Activation, outcome string) AuditEvent {
		return AuditEvent{Time: time.Now().UTC(), TenantID: p.TenantID, Subject: p.Subject, Action: "replay." + a.Action,
			ReplayID: a.ReplayID, SourceID: a.SourceID, FromVersion: a.PreviousProcessingVersion,
			ToVersion: ptr32(a.ActiveProcessingVersion), Outcome: outcome, RequestID: rid}
	}
	// Required mode, two phases: activate.prepared is published inside the DB transaction BEFORE COMMIT; if it cannot be
	// published nothing is committed (503). After the transaction a best-effort follow-up says what happened:
	// phase committed, or phase aborted when COMMIT failed. Prepared without a follow-up = unknown (README: reconciler).
	// Event ids are deterministic, so a retry with the same X-Request-Id does not double-audit.
	var pre PreCommit
	var published *AuditEvent
	if h.Audit != nil && h.AuditRequired {
		pre = func(ctx context.Context, a Activation) error {
			ev := event(a, OutcomePending)
			ev.Phase = PhasePrepared
			ev.EventID = NewEventID(ev)
			if err := h.Audit.Record(ctx, ev); err != nil {
				log.Printf("replay activate: audit event not published, activation refused: %v", err)
				return fmt.Errorf("%w: %v", ErrAuditUnavailable, err)
			}
			published = &ev
			return nil
		}
	}
	act, err := h.Activator.Activate(r.Context(), p.TenantID, replayID, rb, p.Subject, pre)
	rejected := func(action string) {
		h.record(r.Context(), AuditEvent{Time: time.Now().UTC(), TenantID: p.TenantID, Subject: p.Subject, Action: action,
			ReplayID: replayID, Outcome: OutcomeRejected, RequestID: rid})
	}
	reqAction := ActionActivate
	if rb {
		reqAction = ActionRollback
	}
	switch {
	case errors.Is(err, ErrAuditUnavailable):
		writeErr(w, 503, "audit_unavailable", "audit trail unavailable; activation was not applied, retry later")
		return
	case errors.Is(err, ErrNotFound):
		rejected(reqAction)
		writeErr(w, 404, "not_found", "replay not found")
		return
	case errors.Is(err, ErrNotCompleted):
		rejected(reqAction)
		writeErr(w, 409, "replay_not_completed", "only a COMPLETED replay can be activated")
		return
	case errors.Is(err, ErrRegression):
		rejected(reqAction)
		writeErr(w, 409, "version_regression", "target version is lower than the active version; use ?rollback=true to undo an activation")
		return
	case errors.Is(err, ErrRollbackNothing):
		rejected(reqAction)
		writeErr(w, 409, "rollback_unavailable", "this replay is not the active version or has no previous version")
		return
	case errors.Is(err, ErrActivationInvalid):
		writeErr(w, 400, "invalid_request", err.Error())
		return
	case err != nil:
		log.Printf("replay activate: %v", err)
		if published != nil {
			ev := *published
			ev.Phase, ev.Outcome, ev.Time, ev.EventID = PhaseAborted, OutcomeAborted, time.Now().UTC(), ""
			h.record(context.WithoutCancel(r.Context()), ev)
		}
		writeErr(w, 502, "upstream_error", "activation failed")
		return
	}
	if h.Audit != nil && act.Action != "noop" {
		ev := event(act, OutcomeSuccess) // best effort, after commit
		ev.Phase = PhaseCommitted
		h.record(context.WithoutCancel(r.Context()), ev)
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(act)
}
