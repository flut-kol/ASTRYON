package api

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/astryon/file-ingestion/internal/joblist"
)

// ScopeJobsRead gates GET /v1/jobs[/{id}] (read-only; not implied by uploads:write).
const ScopeJobsRead = "astryon:jobs:read"

// JobReader serves the read-only job endpoints; *joblist.PGStore satisfies it.
type JobReader interface {
	List(ctx context.Context, tenant string, f joblist.Filter) (joblist.Page, error)
	Get(ctx context.Context, tenant, id string) (joblist.Job, error)
}

type jobView struct {
	ID              string           `json:"id"`
	Kind            string           `json:"kind"`
	Status          string           `json:"status"`
	SourceID        *string          `json:"source_id"`
	FileObjectID    *string          `json:"file_object_id"`
	AttemptCount    int              `json:"attempt_count"`
	MaxAttempts     int              `json:"max_attempts"`
	EventsProcessed int64            `json:"events_processed"`
	EventsFailed    int64            `json:"events_failed"`
	Error           string           `json:"error,omitempty"`
	Labels          map[string]int64 `json:"labels"`
	CreatedAt       time.Time        `json:"created_at"`
	StartedAt       *time.Time       `json:"started_at"`
	FinishedAt      *time.Time       `json:"finished_at"`
}

func optStr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func toJobView(j joblist.Job) jobView {
	lb := j.Labels
	if lb == nil {
		lb = map[string]int64{}
	}
	return jobView{ID: j.ID, Kind: j.Kind, Status: j.Status, SourceID: optStr(j.SourceID), FileObjectID: optStr(j.FileObjectID),
		AttemptCount: j.AttemptCount, MaxAttempts: j.MaxAttempts, EventsProcessed: j.EventsProcessed, EventsFailed: j.EventsFailed,
		Error: j.Error, Labels: lb, CreatedAt: j.CreatedAt, StartedAt: j.StartedAt, FinishedAt: j.FinishedAt}
}

// jobsPrincipal authenticates and requires ScopeJobsRead.
func (s *Server) jobsPrincipal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := s.Auth.Authenticate(r)
	if err != nil || p.TenantID == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeErr(w, 401, "UNAUTHENTICATED")
		return Principal{}, false
	}
	for _, sc := range p.Scopes {
		if sc == ScopeJobsRead {
			return p, true
		}
	}
	writeErr(w, 403, "FORBIDDEN")
	return Principal{}, false
}

func parseJobsQuery(q url.Values) (joblist.Filter, bool) {
	var f joblist.Filter
	for k, v := range q {
		if len(v) != 1 {
			return f, false
		}
		switch k {
		case "limit":
			n, err := strconv.Atoi(v[0])
			if err != nil || n < 1 || n > joblist.MaxLimit {
				return f, false
			}
			f.Limit = n
		case "cursor":
			if v[0] == "" || len(v[0]) > 256 {
				return f, false
			}
			f.Cursor = v[0]
		case "status":
			if !jobStatuses[v[0]] {
				return f, false
			}
			f.Status = v[0]
		case "source_id":
			if !uuidRe.MatchString(v[0]) {
				return f, false
			}
			f.SourceID = v[0]
		default:
			return f, false
		}
	}
	return f, true
}

func (s *Server) listJobs(w http.ResponseWriter, r *http.Request) {
	p, ok := s.jobsPrincipal(w, r)
	if !ok {
		return
	}
	f, ok := parseJobsQuery(r.URL.Query())
	if !ok {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if s.JobReader == nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	page, err := s.JobReader.List(r.Context(), p.TenantID, f)
	if errors.Is(err, joblist.ErrBadCursor) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	items := make([]jobView, 0, len(page.Jobs))
	for _, j := range page.Jobs {
		items = append(items, toJobView(j))
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, struct {
		Items      []jobView `json:"items"`
		NextCursor string    `json:"next_cursor"`
	}{items, page.NextCursor})
}

func (s *Server) getJob(w http.ResponseWriter, r *http.Request) {
	p, ok := s.jobsPrincipal(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if s.JobReader == nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	j, err := s.JobReader.Get(r.Context(), p.TenantID, id)
	if errors.Is(err, joblist.ErrNotFound) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, toJobView(j))
}
