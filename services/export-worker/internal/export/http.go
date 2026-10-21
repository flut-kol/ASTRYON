package export

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const maxBody = 1 << 20

// Server serves POST /v1/exports and GET /v1/exports/{id}. Jobs run in a bounded pool.
type Server struct {
	Auth       Authenticator
	Runner     *Runner // its Quota is ignored: the server reserves quota at POST and holds it for the job
	Quota      QuotaChecker
	Audit      AuditSink
	Jobs       JobStore
	Workers    int           // concurrently running jobs, default 2
	MaxQueue   int           // jobs waiting for a worker, default 100
	JobTimeout time.Duration // default 1h
	Now        func() time.Time
	// OnEnqueue, when set, switches to dispatcher mode: POST only inserts the QUEUED job (durable store) and calls
	// OnEnqueue (Dispatcher.Wake); a Dispatcher on any replica runs it. Nothing runs in this process from POST.
	OnEnqueue func()

	once   sync.Once
	sem    chan struct{}
	base   context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	closed bool
	queued int
	wg     sync.WaitGroup
}

func (s *Server) init() {
	s.once.Do(func() {
		w := s.Workers
		if w <= 0 {
			w = 2
		}
		s.sem = make(chan struct{}, w)
		s.base, s.cancel = context.WithCancel(context.Background())
		if s.MaxQueue <= 0 {
			s.MaxQueue = 100
		}
		if s.JobTimeout <= 0 {
			s.JobTimeout = time.Hour
		}
	})
}

func (s *Server) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}

func (s *Server) Handler() http.Handler {
	s.init()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/exports", s.wrap(ScopeExportWrite, s.create))
	mux.HandleFunc("GET /v1/exports/{id}", s.wrap(ScopeExportRead, s.get))
	return mux
}

type apiError struct {
	status     int
	retryAfter int
	Code       string `json:"code"`
	Message    string `json:"message"`
	Details    any    `json:"details,omitempty"`
}

func (e *apiError) Error() string { return e.Code + ": " + e.Message }

type handler func(w http.ResponseWriter, p Principal, r *http.Request, body []byte) (int, any, error)

func (s *Server) wrap(scope string, h handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rid := r.Header.Get("X-Request-ID")
		if rid == "" || len(rid) > 64 {
			rid = fmt.Sprintf("%d", time.Now().UnixNano())
		}
		w.Header().Set("X-Request-ID", rid)
		fail := func(e *apiError) {
			w.Header().Set("Content-Type", "application/json")
			if e.retryAfter > 0 {
				w.Header().Set("Retry-After", strconv.Itoa(e.retryAfter))
			}
			w.WriteHeader(e.status)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{
				"code": e.Code, "message": e.Message, "details": e.Details, "trace_id": rid}})
		}
		p, err := s.Auth.Authenticate(r)
		if err != nil {
			fail(&apiError{status: 401, Code: "unauthenticated", Message: "invalid or missing credentials"})
			return
		}
		if !p.HasScope(scope) {
			fail(&apiError{status: 403, Code: "forbidden", Message: "missing scope " + scope})
			return
		}
		if !uuidRe.MatchString(p.TenantID) {
			fail(&apiError{status: 403, Code: "forbidden", Message: "principal has no valid tenant"})
			return
		}
		var body []byte
		if r.Method == http.MethodPost {
			body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, maxBody))
			if err != nil {
				fail(&apiError{status: 413, Code: "body_too_large", Message: "request body too large"})
				return
			}
		}
		status, out, err := h(w, p, r, body)
		if err != nil {
			var ae *apiError
			if errors.As(err, &ae) {
				fail(ae)
			} else if isStoreUnavailable(err) {
				log.Printf("request_id=%q store unavailable: %v", rid, err) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
				fail(&apiError{status: 503, retryAfter: 5, Code: "unavailable", Message: "job store temporarily unavailable"})
			} else {
				log.Printf("request_id=%q internal error: %v", rid, err) // #nosec G706 -- logged value is operator config or sanitized, reviewed 2026-10-21
				fail(&apiError{status: 500, Code: "internal", Message: "internal error"})
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(out)
	}
}

func (s *Server) create(_ http.ResponseWriter, p Principal, _ *http.Request, body []byte) (int, any, error) {
	var spec ExportSpec
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&spec); err != nil {
		return 0, nil, &apiError{status: 400, Code: "invalid_request", Message: "malformed JSON: " + err.Error()}
	}
	spec, err := spec.Normalize()
	if err != nil {
		return 0, nil, &apiError{status: 400, Code: "invalid_spec", Message: err.Error()}
	}
	id := NewExportID()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return 0, nil, &apiError{status: 503, retryAfter: 30, Code: "shutting_down", Message: "service is shutting down"}
	}
	if s.OnEnqueue == nil && s.queued >= s.MaxQueue {
		s.mu.Unlock()
		return 0, nil, &apiError{status: 503, retryAfter: 30, Code: "queue_full", Message: "export queue is full"}
	}
	s.mu.Unlock()

	job := Job{ID: id, TenantID: p.TenantID, Subject: p.Subject, Spec: spec, Status: StatusQueued,
		ObjectKey: ObjectKey(p.TenantID, id, spec.Ext()), CreatedAt: s.now()}
	release := func() {}
	quotaErr := func(qerr error) *apiError {
		if s.Audit != nil {
			_ = s.Audit.Emit(AuditRecord{Time: s.now(), TenantID: p.TenantID, Subject: p.Subject, ExportID: id,
				SpecHash: spec.Hash(), Outcome: OutcomeQuotaExceeded, Error: qerr.Error()})
		}
		ra := 60
		code := "quota_exceeded"
		if errors.Is(qerr, ErrQuotaExceeded) {
			ra = int(time.Until(s.now().Truncate(24 * time.Hour).Add(24 * time.Hour)).Seconds()) // next UTC midnight
			if ra < 1 {
				ra = 1
			}
		} else {
			code = "too_many_concurrent"
		}
		return &apiError{status: 429, retryAfter: ra, Code: code, Message: qerr.Error()}
	}
	if rs, ok := s.Quota.(ReservingCreator); ok {
		// Durable quota: limits are checked and the job row inserted in ONE transaction (replica-safe).
		if err := rs.CreateReserved(job); err != nil {
			if errors.Is(err, ErrQuotaExceeded) || errors.Is(err, ErrTooManyConcurrent) {
				return 0, nil, quotaErr(err)
			}
			return 0, nil, err
		}
	} else {
		if s.Quota != nil {
			rel, qerr := s.Quota.Acquire(p.TenantID)
			if qerr != nil {
				return 0, nil, quotaErr(qerr)
			}
			release = rel
		}
		if err := s.Jobs.Create(job); err != nil {
			release()
			return 0, nil, err
		}
	}
	if s.OnEnqueue != nil {
		s.OnEnqueue()
		return http.StatusAccepted, map[string]any{"id": id, "status": StatusQueued}, nil
	}
	s.mu.Lock()
	if s.closed { // lost the race with Shutdown
		s.mu.Unlock()
		release()
		s.finish(job, func(j *Job) { j.Status, j.ErrorCode, j.FinishedAt = StatusFailed, "shutdown", s.now() })
		return 0, nil, &apiError{status: 503, retryAfter: 30, Code: "shutting_down", Message: "service is shutting down"}
	}
	s.queued++
	s.wg.Add(1)
	s.mu.Unlock()
	go s.run(p, job, release)
	return http.StatusAccepted, map[string]any{"id": id, "status": StatusQueued}, nil
}

// finish updates a job unless it is already terminal.
func (s *Server) finish(j Job, fn func(*Job)) {
	_, _ = s.Jobs.Update(j.TenantID, j.ID, func(c *Job) {
		if !c.Terminal() {
			fn(c)
		}
	})
}

func (s *Server) run(p Principal, job Job, release func()) {
	defer s.wg.Done()
	defer release()
	dequeue := func() { s.mu.Lock(); s.queued--; s.mu.Unlock() }
	select {
	case s.sem <- struct{}{}:
		dequeue()
	case <-s.base.Done():
		dequeue()
		s.finish(job, func(c *Job) { c.Status, c.ErrorCode, c.FinishedAt = StatusFailed, "shutdown", s.now() })
		return
	}
	defer func() { <-s.sem }()
	s.finish(job, func(c *Job) { c.Status, c.StartedAt = StatusRunning, s.now() })

	ctx, cancel := context.WithTimeout(s.base, s.JobTimeout)
	defer cancel()
	r := *s.Runner
	r.Quota = nil // reserved at POST
	if r.Audit == nil {
		r.Audit = s.Audit
	}
	res, err := r.runNoPresign(ctx, p, job.ID, job.Spec)
	if err != nil {
		code := errCode(err, ctx)
		s.finish(job, func(c *Job) {
			c.Status, c.ErrorCode, c.Rows, c.Bytes, c.FinishedAt = StatusFailed, code, res.Rows, res.Bytes, s.now()
		})
		return
	}
	s.finish(job, func(c *Job) {
		c.Status, c.Rows, c.Bytes, c.ObjectKey, c.FinishedAt = StatusSucceeded, res.Rows, res.Bytes, res.Key, s.now()
	})
}

// runNoPresign runs the export without issuing a URL (URLs are signed at GET time and never stored).
func (r *Runner) runNoPresign(ctx context.Context, p Principal, id string, spec ExportSpec) (Result, error) {
	c := *r
	c.Presign = nil
	return c.Run(ctx, p, id, spec)
}

func errCode(err error, ctx context.Context) string {
	switch {
	case errors.Is(err, ErrCHTimeout) && ctx.Err() == nil:
		return "clickhouse_timeout"
	case errors.Is(err, ErrInvalidSpec):
		return "invalid_spec"
	case errors.Is(err, ErrRowCap):
		return "row_cap_exceeded"
	case errors.Is(err, ErrByteCap):
		return "byte_cap_exceeded"
	case errors.Is(err, ErrClickHouse), errors.Is(err, ErrTruncated):
		if ctx.Err() == nil {
			return "clickhouse_error"
		}
		return errCodeCtx(err, ctx)
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	case errors.Is(err, context.Canceled) || ctx.Err() != nil:
		return "shutdown"
	}
	return "export_failed"
}

func errCodeCtx(err error, ctx context.Context) string {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return "timeout"
	}
	return "shutdown"
}

type jobView struct {
	ID          string     `json:"id"`
	Status      string     `json:"status"`
	Rows        int64      `json:"rows"`
	Bytes       int64      `json:"bytes"`
	ErrorCode   string     `json:"error_code,omitempty"`
	Format      string     `json:"format"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	DownloadURL string     `json:"download_url,omitempty"`
	ExpiresAt   *time.Time `json:"download_expires_at,omitempty"`
}

func tp(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (s *Server) get(_ http.ResponseWriter, p Principal, r *http.Request, _ []byte) (int, any, error) {
	notFound := &apiError{status: 404, Code: "not_found", Message: "export not found"}
	id := r.PathValue("id")
	if !exportIDRe.MatchString(id) {
		return 0, nil, notFound
	}
	j, err := s.Jobs.Get(p.TenantID, id) // tenant-scoped: other tenants' ids are 404, never 403
	if err != nil {
		if errors.Is(err, ErrJobNotFound) {
			return 0, nil, notFound
		}
		return 0, nil, err
	}
	v := jobView{ID: j.ID, Status: j.Status, Rows: j.Rows, Bytes: j.Bytes, ErrorCode: j.ErrorCode, Format: j.Spec.Format,
		CreatedAt: j.CreatedAt, StartedAt: tp(j.StartedAt), FinishedAt: tp(j.FinishedAt)}
	if j.Status == StatusSucceeded && s.Runner.Presign != nil {
		u, exp, err := s.Runner.DownloadURL(p, j.ObjectKey, s.Runner.TTL)
		if err != nil {
			log.Printf("presign %s: %v", j.ID, err)
			return 0, nil, &apiError{status: 502, Code: "presign_failed", Message: "could not issue download link"}
		}
		v.DownloadURL, v.ExpiresAt = u, &exp
	}
	return http.StatusOK, v, nil
}

// Shutdown stops accepting jobs and waits for queued/running ones until ctx expires; then it cancels
// them, waits for the goroutines and marks every job still unfinished FAILED ("shutdown").
func (s *Server) Shutdown(ctx context.Context) {
	s.init()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-ctx.Done():
		s.cancel()
		<-done
	}
	s.cancel()
	s.Jobs.FailUnfinished("shutdown", s.now())
}
