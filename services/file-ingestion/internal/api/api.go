// Package api serves the upload endpoints: POST /v1/files/uploads (create multipart session, scope
// astryon:uploads:write) and POST /v1/files/uploads/{id}/complete (finish, verify SHA-256, enqueue the job).
// The tenant comes only from the authenticated Principal; bodies that name a tenant are rejected.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/profile"
	"github.com/astryon/file-ingestion/internal/quota"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/upload"
)

const ScopeUploadsWrite = "astryon:uploads:write"

var ErrUnauthenticated = errors.New("UNAUTHENTICATED")

type Principal struct {
	TenantID string
	Scopes   []string
}

type Authenticator interface {
	Authenticate(r *http.Request) (Principal, error)
}

// Multipart is the S3 part of the flow (satisfied by *s3.Client).
type Multipart interface {
	CompleteMultipart(ctx context.Context, key, uploadID string, parts []s3.Part) error
	Abort(ctx context.Context, key, uploadID string) error
}

// Enqueuer creates the process_file job (satisfied by *jobs.Store).
type Enqueuer interface {
	Enqueue(ctx context.Context, tenant, source, fileObject string) (string, error)
}

type Server struct {
	Auth      Authenticator
	Uploads   upload.Service
	Files     objects.Store
	Objs      objects.Objects
	Multipart Multipart
	Jobs      Enqueuer
	Bucket    string
	// Insights reads the latest job of a file object for GET (nil = job info omitted); *jobs.Store satisfies it.
	Insights interface {
		Describe(ctx context.Context, tenant, fileObject string) (jobs.Summary, error)
	}
	// Quota enforces per-tenant upload quotas (nil = unlimited). Store errors fail closed with 503.
	Quota interface {
		Check(ctx context.Context, tenant string, size int64) error
	}

	mu     sync.Mutex
	counts map[string]int64
	denied map[string]int64
}

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// Handler routes the endpoints and counts responses by route and status code (see WritePrometheus).
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/files/uploads", s.count("create", s.create))
	mux.HandleFunc("GET /v1/files/uploads/{id}", s.count("get", s.get))
	mux.HandleFunc("POST /v1/files/uploads/{id}/complete", s.count("complete", s.complete))
	return mux
}

type statusWriter struct {
	http.ResponseWriter
	code int
}

func (w *statusWriter) WriteHeader(c int) { w.code = c; w.ResponseWriter.WriteHeader(c) }

func (s *Server) count(route string, h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		sw := &statusWriter{ResponseWriter: w, code: 200}
		h(sw, r)
		s.mu.Lock()
		if s.counts == nil {
			s.counts = map[string]int64{}
		}
		s.counts[fmt.Sprintf("%s|%d", route, sw.code)]++
		s.mu.Unlock()
	}
}

// WritePrometheus renders astryon_file_api_requests_total{route,code}.
func (s *Server) WritePrometheus(w io.Writer) {
	s.mu.Lock()
	defer s.mu.Unlock()
	fmt.Fprint(w, "# HELP astryon_file_api_requests_total Upload API responses\n# TYPE astryon_file_api_requests_total counter\n")
	keys := make([]string, 0, len(s.counts))
	for k := range s.counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		route, code, _ := strings.Cut(k, "|")
		fmt.Fprintf(w, "astryon_file_api_requests_total{route=%q,code=%q} %d\n", route, code, s.counts[k])
	}
	fmt.Fprint(w, "# HELP astryon_file_api_quota_denied_total Uploads refused by tenant quota\n# TYPE astryon_file_api_quota_denied_total counter\n")
	reasons := make([]string, 0, len(s.denied))
	for k := range s.denied {
		reasons = append(reasons, k)
	}
	sort.Strings(reasons)
	for _, k := range reasons {
		fmt.Fprintf(w, "astryon_file_api_quota_denied_total{reason=%q} %d\n", k, s.denied[k])
	}
}

func (s *Server) principal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := s.Auth.Authenticate(r)
	if err != nil || p.TenantID == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeErr(w, 401, "UNAUTHENTICATED")
		return Principal{}, false
	}
	for _, sc := range p.Scopes {
		if sc == ScopeUploadsWrite {
			return p, true
		}
	}
	writeErr(w, 403, "FORBIDDEN")
	return Principal{}, false
}

var messages = map[string]string{
	"UNAUTHENTICATED":     "Valid access token required.",
	"FORBIDDEN":           "Scope astryon:uploads:write required.",
	"BODY_INVALID":        "Request body or path is invalid.",
	"NOT_FOUND":           "File object not found.",
	"STORAGE_UNAVAILABLE": "Object storage is unavailable; retry later.",
	"SOURCE_UNKNOWN":      "Source does not exist for this tenant.",
	"ALREADY_REGISTERED":  "This file was already registered for the tenant.",
	"UNAVAILABLE":         "Temporarily unavailable; retry later.",
	"CHECKSUM_MISMATCH":   "Uploaded content does not match the declared SHA-256 or size; the object was discarded.",
	"FILE_TOO_LARGE":      "File exceeds the maximum upload size for this tenant.",
	"TOO_MANY_UPLOADS":    "Too many uploads are open for this tenant; complete or wait for them to expire, then retry.",
	"UPLOAD_QUOTA":        "The tenant's upload byte quota for the last 24 hours is exhausted.",
	"UPLOAD_INCOMPLETE":   "Parts are missing or their ETags do not match.",
}

// writeErr uses the public error shape from docs/errors/api-error-catalog.md.
func writeErr(w http.ResponseWriter, code int, id string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"code": id, "message": messages[id]})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// decode reads a small strict JSON body: unknown fields (including tenant_id) and trailing data are rejected.
func decode(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil || dec.More() {
		writeErr(w, 400, "BODY_INVALID")
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeErr(w, 400, "BODY_INVALID")
		return false
	}
	return true
}

type createReq struct {
	SourceID    string `json:"source_id"`
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	SHA256      string `json:"sha256"`
	SizeBytes   int64  `json:"size_bytes"`
	Parts       int    `json:"parts"`
	// SourceType selects a processing profile (internal/profile); unknown values use the "unknown" profile.
	SourceType  string            `json:"source_type"`
	Tags        map[string]string `json:"tags"`
	Sensitivity string            `json:"sensitivity"` // public|internal|restricted; default from the profile, never below its floor
}

const (
	maxTags     = 16
	maxTagValue = 64
)

var (
	tagKeyRe = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,31}$`)
	tagValRe = regexp.MustCompile(`^[A-Za-z0-9_.:/@+ -]{0,64}$`)
	typeRe   = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)
)

func validTags(t map[string]string) bool {
	if len(t) > maxTags {
		return false
	}
	for k, v := range t {
		if !tagKeyRe.MatchString(k) || !tagValRe.MatchString(v) {
			return false
		}
	}
	return true
}

type createResp struct {
	FileObjectID string    `json:"file_object_id"`
	UploadID     string    `json:"upload_id"`
	PartURLs     []string  `json:"part_urls"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func (s *Server) create(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	var in createReq
	if !decode(w, r, &in) {
		return
	}
	in.SHA256 = strings.ToLower(in.SHA256)
	if !uuidRe.MatchString(in.SourceID) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if !validTags(in.Tags) || (in.SourceType != "" && !typeRe.MatchString(in.SourceType)) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	prof := profile.Select(in.SourceType)
	sens, ok := prof.Sensitivity(in.Sensitivity)
	if !ok || !prof.AllowsFormat(in.ContentType, in.Filename) || prof.MissingTag(in.Tags) != "" {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if prof.MaxBytes > 0 && in.SizeBytes > prof.MaxBytes {
		writeErr(w, 413, "FILE_TOO_LARGE")
		return
	}
	if s.Quota != nil {
		if !s.checkQuota(w, r, p.TenantID, in.SizeBytes) {
			return
		}
	}
	sess, err := s.Uploads.Create(r.Context(), p.TenantID, upload.Request{Filename: in.Filename, ContentType: in.ContentType,
		SHA256: in.SHA256, SizeBytes: in.SizeBytes, Parts: in.Parts})
	if errors.Is(err, upload.ErrInvalid) {
		writeErr(w, 400, "BODY_INVALID")
		return
	}
	if err != nil {
		writeErr(w, 502, "STORAGE_UNAVAILABLE")
		return
	}
	id, err := s.Files.Register(r.Context(), p.TenantID, in.SourceID, objects.File{Bucket: s.Bucket, Key: sess.ObjectKey, SHA256: in.SHA256, Size: in.SizeBytes, ContentType: in.ContentType,
		SourceType: prof.Name, Sensitivity: sens, Tags: in.Tags})
	if err != nil {
		_ = s.Multipart.Abort(r.Context(), sess.ObjectKey, sess.UploadID)
		switch {
		case errors.Is(err, objects.ErrUnknownSource):
			writeErr(w, 422, "SOURCE_UNKNOWN")
		case errors.Is(err, objects.ErrAlreadyExists):
			writeErr(w, 409, "ALREADY_REGISTERED")
		default:
			writeErr(w, 503, "UNAVAILABLE")
		}
		return
	}
	writeJSON(w, 201, createResp{FileObjectID: id, UploadID: sess.UploadID, PartURLs: sess.PartURLs, ExpiresAt: sess.ExpiresAt})
}

// checkQuota maps quota results to 413 (file size) or 429 (open uploads, daily bytes, with Retry-After).
func (s *Server) checkQuota(w http.ResponseWriter, r *http.Request, tenant string, size int64) bool {
	err := s.Quota.Check(r.Context(), tenant, size)
	reason := ""
	switch {
	case err == nil:
		return true
	case errors.Is(err, quota.ErrFileTooLarge):
		reason = "file_size"
		writeErr(w, 413, "FILE_TOO_LARGE")
	case errors.Is(err, quota.ErrTooManyOpen):
		reason = "open_uploads"
		w.Header().Set("Retry-After", "60")
		writeErr(w, 429, "TOO_MANY_UPLOADS")
	case errors.Is(err, quota.ErrDailyBytesFull):
		reason = "daily_bytes"
		w.Header().Set("Retry-After", "3600")
		writeErr(w, 429, "UPLOAD_QUOTA")
	default:
		writeErr(w, 503, "UNAVAILABLE")
		return false
	}
	s.mu.Lock()
	if s.denied == nil {
		s.denied = map[string]int64{}
	}
	s.denied[reason]++
	s.mu.Unlock()
	return false
}

type getResp struct {
	FileObjectID string            `json:"file_object_id"`
	SourceID     string            `json:"source_id"`
	SourceType   string            `json:"source_type"`
	Committed    bool              `json:"committed"`
	SizeBytes    int64             `json:"size_bytes"`
	Sensitivity  string            `json:"sensitivity"`
	Tags         map[string]string `json:"tags"`
	// Profile is the declared handling metadata of the source type (retention, read scope, export flag are not enforced yet).
	Profile struct {
		RetentionClass    string `json:"retention_class"`
		RequiredReadScope string `json:"required_read_scope"`
		ExportAllowed     bool   `json:"export_allowed"`
	} `json:"profile"`
	Job *jobResp `json:"job,omitempty"`
}

type jobResp struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	// Error is the catalog code when the job failed or was quarantined (e.g. CREDENTIALS_DETECTED).
	Error string `json:"error,omitempty"`
	// Labels hold per-label match counts and column/field names only, never matched values.
	Labels classify.Labels `json:"labels"`
}

func (s *Server) get(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	f, err := s.Files.Get(r.Context(), p.TenantID, id)
	if errors.Is(err, objects.ErrNotFound) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	out := getResp{FileObjectID: f.ID, SourceID: f.SourceID, SourceType: f.SourceType, Committed: f.Committed, SizeBytes: f.Size,
		Sensitivity: f.Sensitivity, Tags: f.Tags}
	if out.Tags == nil {
		out.Tags = map[string]string{}
	}
	pr := profile.Select(f.SourceType)
	out.Profile.RetentionClass, out.Profile.RequiredReadScope, out.Profile.ExportAllowed = pr.RetentionClass, pr.RequiredReadScope, pr.ExportAllowed
	if s.Insights != nil {
		sum, err := s.Insights.Describe(r.Context(), p.TenantID, id)
		switch {
		case err == nil:
			out.Job = &jobResp{ID: sum.ID, Status: sum.Status, Error: sum.Error, Labels: sum.Labels}
		case !errors.Is(err, jobs.ErrNoSummary):
			writeErr(w, 503, "UNAVAILABLE")
			return
		}
	}
	writeJSON(w, 200, out)
}

type completeReq struct {
	UploadID string `json:"upload_id"`
	Parts    []struct {
		Number int    `json:"number"`
		ETag   string `json:"etag"`
	} `json:"parts"`
}

func (s *Server) complete(w http.ResponseWriter, r *http.Request) {
	p, ok := s.principal(w, r)
	if !ok {
		return
	}
	id := r.PathValue("id")
	var in completeReq
	if !uuidRe.MatchString(id) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if !decode(w, r, &in) || !validParts(w, in) {
		return
	}
	f, err := s.Files.Get(r.Context(), p.TenantID, id)
	if errors.Is(err, objects.ErrNotFound) {
		writeErr(w, 404, "NOT_FOUND")
		return
	}
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	if !f.Committed { // a repeated call after success skips S3 and just returns the (idempotent) job
		parts := make([]s3.Part, len(in.Parts))
		for i, pt := range in.Parts {
			parts[i] = s3.Part{Number: pt.Number, ETag: pt.ETag}
		}
		if err := s.Multipart.CompleteMultipart(r.Context(), f.Key, in.UploadID, parts); err != nil {
			writeErr(w, 422, "UPLOAD_INCOMPLETE")
			return
		}
		switch err := objects.Verify(r.Context(), s.Files, s.Objs, p.TenantID, id); {
		case errors.Is(err, objects.ErrChecksumMismatch):
			writeErr(w, 422, "CHECKSUM_MISMATCH")
			return
		case err != nil:
			writeErr(w, 503, "UNAVAILABLE")
			return
		}
	}
	job, err := s.Jobs.Enqueue(r.Context(), p.TenantID, f.SourceID, id)
	if err != nil {
		writeErr(w, 503, "UNAVAILABLE")
		return
	}
	writeJSON(w, 200, map[string]string{"file_object_id": id, "status": "committed", "job_id": job})
}

func validParts(w http.ResponseWriter, in completeReq) bool {
	if in.UploadID == "" || len(in.UploadID) > 512 || len(in.Parts) < 1 || len(in.Parts) > 10000 {
		writeErr(w, 400, "BODY_INVALID")
		return false
	}
	for i, pt := range in.Parts {
		if pt.Number != i+1 || pt.ETag == "" || len(pt.ETag) > 256 {
			writeErr(w, 400, "BODY_INVALID")
			return false
		}
	}
	return true
}

// MultipartStarter adapts *s3.Client to upload.Store (expiry -> ttl).
type MultipartStarter struct {
	C   *s3.Client
	Now func() time.Time
}

func (m MultipartStarter) CreateMultipart(ctx context.Context, key string, parts int, expires time.Time) (string, []string, error) {
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	return m.C.CreateMultipart(ctx, key, parts, expires.Sub(now()))
}
