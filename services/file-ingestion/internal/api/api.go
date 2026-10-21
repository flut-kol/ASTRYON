// Package api serves the upload endpoints: POST /v1/files/uploads (create multipart session, scope
// astryon:uploads:write) and POST /v1/files/uploads/{id}/complete (finish, verify SHA-256, enqueue the job).
// The tenant comes only from the authenticated Principal; bodies that name a tenant are rejected.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/upload"
)

const ScopeUploadsWrite = "astryon:uploads:write"

var ErrUnauthenticated = errors.New("unauthenticated")

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
}

var uuidRe = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/files/uploads", s.create)
	mux.HandleFunc("POST /v1/files/uploads/{id}/complete", s.complete)
	return mux
}

func (s *Server) principal(w http.ResponseWriter, r *http.Request) (Principal, bool) {
	p, err := s.Auth.Authenticate(r)
	if err != nil || p.TenantID == "" {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeErr(w, 401, "unauthenticated")
		return Principal{}, false
	}
	for _, sc := range p.Scopes {
		if sc == ScopeUploadsWrite {
			return p, true
		}
	}
	writeErr(w, 403, "forbidden")
	return Principal{}, false
}

func writeErr(w http.ResponseWriter, code int, id string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": id})
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
		writeErr(w, 400, "invalid_request")
		return false
	}
	if _, err := dec.Token(); err != io.EOF {
		writeErr(w, 400, "invalid_request")
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
		writeErr(w, 400, "invalid_request")
		return
	}
	sess, err := s.Uploads.Create(r.Context(), p.TenantID, upload.Request{Filename: in.Filename, ContentType: in.ContentType,
		SHA256: in.SHA256, SizeBytes: in.SizeBytes, Parts: in.Parts})
	if errors.Is(err, upload.ErrInvalid) {
		writeErr(w, 400, "invalid_request")
		return
	}
	if err != nil {
		writeErr(w, 502, "storage_unavailable")
		return
	}
	id, err := s.Files.Register(r.Context(), p.TenantID, in.SourceID, objects.File{Bucket: s.Bucket, Key: sess.ObjectKey, SHA256: in.SHA256, Size: in.SizeBytes})
	if err != nil {
		_ = s.Multipart.Abort(r.Context(), sess.ObjectKey, sess.UploadID)
		switch {
		case errors.Is(err, objects.ErrUnknownSource):
			writeErr(w, 422, "unknown_source")
		case errors.Is(err, objects.ErrAlreadyExists):
			writeErr(w, 409, "already_registered")
		default:
			writeErr(w, 503, "unavailable")
		}
		return
	}
	writeJSON(w, 201, createResp{FileObjectID: id, UploadID: sess.UploadID, PartURLs: sess.PartURLs, ExpiresAt: sess.ExpiresAt})
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
		writeErr(w, 404, "not_found")
		return
	}
	if !decode(w, r, &in) || !validParts(w, in) {
		return
	}
	f, err := s.Files.Get(r.Context(), p.TenantID, id)
	if errors.Is(err, objects.ErrNotFound) {
		writeErr(w, 404, "not_found")
		return
	}
	if err != nil {
		writeErr(w, 503, "unavailable")
		return
	}
	if !f.Committed { // a repeated call after success skips S3 and just returns the (idempotent) job
		parts := make([]s3.Part, len(in.Parts))
		for i, pt := range in.Parts {
			parts[i] = s3.Part{Number: pt.Number, ETag: pt.ETag}
		}
		if err := s.Multipart.CompleteMultipart(r.Context(), f.Key, in.UploadID, parts); err != nil {
			writeErr(w, 422, "upload_incomplete")
			return
		}
		switch err := objects.Verify(r.Context(), s.Files, s.Objs, p.TenantID, id); {
		case errors.Is(err, objects.ErrChecksumMismatch):
			writeErr(w, 422, "checksum_mismatch")
			return
		case err != nil:
			writeErr(w, 503, "unavailable")
			return
		}
	}
	job, err := s.Jobs.Enqueue(r.Context(), p.TenantID, f.SourceID, id)
	if err != nil {
		writeErr(w, 503, "unavailable")
		return
	}
	writeJSON(w, 200, map[string]string{"file_object_id": id, "status": "committed", "job_id": job})
}

func validParts(w http.ResponseWriter, in completeReq) bool {
	if in.UploadID == "" || len(in.UploadID) > 512 || len(in.Parts) < 1 || len(in.Parts) > 10000 {
		writeErr(w, 400, "invalid_request")
		return false
	}
	for i, pt := range in.Parts {
		if pt.Number != i+1 || pt.ETag == "" || len(pt.ETag) > 256 {
			writeErr(w, 400, "invalid_request")
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
