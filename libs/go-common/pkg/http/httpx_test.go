package httpx

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/astryon/go-common/pkg/authn"
	"github.com/astryon/go-common/pkg/logging"
)

func TestRequestID(t *testing.T) {
	tests := []struct {
		name, in string
		keep     bool
	}{
		{"absent", "", false},
		{"valid", "abc-123", true},
		{"too long", strings.Repeat("a", 129), false},
		{"control char", "a\x01b", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var ctxID string
			h := RequestIDMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { ctxID = RequestID(r.Context()) }))
			req := httptest.NewRequest("GET", "/", nil)
			if tc.in != "" {
				req.Header.Set(RequestIDHeader, tc.in)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			got := rec.Header().Get(RequestIDHeader)
			if got == "" || got != ctxID {
				t.Fatalf("header %q ctx %q", got, ctxID)
			}
			if tc.keep != (got == tc.in) {
				t.Fatalf("keep=%v got %q", tc.keep, got)
			}
		})
	}
}

func TestRecoverAndAccessLog(t *testing.T) {
	var buf bytes.Buffer
	lg := slog.New(slog.NewJSONHandler(&buf, nil))
	tests := []struct {
		name   string
		h      http.HandlerFunc
		status int
		code   string
	}{
		{"panic", func(http.ResponseWriter, *http.Request) { panic("boom") }, 500, `"code":"INTERNAL"`},
		{"ok", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(204) }, 204, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			buf.Reset()
			h := Chain(tc.h, RequestIDMiddleware, AccessLog, Recover)
			req := httptest.NewRequest("GET", "/x", nil).WithContext(logging.WithContext(context.Background(), lg))
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status || !strings.Contains(rec.Body.String(), tc.code) {
				t.Fatalf("code %d body %s", rec.Code, rec.Body)
			}
			if tc.code != "" && !strings.Contains(rec.Body.String(), `"trace_id":"`+rec.Header().Get(RequestIDHeader)) {
				t.Fatalf("trace_id missing: %s", rec.Body)
			}
			log := buf.String()
			if !strings.Contains(log, `"http request"`) || !strings.Contains(log, `"request_id"`) {
				t.Fatalf("log: %s", log)
			}
		})
	}
}

func TestBodyLimit(t *testing.T) {
	tests := []struct {
		name   string
		body   string
		status int
	}{
		{"under", "12345", 200},
		{"at", "1234567890", 200},
		{"over", "12345678901", 413},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if _, err := io.ReadAll(r.Body); err != nil {
					WriteBodyError(w, r, err, 10)
					return
				}
				w.WriteHeader(200)
			}), RequestIDMiddleware, BodyLimit(10))
			req := httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			req.ContentLength = -1 // force the reader-based path
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("got %d", rec.Code)
			}
			// Content-Length fast path
			req = httptest.NewRequest("POST", "/", strings.NewReader(tc.body))
			rec = httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status {
				t.Fatalf("CL path got %d", rec.Code)
			}
		})
	}
}

type fakeVerifier struct {
	c   *authn.Claims
	err error
	got string
}

func (f *fakeVerifier) Verify(_ context.Context, tok string) (*authn.Claims, error) {
	f.got = tok
	return f.c, f.err
}

func TestAuthn(t *testing.T) {
	tests := []struct {
		name   string
		header string
		extra  map[string]string
		v      *fakeVerifier
		status int
		tenant string
	}{
		{"ok", "Bearer tok", nil, &fakeVerifier{c: &authn.Claims{TenantID: "t1"}}, 200, "t1"},
		{"spoof headers ignored", "bearer tok", map[string]string{"X-Tenant-ID": "evil", "Tenant-Id": "evil"}, &fakeVerifier{c: &authn.Claims{TenantID: "t1"}}, 200, "t1"},
		{"no header", "", nil, &fakeVerifier{}, 401, ""},
		{"not bearer", "Basic abc", nil, &fakeVerifier{}, 401, ""},
		{"invalid", "Bearer tok", nil, &fakeVerifier{err: authn.ErrInvalidToken}, 401, ""},
		{"empty tenant", "Bearer tok", nil, &fakeVerifier{c: &authn.Claims{}}, 401, ""},
		{"jwks down", "Bearer tok", nil, &fakeVerifier{err: errors.Join(authn.ErrJWKSUnavailable)}, 503, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var tenant string
			h := Chain(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { tenant = TenantID(r.Context()) }),
				RequestIDMiddleware, Authn(tc.v))
			req := httptest.NewRequest("POST", "/", strings.NewReader(`{"tenant_id":"evil"}`))
			if tc.header != "" {
				req.Header.Set("Authorization", tc.header)
			}
			for k, v := range tc.extra {
				req.Header.Set(k, v)
			}
			rec := httptest.NewRecorder()
			h.ServeHTTP(rec, req)
			if rec.Code != tc.status || tenant != tc.tenant {
				t.Fatalf("status %d tenant %q body %s", rec.Code, tenant, rec.Body)
			}
		})
	}
	if TenantID(context.Background()) != "" {
		t.Fatal("empty ctx must have no tenant")
	}
}
