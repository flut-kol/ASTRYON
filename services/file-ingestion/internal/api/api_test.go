package api

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/quota"
	"github.com/astryon/file-ingestion/internal/s3"
	"github.com/astryon/file-ingestion/internal/upload"
)

const (
	tenant = "0a1b2c3d-1111-4222-8333-444455556666"
	source = "0a1b2c3d-1111-4222-8333-444455557777"
	fileID = "0a1b2c3d-1111-4222-8333-444455558888"
	sha    = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type auth struct{ p Principal }

func (a auth) Authenticate(*http.Request) (Principal, error) {
	if a.p.TenantID == "" {
		return Principal{}, ErrUnauthenticated
	}
	return a.p, nil
}

type startStore struct{ fail error }

func (s startStore) CreateMultipart(_ context.Context, key string, parts int, _ time.Time) (string, []string, error) {
	if s.fail != nil {
		return "", nil, s.fail
	}
	return "up-1", make([]string, parts), nil
}

type files struct {
	f         objects.File
	registerE error
	tenants   []string
}

func (f *files) Register(_ context.Context, tenant, _ string, in objects.File) (string, error) {
	f.tenants = append(f.tenants, tenant)
	if f.registerE != nil {
		return "", f.registerE
	}
	f.f = in
	f.f.ID, f.f.SourceID = fileID, source
	return fileID, nil
}
func (f *files) Get(_ context.Context, tenant, id string) (objects.File, error) {
	if tenant != tenantA || id != f.f.ID {
		return objects.File{}, objects.ErrNotFound
	}
	return f.f, nil
}
func (f *files) MarkCommitted(context.Context, string, string, int64) error {
	f.f.Committed = true
	return nil
}
func (f *files) MarkRejected(context.Context, string, string) error { return nil }

const tenantA = tenant

type mp struct {
	completeE error
	completed int
	aborted   int
}

func (m *mp) CompleteMultipart(context.Context, string, string, []s3.Part) error {
	m.completed++
	return m.completeE
}
func (m *mp) Abort(context.Context, string, string) error { m.aborted++; return nil }

type objs struct{ body string }

func (o objs) Head(context.Context, string) (int64, error) { return int64(len(o.body)), nil }
func (o objs) Get(_ context.Context, _ string, _ int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(o.body)), nil
}
func (o objs) Delete(context.Context, string) error { return nil }

type enq struct{ calls []string }

func (e *enq) Enqueue(_ context.Context, t, s, f string) (string, error) {
	e.calls = append(e.calls, t+"/"+s+"/"+f)
	return "job-1", nil
}

func newServer(p Principal) (*Server, *files, *mp, *enq) {
	fs, m, e := &files{}, &mp{}, &enq{}
	return &Server{Auth: auth{p}, Files: fs, Objs: objs{}, Multipart: m, Jobs: e, Bucket: "b",
		Uploads: upload.Service{Store: startStore{}, Now: time.Now, MaxBytes: 1 << 30, MaxParts: 100,
			ContentTypes: map[string]bool{"application/x-ndjson": true}}}, fs, m, e
}

func call(h http.Handler, path, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", path, bytes.NewBufferString(body)))
	return w
}

var okBody = `{"source_id":"` + source + `","filename":"a.ndjson","content_type":"application/x-ndjson","sha256":"` + sha + `","size_bytes":10,"parts":2}`

func TestCreateUsesTrustedTenantAndRegistersFile(t *testing.T) {
	s, fs, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	w := call(s.Handler(), "/v1/files/uploads", okBody)
	if w.Code != 201 || !strings.Contains(w.Body.String(), fileID) || fs.tenants[0] != tenant || !strings.HasPrefix(fs.f.Key, tenant+"/uploads/") {
		t.Fatalf("%d %s %+v", w.Code, w.Body, fs)
	}
}

func TestCreateRejectsBadInputAndAuthFailures(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	for name, body := range map[string]string{
		"tenant in body": strings.Replace(okBody, `{`, `{"tenant_id":"x",`, 1),
		"bad source":     strings.Replace(okBody, source, "nope", 1),
		"bad sha":        strings.Replace(okBody, sha, "zz", 1),
		"too many parts": strings.Replace(okBody, `"parts":2`, `"parts":1000`, 1),
		"unknown type":   strings.Replace(okBody, "application/x-ndjson", "text/html", 1),
		"trailing data":  okBody + `{}`,
		"path traversal": strings.Replace(okBody, "a.ndjson", "../a", 1),
		"empty":          ``,
	} {
		if w := call(s.Handler(), "/v1/files/uploads", body); w.Code != 400 {
			t.Errorf("%s: %d %s", name, w.Code, w.Body)
		}
	}
	noScope, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{"astryon:events:write"}})
	if w := call(noScope.Handler(), "/v1/files/uploads", okBody); w.Code != 403 {
		t.Errorf("scope: %d", w.Code)
	}
	anon, _, _, _ := newServer(Principal{})
	if w := call(anon.Handler(), "/v1/files/uploads", okBody); w.Code != 401 || w.Header().Get("WWW-Authenticate") == "" {
		t.Errorf("anon: %d", w.Code)
	}
}

func TestCreateMapsStoreErrorsAndAbortsUpload(t *testing.T) {
	for err, want := range map[error]int{objects.ErrUnknownSource: 422, objects.ErrAlreadyExists: 409, errors.New("db"): 503} {
		s, fs, m, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
		fs.registerE = err
		if w := call(s.Handler(), "/v1/files/uploads", okBody); w.Code != want || m.aborted != 1 {
			t.Errorf("%v: %d aborted=%d", err, w.Code, m.aborted)
		}
	}
}

const completeBody = `{"upload_id":"up-1","parts":[{"number":1,"etag":"\"a\""},{"number":2,"etag":"\"b\""}]}`

func prepared(t *testing.T, body string) (*Server, *files, *mp, *enq) {
	s, fs, m, e := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	call(s.Handler(), "/v1/files/uploads", okBody)
	return s, fs, m, e
}

func TestCompleteVerifiesThenEnqueuesOnceAndIsIdempotent(t *testing.T) {
	s, fs, m, e := prepared(t, "")
	s.Objs = objs{body: ""} // sha256 of empty content == sha, declared size 10 would mismatch: use size -1
	fs.f.Size = -1
	w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "job-1") || !fs.f.Committed || m.completed != 1 {
		t.Fatalf("%d %s %+v", w.Code, w.Body, fs.f)
	}
	w = call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody)
	if w.Code != 200 || m.completed != 1 || len(e.calls) != 2 || e.calls[0] != tenant+"/"+source+"/"+fileID {
		t.Fatalf("repeat: %d completed=%d calls=%v", w.Code, m.completed, e.calls)
	}
}

func TestCompleteRejectsChecksumMismatchAndIncompleteUploads(t *testing.T) {
	s, fs, _, e := prepared(t, "")
	s.Objs = objs{body: "different"}
	fs.f.Size = -1
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody); w.Code != 422 || !strings.Contains(w.Body.String(), "CHECKSUM_MISMATCH") || len(e.calls) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	s, _, m, e := prepared(t, "")
	m.completeE = errors.New("InvalidPart")
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody); w.Code != 422 || len(e.calls) != 0 {
		t.Fatalf("%d", w.Code)
	}
}

func TestCompleteIsTenantScopedAndValidatesInput(t *testing.T) {
	s, _, _, _ := prepared(t, "")
	other := &Server{Auth: auth{Principal{TenantID: "0a1b2c3d-1111-4222-8333-999999999999", Scopes: []string{ScopeUploadsWrite}}},
		Files: s.Files, Objs: s.Objs, Multipart: s.Multipart, Jobs: s.Jobs}
	if w := call(other.Handler(), "/v1/files/uploads/"+fileID+"/complete", completeBody); w.Code != 404 {
		t.Fatalf("cross-tenant: %d", w.Code)
	}
	for name, body := range map[string]string{
		"no parts":      `{"upload_id":"u","parts":[]}`,
		"gap":           `{"upload_id":"u","parts":[{"number":2,"etag":"x"}]}`,
		"no upload id":  `{"parts":[{"number":1,"etag":"x"}]}`,
		"unknown field": `{"upload_id":"u","tenant_id":"t","parts":[{"number":1,"etag":"x"}]}`,
	} {
		if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/complete", body); w.Code != 400 {
			t.Errorf("%s: %d", name, w.Code)
		}
	}
	if w := call(s.Handler(), "/v1/files/uploads/not-a-uuid/complete", completeBody); w.Code != 404 {
		t.Errorf("bad id: %d", w.Code)
	}
}

func TestMetricsCountResponsesByRouteAndCode(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	call(s.Handler(), "/v1/files/uploads", okBody)
	call(s.Handler(), "/v1/files/uploads", `{}`)
	var b strings.Builder
	s.WritePrometheus(&b)
	for _, want := range []string{`{route="create",code="201"} 1`, `{route="create",code="400"} 1`} {
		if !strings.Contains(b.String(), want) {
			t.Errorf("missing %s in\n%s", want, b.String())
		}
	}
}

type fakeQuota struct{ err error }

func (q fakeQuota) Check(context.Context, string, int64) error { return q.err }

func TestQuotaMapsTo413And429AndFailsClosed(t *testing.T) {
	for _, c := range []struct {
		err   error
		code  int
		id    string
		retry bool
	}{
		{quota.ErrFileTooLarge, 413, "FILE_TOO_LARGE", false},
		{quota.ErrTooManyOpen, 429, "TOO_MANY_UPLOADS", true},
		{quota.ErrDailyBytesFull, 429, "UPLOAD_QUOTA", true},
		{errors.New("db down"), 503, "UNAVAILABLE", false},
	} {
		s, fs, m, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
		s.Quota = fakeQuota{c.err}
		w := call(s.Handler(), "/v1/files/uploads", okBody)
		if w.Code != c.code || !strings.Contains(w.Body.String(), c.id) || (w.Header().Get("Retry-After") != "") != c.retry {
			t.Errorf("%v: %d %s %v", c.err, w.Code, w.Body, w.Header())
		}
		if len(fs.tenants) != 0 || m.aborted != 0 {
			t.Errorf("%v: nothing may be registered or started", c.err)
		}
	}
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	s.Quota = fakeQuota{quota.ErrTooManyOpen}
	call(s.Handler(), "/v1/files/uploads", okBody)
	var b bytes.Buffer
	s.WritePrometheus(&b)
	if !strings.Contains(b.String(), `astryon_file_api_quota_denied_total{reason="open_uploads"} 1`) {
		t.Fatal(b.String())
	}
}
