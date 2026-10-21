package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/joblist"
)

type jobReader struct {
	tenant string
	f      joblist.Filter
	page   joblist.Page
	job    joblist.Job
	err    error
}

func (j *jobReader) List(_ context.Context, t string, f joblist.Filter) (joblist.Page, error) {
	j.tenant, j.f = t, f
	return j.page, j.err
}
func (j *jobReader) Get(_ context.Context, t, _ string) (joblist.Job, error) {
	j.tenant = t
	return j.job, j.err
}

func get(s *Server, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

func TestJobsListShapeTenantAndScope(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeJobsRead}})
	jr := &jobReader{page: joblist.Page{NextCursor: "nc", Jobs: []joblist.Job{
		{ID: fileID, Kind: "process_file", Status: "QUARANTINED", SourceID: fileID, Error: "CREDENTIALS_DETECTED",
			Labels: map[string]int64{"contains_credentials": 2}, CreatedAt: time.Unix(5, 0)}, {ID: "x"}}}}
	s.JobReader = jr
	w := get(s, "/v1/jobs?status=QUARANTINED&source_id="+fileID+"&limit=10")
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if jr.tenant != tenant || jr.f.Status != "QUARANTINED" || jr.f.SourceID != fileID || jr.f.Limit != 10 {
		t.Fatalf("%+v", jr)
	}
	var got struct {
		Items []map[string]any `json:"items"`
		Next  string           `json:"next_cursor"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got.Items) != 2 || got.Next != "nc" {
		t.Fatalf("%v %s", err, w.Body)
	}
	if got.Items[1]["source_id"] != nil || got.Items[1]["labels"] == nil {
		t.Fatalf("null source and empty labels expected: %v", got.Items[1])
	}
	if strings.Contains(w.Body.String(), "payload") {
		t.Fatal("payload leaked")
	}
}

func TestJobsRequireOwnScopeAndRejectBadInput(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	s.JobReader = &jobReader{}
	if w := get(s, "/v1/jobs"); w.Code != 403 {
		t.Fatalf("uploads:write must not read jobs: %d", w.Code)
	}
	if w := get(s, "/v1/jobs/"+fileID); w.Code != 403 {
		t.Fatalf("%d", w.Code)
	}
	s, _, _, _ = newServer(Principal{TenantID: tenant, Scopes: []string{ScopeJobsRead}})
	s.JobReader = &jobReader{}
	if w := get(s, "/v1/files/uploads"); w.Code != 403 {
		t.Fatalf("jobs:read must not list uploads: %d", w.Code)
	}
	for _, q := range []string{"limit=0", "limit=201", "status=BOGUS", "source_id=x", "tenant_id=" + tenant, "limit=1&limit=2", "cursor="} {
		if w := get(s, "/v1/jobs?"+q); w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
	s.JobReader = &jobReader{err: joblist.ErrBadCursor}
	if w := get(s, "/v1/jobs?cursor=zz"); w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestJobGetNotFoundAndTenant(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeJobsRead}})
	jr := &jobReader{job: joblist.Job{ID: fileID, Kind: "process_file", Status: "RUNNING"}}
	s.JobReader = jr
	if w := get(s, "/v1/jobs/"+fileID); w.Code != 200 || jr.tenant != tenant {
		t.Fatalf("%d", w.Code)
	}
	if w := get(s, "/v1/jobs/not-a-uuid"); w.Code != 404 {
		t.Fatal(w.Code)
	}
	jr.err = joblist.ErrNotFound
	if w := get(s, "/v1/jobs/"+fileID); w.Code != 404 {
		t.Fatal(w.Code)
	}
	s.JobReader = nil
	if w := get(s, "/v1/jobs"); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
