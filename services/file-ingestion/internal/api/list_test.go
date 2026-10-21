package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/classify"
	"github.com/astryon/file-ingestion/internal/listing"
)

type lister struct {
	tenant string
	f      listing.Filter
	page   listing.Page
	err    error
}

func (l *lister) List(_ context.Context, tenant string, f listing.Filter) (listing.Page, error) {
	l.tenant, l.f = tenant, f
	return l.page, l.err
}

func TestListFiltersTenantAndNoValues(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	l := &lister{page: listing.Page{NextCursor: "nc", Items: []listing.Item{{FileObjectID: fileID, SourceType: "geo", Sensitivity: "restricted",
		CreatedAt: time.Unix(5, 0), JobID: "j", Status: "QUARANTINED", Error: "CREDENTIALS_DETECTED",
		Labels: classify.Labels{classify.Credentials: {Count: 2, Fields: []string{"password"}}}}, {FileObjectID: "x"}}}}
	s.Lister = l
	qs := url.Values{"source_type": {"geo"}, "sensitivity": {"restricted"}, "status": {"QUARANTINED"}, "label": {"contains_credentials"},
		"tag": {"team:ops", "env", "url:a:b"}, "created_from": {"2026-01-01T00:00:00Z"}, "created_to": {"2026-02-01T00:00:00Z"}, "limit": {"10"}, "tenant_id": nil}
	qs.Del("tenant_id")
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/files/uploads?"+qs.Encode(), nil))
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	if l.tenant != tenant || l.f.Status != "QUARANTINED" || l.f.Label != classify.Credentials || l.f.Limit != 10 || len(l.f.Tags) != 3 ||
		l.f.Tags[1].HasValue || l.f.Tags[2].Value != "a:b" || l.f.From.IsZero() {
		t.Fatalf("%+v", l)
	}
	var got listResp
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil || len(got.Items) != 2 || got.NextCursor != "nc" ||
		got.Items[0].Job.Labels[classify.Credentials].Count != 2 || got.Items[1].Job != nil || got.Items[1].Tags == nil {
		t.Fatalf("%v %s", err, w.Body)
	}
}

func TestListRejectsBadInputAndAuth(t *testing.T) {
	s, _, _, _ := newServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}})
	s.Lister = &lister{}
	many := "tag=a&tag=b&tag=c&tag=d&tag=e&tag=f"
	for _, q := range []string{"limit=0", "limit=201", "limit=x", "status=BOGUS", "label=contains_x", "sensitivity=secret", "source_type=../x",
		"tag=Bad", "tag=k:%3Cscript%3E", many, "created_from=yesterday", "created_from=2026-02-01T00:00:00Z&created_to=2026-01-01T00:00:00Z",
		"tenant_id=" + tenant, "limit=1&limit=2", "cursor="} {
		w := httptest.NewRecorder()
		s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/files/uploads?"+q, nil))
		if w.Code != 400 {
			t.Errorf("%s: %d", q, w.Code)
		}
	}
	s.Lister = &lister{err: listing.ErrBadCursor}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/files/uploads?cursor=zz", nil))
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
	s.Lister = nil
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/files/uploads", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	anon, _, _, _ := newServer(Principal{})
	w = httptest.NewRecorder()
	anon.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/v1/files/uploads", nil))
	if w.Code != 401 || !strings.Contains(w.Body.String(), "UNAUTHENTICATED") {
		t.Fatal(w.Code)
	}
}
