package adminapi

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/astryon/connector-manager/internal/registry"
)

type fakeSources struct {
	tenant string
	after  string
	items  []Source
	err    error
}

func (f *fakeSources) ListSources(_ context.Context, t, after string, limit int) ([]Source, error) {
	f.tenant, f.after = t, after
	if len(f.items) > limit {
		return f.items[:limit], f.err
	}
	return f.items, f.err
}
func (f *fakeSources) GetSource(_ context.Context, t, id string) (Source, error) {
	f.tenant = t
	return Source{ID: id, Name: "s"}, f.err
}

func srvWith(scopes map[string]bool, fs *fakeSources) *Server {
	return &Server{Auth: fakeAuth{p: registry.Principal{TenantID: "tenant-A", Scopes: scopes}}, Store: &fakeStore{}, Sources: fs}
}

func TestSourcesScopeAndTenant(t *testing.T) {
	fs := &fakeSources{items: []Source{{ID: id1, Name: "a"}, {ID: src, Name: "b"}}}
	// connectors:write alone must not read sources, and vice versa.
	if w := do(srvWith(map[string]bool{registry.ManageScope: true}, fs), "GET", "/v1/sources", ""); w.Code != 403 {
		t.Fatalf("%d", w.Code)
	}
	s := srvWith(map[string]bool{SourcesReadScope: true}, fs)
	if w := do(s, "GET", "/v1/connectors", ""); w.Code != 403 {
		t.Fatalf("%d", w.Code)
	}
	w := do(s, "GET", "/v1/sources?limit=1&tenant_id=x", "")
	if w.Code != 400 {
		t.Fatalf("unknown param: %d", w.Code)
	}
	w = do(s, "GET", "/v1/sources?limit=1", "")
	var got struct {
		Items []Source `json:"items"`
		Next  string   `json:"next_cursor"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &got) != nil || len(got.Items) != 1 || got.Next != id1 || fs.tenant != "tenant-A" {
		t.Fatalf("%d %s %+v", w.Code, w.Body, fs)
	}
	if strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "config") {
		t.Fatal("must not expose config or secrets")
	}
	w = do(s, "GET", "/v1/sources?cursor=bad", "")
	if w.Code != 400 {
		t.Fatal(w.Code)
	}
}

func TestSourceGet(t *testing.T) {
	fs := &fakeSources{}
	s := srvWith(map[string]bool{SourcesReadScope: true}, fs)
	if w := do(s, "GET", "/v1/sources/"+src, ""); w.Code != 200 || fs.tenant != "tenant-A" {
		t.Fatalf("%d", w.Code)
	}
	if w := do(s, "GET", "/v1/sources/zzz", ""); w.Code != 400 {
		t.Fatal(w.Code)
	}
	fs.err = ErrNotFound
	if w := do(s, "GET", "/v1/sources/"+src, ""); w.Code != 404 {
		t.Fatal(w.Code)
	}
	s.Sources = nil
	if w := do(s, "GET", "/v1/sources/"+src, ""); w.Code != 503 {
		t.Fatal(w.Code)
	}
}
