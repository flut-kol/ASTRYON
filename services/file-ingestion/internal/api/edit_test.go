package api

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/editsave"
	"github.com/astryon/file-ingestion/internal/objects"
)

type editWriter struct{ puts []string }

func (w *editWriter) Put(_ context.Context, key string, _ []byte, _ string) error {
	w.puts = append(w.puts, key)
	return nil
}
func (w *editWriter) PresignGet(key string, _ time.Duration) (string, error) {
	return "https://example/" + key, nil
}

type editVersions struct{ rows []editsave.Version }

func (v *editVersions) NextVersionNumber(context.Context, string, string) (int, error) {
	return len(v.rows) + 1, nil
}

var versionIDs = []string{"11111111-1111-4111-8111-111111111111", "22222222-2222-4222-8222-222222222222"}

func (v *editVersions) Insert(_ context.Context, _, fileObjectID string, nv editsave.NewVersion) (editsave.Version, error) {
	out := editsave.Version{ID: versionIDs[len(v.rows)], FileObjectID: fileObjectID, VersionNumber: nv.VersionNumber,
		Header: nv.Header, OutputDelimiter: nv.OutputDelimiter, OutputHeaderRow: nv.OutputHeaderRow,
		Bucket: nv.Bucket, ObjectKey: nv.ObjectKey, SizeBytes: nv.SizeBytes, RowCount: nv.RowCount, CreatedAt: time.Now()}
	v.rows = append(v.rows, out)
	return out, nil
}
func (v *editVersions) List(_ context.Context, _, fileObjectID string) ([]editsave.Version, error) {
	return v.rows, nil
}
func (v *editVersions) Get(_ context.Context, _, _, versionID string) (editsave.Version, error) {
	for _, r := range v.rows {
		if r.ID == versionID {
			return r, nil
		}
	}
	return editsave.Version{}, editsave.ErrNotFound
}

func csvFile(id string) *files {
	return &files{f: objects.File{ID: id, SourceID: source, Bucket: "b", Key: "k", ContentType: "text/csv", Committed: true}}
}

func editServer(p Principal, body string) (*Server, *editVersions, *editWriter) {
	fs, fv, fw := csvFile(fileID), &editVersions{}, &editWriter{}
	s := &Server{Auth: auth{p}, Edits: editsave.Service{Files: fs, Objs: objs{body: body}, Writer: fw, Versions: fv, Now: time.Now}}
	return s, fv, fw
}

func getReq(s *Server, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
	return w
}

const csvBody = "name,age\nJane,30\nJohn,40\n"

func TestPreviewHappyPath(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}, csvBody)
	w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/preview",
		`{"ops":[{"type":"rename_column","column":"name","new_name":"full_name"}]}`)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var got previewResp
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Header[0] != "full_name" || len(got.Rows) != 2 {
		t.Fatalf("%+v", got)
	}
}

func TestPreviewRejectsInvalidOps(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}, csvBody)
	w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/preview",
		`{"ops":[{"type":"rename_column","column":"name","new_name":"age"}]}`)
	if w.Code != 422 || !strings.Contains(w.Body.String(), "OPS_INVALID") {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestPreviewRequiresScope(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: tenant, Scopes: []string{"astryon:events:write"}}, csvBody)
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/preview", `{"ops":[]}`); w.Code != 403 {
		t.Fatalf("%d", w.Code)
	}
}

func TestCreateVersionHappyPathIncludesDownloadURL(t *testing.T) {
	s, fv, fw := editServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}, csvBody)
	w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/versions", `{"ops":[]}`)
	if w.Code != 201 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var got versionResp
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.VersionNumber != 1 || got.DownloadURL == "" || len(fv.rows) != 1 || len(fw.puts) != 1 {
		t.Fatalf("%+v rows=%d puts=%d", got, len(fv.rows), len(fw.puts))
	}
}

func TestCreateVersionTenantIsolation(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: "0a1b2c3d-1111-4222-8333-999999999999", Scopes: []string{ScopeUploadsWrite}}, csvBody)
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/versions", `{"ops":[]}`); w.Code != 404 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestCreateVersionRejectsBadFilename(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}, csvBody)
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/versions", `{"ops":[],"filename":"../x.csv"}`); w.Code != 400 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestListAndGetVersions(t *testing.T) {
	s, _, _ := editServer(Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}, csvBody)
	call(s.Handler(), "/v1/files/uploads/"+fileID+"/versions", `{"ops":[]}`)
	call(s.Handler(), "/v1/files/uploads/"+fileID+"/versions", `{"ops":[]}`)

	w := getReq(s, "/v1/files/uploads/"+fileID+"/versions")
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var list versionListResp
	if err := json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if len(list.Items) != 2 || list.Items[0].DownloadURL != "" {
		t.Fatalf("%+v", list)
	}

	w = getReq(s, "/v1/files/uploads/"+fileID+"/versions/"+list.Items[0].VersionID)
	if w.Code != 200 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
	var got versionResp
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.DownloadURL == "" {
		t.Fatalf("%+v", got)
	}

	if w := getReq(s, "/v1/files/uploads/"+fileID+"/versions/00000000-0000-4000-8000-000000000000"); w.Code != 404 {
		t.Fatalf("%d", w.Code)
	}
}

func TestEditsUnavailableReturns503(t *testing.T) {
	s := &Server{Auth: auth{Principal{TenantID: tenant, Scopes: []string{ScopeUploadsWrite}}}}
	if w := call(s.Handler(), "/v1/files/uploads/"+fileID+"/preview", `{"ops":[]}`); w.Code != 503 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}
