package editsave

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/objects"
	"github.com/astryon/file-ingestion/internal/transform"
)

const (
	tenant = "t1"
	fileID = "f1"
)

type fakeFiles struct{ f objects.File }

func (f fakeFiles) Register(context.Context, string, string, objects.File) (string, error) {
	return "", nil
}
func (f fakeFiles) Get(_ context.Context, _, _ string) (objects.File, error)   { return f.f, nil }
func (f fakeFiles) MarkCommitted(context.Context, string, string, int64) error { return nil }
func (f fakeFiles) MarkRejected(context.Context, string, string) error         { return nil }

type fakeObjs struct{ body string }

func (o fakeObjs) Head(context.Context, string) (int64, error) { return int64(len(o.body)), nil }
func (o fakeObjs) Get(context.Context, string, int64) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader(o.body)), nil
}
func (o fakeObjs) Delete(context.Context, string) error { return nil }

type fakeWriter struct {
	puts []string // keys written
}

func (w *fakeWriter) Put(_ context.Context, key string, _ []byte, _ string) error {
	w.puts = append(w.puts, key)
	return nil
}
func (w *fakeWriter) PresignGet(key string, _ time.Duration) (string, error) {
	return "https://example/" + key, nil
}

type fakeVersions struct {
	rows []Version
}

func (v *fakeVersions) NextVersionNumber(context.Context, string, string) (int, error) {
	return len(v.rows) + 1, nil
}
func (v *fakeVersions) Insert(_ context.Context, _, fileObjectID string, nv NewVersion) (Version, error) {
	out := Version{ID: "v" + time.Now().String(), FileObjectID: fileObjectID, VersionNumber: nv.VersionNumber,
		Ops: nv.Ops, InputHeaderRow: nv.InputHeaderRow, OutputHeaderRow: nv.OutputHeaderRow,
		OutputDelimiter: nv.OutputDelimiter, Header: nv.Header, Bucket: nv.Bucket, ObjectKey: nv.ObjectKey,
		ChecksumSHA256: nv.ChecksumSHA256, SizeBytes: nv.SizeBytes, RowCount: nv.RowCount, CreatedAt: time.Now()}
	v.rows = append(v.rows, out)
	return out, nil
}
func (v *fakeVersions) List(context.Context, string, string) ([]Version, error) { return v.rows, nil }
func (v *fakeVersions) Get(_ context.Context, _, _, versionID string) (Version, error) {
	for _, r := range v.rows {
		if r.ID == versionID {
			return r, nil
		}
	}
	return Version{}, ErrNotFound
}

func committedCSV() objects.File {
	return objects.File{ID: fileID, Bucket: "b", Key: "k", ContentType: "text/csv", Committed: true}
}

func TestSaveHappyPath(t *testing.T) {
	versions := &fakeVersions{}
	writer := &fakeWriter{}
	svc := Service{
		Files: fakeFiles{f: committedCSV()}, Objs: fakeObjs{body: "name,age\nJane,30\nJohn,40\n"},
		Writer: writer, Versions: versions,
	}
	v, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{InputHeaderRow: true, OutputHeaderRow: true}, "", "")
	if err != nil {
		t.Fatalf("Save: %v", err)
	}
	if v.VersionNumber != 1 {
		t.Fatalf("version = %d, want 1", v.VersionNumber)
	}
	if v.RowCount != 2 {
		t.Fatalf("row_count = %d, want 2", v.RowCount)
	}
	if !strings.Contains(writer.puts[0], "edits/"+fileID+"/v1/") {
		t.Fatalf("unexpected key %q", writer.puts[0])
	}
}

func TestSaveRejectsUnsupportedContentType(t *testing.T) {
	svc := Service{Files: fakeFiles{f: objects.File{Committed: true, ContentType: "application/x-ndjson"}},
		Objs: fakeObjs{}, Writer: &fakeWriter{}, Versions: &fakeVersions{}}
	_, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{}, "", "")
	if !errors.Is(err, ErrUnsupportedContentType) {
		t.Fatalf("got %v, want ErrUnsupportedContentType", err)
	}
}

func TestSaveRejectsNotCommitted(t *testing.T) {
	svc := Service{Files: fakeFiles{f: objects.File{Committed: false, ContentType: "text/csv"}},
		Objs: fakeObjs{}, Writer: &fakeWriter{}, Versions: &fakeVersions{}}
	_, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{}, "", "")
	if !errors.Is(err, ErrNotCommitted) {
		t.Fatalf("got %v, want ErrNotCommitted", err)
	}
}

func TestSaveVersionNumberingAcrossTwoSaves(t *testing.T) {
	versions := &fakeVersions{}
	svc := Service{Files: fakeFiles{f: committedCSV()}, Objs: fakeObjs{body: "a\n1\n"},
		Writer: &fakeWriter{}, Versions: versions}
	v1, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{InputHeaderRow: true, OutputHeaderRow: true}, "", "")
	if err != nil {
		t.Fatalf("first save: %v", err)
	}
	v2, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{InputHeaderRow: true, OutputHeaderRow: true}, "", "")
	if err != nil {
		t.Fatalf("second save: %v", err)
	}
	if v1.VersionNumber != 1 || v2.VersionNumber != 2 {
		t.Fatalf("got versions %d, %d; want 1, 2", v1.VersionNumber, v2.VersionNumber)
	}
}

func TestSaveSizeCapExceeded(t *testing.T) {
	svc := Service{Files: fakeFiles{f: committedCSV()}, Objs: fakeObjs{body: "a\n" + strings.Repeat("x", 1000) + "\n"},
		Writer: &fakeWriter{}, Versions: &fakeVersions{}, MaxBytes: 10}
	_, err := svc.Save(context.Background(), tenant, fileID, nil, FormatOptions{InputHeaderRow: true, OutputHeaderRow: true}, "", "")
	if !errors.Is(err, ErrEditTooLarge) {
		t.Fatalf("got %v, want ErrEditTooLarge", err)
	}
}

func TestPreviewAppliesOpsAndTruncates(t *testing.T) {
	svc := Service{Files: fakeFiles{f: committedCSV()}, Objs: fakeObjs{body: "a,b\n1,2\n3,4\n5,6\n"},
		Writer: &fakeWriter{}, Versions: &fakeVersions{}}
	res, err := svc.Preview(context.Background(), tenant, fileID, []transform.OpSpec{
		{Type: transform.RenameColumn, Column: "a", NewName: "x"},
	}, FormatOptions{InputHeaderRow: true, OutputHeaderRow: true}, 2)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Header[0] != "x" {
		t.Fatalf("header = %v", res.Header)
	}
	if len(res.Rows) != 2 || !res.Truncated {
		t.Fatalf("rows=%v truncated=%v", res.Rows, res.Truncated)
	}
}
