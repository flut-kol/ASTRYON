package objects

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"testing"

	"github.com/astryon/file-ingestion/internal/jobs"
)

type memObjs struct {
	data    map[string][]byte
	deleted []string
	offsets []int64
}

func (m *memObjs) Head(_ context.Context, k string) (int64, error) { return int64(len(m.data[k])), nil }
func (m *memObjs) Get(_ context.Context, k string, off int64) (io.ReadCloser, error) {
	m.offsets = append(m.offsets, off)
	return io.NopCloser(bytes.NewReader(m.data[k][off:])), nil
}
func (m *memObjs) Delete(_ context.Context, k string) error {
	m.deleted = append(m.deleted, k)
	delete(m.data, k)
	return nil
}

type memStore struct {
	f                  File
	committed, rejects int
}

func (s *memStore) Register(context.Context, string, string, File) (string, error) {
	return s.f.ID, nil
}
func (s *memStore) Get(_ context.Context, tenant, id string) (File, error) {
	if tenant != "t1" || id != s.f.ID {
		return File{}, ErrNotFound
	}
	return s.f, nil
}
func (s *memStore) MarkCommitted(context.Context, string, string, int64) error {
	s.committed++
	s.f.Committed = true
	return nil
}
func (s *memStore) MarkRejected(context.Context, string, string) error { s.rejects++; return nil }

func sum(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }

func TestVerifyCommitsMatchingObject(t *testing.T) {
	data := []byte("{\"a\":1}\n")
	o := &memObjs{data: map[string][]byte{"k": data}}
	st := &memStore{f: File{ID: "f1", Key: "k", SHA256: sum(data), Size: int64(len(data))}}
	if err := Verify(context.Background(), st, o, "t1", "f1"); err != nil || st.committed != 1 || len(o.deleted) != 0 {
		t.Fatalf("err=%v %+v %+v", err, st, o)
	}
	if err := Verify(context.Background(), st, o, "t1", "f1"); err != nil || st.committed != 1 { // idempotent
		t.Fatalf("second verify: %v committed=%d", err, st.committed)
	}
}

func TestVerifyRejectsAndDeletesMismatch(t *testing.T) {
	for name, f := range map[string]File{
		"wrong sha":  {ID: "f1", Key: "k", SHA256: sum([]byte("other")), Size: -1},
		"wrong size": {ID: "f1", Key: "k", SHA256: sum([]byte("data")), Size: 99},
	} {
		o := &memObjs{data: map[string][]byte{"k": []byte("data")}}
		st := &memStore{f: f}
		if err := Verify(context.Background(), st, o, "t1", "f1"); !errors.Is(err, ErrChecksumMismatch) || st.committed != 0 || st.rejects != 1 || len(o.deleted) != 1 {
			t.Errorf("%s: err=%v %+v %+v", name, err, st, o)
		}
	}
}

func TestVerifyIsTenantScoped(t *testing.T) {
	st := &memStore{f: File{ID: "f1", Key: "k"}}
	if err := Verify(context.Background(), st, &memObjs{}, "other-tenant", "f1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("err=%v", err)
	}
}

func TestSourceOnlyServesVerifiedObjectsAndRangeReads(t *testing.T) {
	data := []byte("0123456789")
	o := &memObjs{data: map[string][]byte{"k": data}}
	st := &memStore{f: File{ID: "f1", Bucket: "b", Key: "k", SHA256: sum(data)}}
	src := Source{Store: st, Objs: o, Bucket: "b"}
	job := jobs.Job{TenantID: "t1", FileObjectID: "f1"}
	if _, _, err := src.Open(context.Background(), job, 0); !errors.Is(err, ErrNotVerified) {
		t.Fatalf("uncommitted object served: %v", err)
	}
	st.f.Committed = true
	rc, fp, err := src.Open(context.Background(), job, 4)
	if err != nil || fp != sum(data) {
		t.Fatalf("%v %s", err, fp)
	}
	got, _ := io.ReadAll(rc)
	if string(got) != "456789" || o.offsets[0] != 4 {
		t.Fatalf("got %q offsets %v", got, o.offsets)
	}
	if _, _, err := (Source{Store: st, Objs: o, Bucket: "other"}).Open(context.Background(), job, 0); err == nil {
		t.Fatal("row in a foreign bucket must be refused")
	}
	job.TenantID = "t2"
	if _, _, err := src.Open(context.Background(), job, 0); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-tenant open: %v", err)
	}
}
