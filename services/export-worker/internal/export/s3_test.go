package export

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeS3 is a minimal path-style S3 (PutObject, multipart, DELETE) that also re-verifies the SigV4
// signature of every request using the independently tested signer.
type fakeS3 struct {
	*httptest.Server
	mu        sync.Mutex
	objects   map[string][]byte
	uploads   map[string][][]byte
	aborted   int
	completed int
	badSig    int
	failPart  int // fail this part number with 500 (0 = never)
}

func newFakeS3(t *testing.T) *fakeS3 {
	f := &fakeS3{objects: map[string][]byte{}, uploads: map[string][][]byte{}}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		defer f.mu.Unlock()
		// verify signature
		auth := r.Header.Get("Authorization")
		hdr := map[string]string{"host": r.Host, "x-amz-date": r.Header.Get("X-Amz-Date"), "x-amz-content-sha256": r.Header.Get("X-Amz-Content-Sha256")}
		ts, _ := time.Parse("20060102T150405Z", hdr["x-amz-date"])
		want := authorizationHeader(awsCreds, r.Method, r.URL.Path, r.URL.Query(), hdr, hdr["x-amz-content-sha256"], ts, "us-east-1", "s3")
		if auth != want || hdr["x-amz-content-sha256"] != sha256Hex(body) {
			f.badSig++
			http.Error(w, "SignatureDoesNotMatch", 403)
			return
		}
		key := r.URL.Path
		q := r.URL.Query()
		switch {
		case r.Method == "POST" && q.Has("uploads"):
			f.uploads[key+"#u1"] = nil
			_, _ = io.WriteString(w, `<InitiateMultipartUploadResult><UploadId>u1</UploadId></InitiateMultipartUploadResult>`)
		case r.Method == "PUT" && q.Has("partNumber"):
			if f.failPart != 0 && q.Get("partNumber") == itoa(int64(f.failPart)) {
				http.Error(w, "boom", 500)
				return
			}
			f.uploads[key+"#u1"] = append(f.uploads[key+"#u1"], body)
			w.Header().Set("ETag", `"etag`+q.Get("partNumber")+`"`)
		case r.Method == "POST" && q.Has("uploadId"):
			f.objects[key] = bytes.Join(f.uploads[key+"#u1"], nil)
			delete(f.uploads, key+"#u1")
			f.completed++
			_, _ = io.WriteString(w, `<CompleteMultipartUploadResult/>`)
		case r.Method == "DELETE" && q.Has("uploadId"):
			delete(f.uploads, key+"#u1")
			f.aborted++
			w.WriteHeader(204)
		case r.Method == "PUT":
			f.objects[key] = body
		case r.Method == "DELETE":
			delete(f.objects, key)
			w.WriteHeader(204)
		case r.Method == "GET":
			b, ok := f.objects[key]
			if !ok {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(b)
		}
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeS3) store() *S3Store {
	return &S3Store{Endpoint: f.URL, Bucket: "bkt", Creds: awsCreds, PartSize: 10}
}

func TestS3SinglePutAndSignature(t *testing.T) {
	f := newFakeS3(t)
	s := f.store()
	n, err := s.Put(context.Background(), "exports/t/a b.csv", strings.NewReader("short"))
	if err != nil || n != 5 || f.badSig != 0 || string(f.objects["/bkt/exports/t/a b.csv"]) != "short" {
		t.Fatalf("%v %d sig=%d %v", err, n, f.badSig, f.objects)
	}
	if err := s.Delete(context.Background(), "exports/t/a b.csv"); err != nil || len(f.objects) != 0 {
		t.Fatal(err)
	}
	if err := s.Delete(context.Background(), "missing"); err != nil {
		t.Fatal(err)
	}
}

func TestS3MultipartExactAndShortLastPart(t *testing.T) {
	for _, size := range []int{20, 25, 10, 11, 0} { // PartSize=10
		f := newFakeS3(t)
		data := strings.Repeat("x", size)
		n, err := f.store().Put(context.Background(), "k", strings.NewReader(data))
		if err != nil || n != int64(size) || f.badSig != 0 {
			t.Fatalf("size %d: %v n=%d sig=%d", size, err, n, f.badSig)
		}
		if string(f.objects["/bkt/k"]) != data {
			t.Fatalf("size %d: content mismatch", size)
		}
		if size > 10 && f.completed != 1 {
			t.Fatalf("size %d: expected multipart", size)
		}
	}
}

func TestS3MultipartAbortOnReaderErrorAndPartFailure(t *testing.T) {
	f := newFakeS3(t)
	r := io.MultiReader(strings.NewReader(strings.Repeat("y", 25)), &errReader{errBoom})
	if _, err := f.store().Put(context.Background(), "k", r); !errors.Is(err, errBoom) {
		t.Fatal(err)
	}
	if f.aborted != 1 || len(f.objects) != 0 || len(f.uploads) != 0 {
		t.Fatalf("aborted=%d %v %v", f.aborted, f.objects, f.uploads)
	}
	f = newFakeS3(t)
	f.failPart = 2
	if _, err := f.store().Put(context.Background(), "k", strings.NewReader(strings.Repeat("z", 30))); err == nil || f.aborted != 1 {
		t.Fatalf("err=%v aborted=%d", err, f.aborted)
	}
}

type errReader struct{ err error }

func (e *errReader) Read([]byte) (int, error) { return 0, e.err }

func TestRunnerWithS3Store(t *testing.T) {
	f := newFakeS3(t)
	ch := newFakeCH(ndjsonRows(200), 200)
	defer ch.Close()
	st := f.store()
	r := &Runner{CH: ClickHouse{URL: ch.URL}, Store: st, Presign: st, Audit: &MemoryAudit{}}
	s := goodSpec()
	s.MaxRows = 150 // overflow -> multipart aborted, nothing left
	if _, err := r.Run(context.Background(), Principal{TenantID: tenantA}, exportID, s); !errors.Is(err, ErrRowCap) {
		t.Fatal(err)
	}
	if len(f.objects) != 0 || len(f.uploads) != 0 || f.aborted != 1 {
		t.Fatalf("leftovers %v %v aborted=%d", f.objects, f.uploads, f.aborted)
	}
	s.MaxRows = 200
	res, err := r.Run(context.Background(), Principal{TenantID: tenantA}, exportID, s)
	if err != nil || res.Rows != 200 || f.badSig != 0 {
		t.Fatalf("%v %+v sig=%d", err, res, f.badSig)
	}
	if !strings.Contains(res.DownloadURL, "/bkt/exports/"+tenantA+"/") {
		t.Fatal(res.DownloadURL)
	}
}
