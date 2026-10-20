package export

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"time"
)

const tenantA = "11111111-1111-1111-1111-111111111111"
const tenantB = "22222222-2222-2222-2222-222222222222"
const exportID = "exp-00000001"

type memStore struct {
	mu      sync.Mutex
	objects map[string][]byte
	puts    []string
	deleted []string
	putErr  error
}

func newMemStore() *memStore { return &memStore{objects: map[string][]byte{}} }

func (m *memStore) Put(ctx context.Context, key string, r io.Reader) (int64, error) {
	m.mu.Lock()
	m.puts = append(m.puts, key)
	m.mu.Unlock()
	// emulate a streaming store: object becomes visible partially, error leaves partial data.
	var buf []byte
	tmp := make([]byte, 7)
	for {
		n, err := r.Read(tmp)
		buf = append(buf, tmp[:n]...)
		m.mu.Lock()
		m.objects[key] = append([]byte(nil), buf...)
		m.mu.Unlock()
		if err == io.EOF {
			return int64(len(buf)), nil
		}
		if err != nil {
			return 0, err
		}
	}
}

func (m *memStore) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	m.deleted = append(m.deleted, key)
	return nil
}

type fakeCH struct {
	*httptest.Server
	mu         sync.Mutex // guards last* (concurrent dispatcher tests)
	lastQuery  string
	lastParams url.Values
	lastUser   string
}

// newFakeCH serves body; if breakAfter>=0 the connection is aborted after that many bytes.
func newFakeCH(body string, status int) *fakeCH {
	f := &fakeCH{}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.lastQuery = string(b)
		f.lastParams = r.URL.Query()
		f.lastUser = r.Header.Get("X-ClickHouse-User")
		f.mu.Unlock()
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body)
	}))
	return f
}

func ndjsonRows(n int) string {
	var b strings.Builder
	for i := 0; i < n; i++ {
		fmt.Fprintf(&b, "{\"event_id\":\"e%d\",\"payload\":\"x\"}\n", i)
	}
	return b.String()
}

func goodSpec() ExportSpec {
	pv := uint32(1)
	f := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return ExportSpec{Format: "ndjson", From: f, To: f.Add(24 * time.Hour), ProcessingVersion: &pv, MaxRows: 100}
}

func newRunner(ch *fakeCH, st ObjectStore, a AuditSink) *Runner {
	return &Runner{CH: ClickHouse{URL: ch.URL, User: "u", Password: "p"}, Store: st, Audit: a,
		Presign: &S3Store{Endpoint: "http://s3.local:9000", Bucket: "b", Creds: awsCreds}}
}

var errBoom = errors.New("boom")
