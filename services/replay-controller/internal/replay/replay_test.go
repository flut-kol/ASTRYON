package replay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	tenantA = "11111111-1111-1111-1111-111111111111"
	tenantB = "22222222-2222-2222-2222-222222222222"
	srcX    = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
)

type fixture struct {
	alloc *MemAllocator
	pub   *MemPublisher
	srv   http.Handler
}

func newFixture() fixture {
	f := fixture{alloc: NewMemAllocator(), pub: &MemPublisher{}}
	pl := &Planner{Alloc: f.alloc, Counter: FixedCounter{N: 42}, Publisher: f.pub}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenantA, Scopes: []string{ScopeReplayWrite}}}
	f.srv = (&Handler{Auth: auth, Planner: pl}).Routes()
	return f
}

func (f fixture) do(token, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body))
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	f.srv.ServeHTTP(w, r)
	return w
}

func body(extra string, dry bool) string {
	d := "false"
	if dry {
		d = "true"
	}
	return `{"source_id":"` + srcX + `","from":"2026-01-01T00:00:00Z","to":"2026-01-03T12:00:00Z","pipeline_version":3,"dry_run":` + d + extra + `}`
}

func TestAuth(t *testing.T) {
	f := newFixture()
	if c := f.do("", body("", true)).Code; c != 401 {
		t.Fatal(c)
	}
	if c := f.do("bad", body("", true)).Code; c != 401 {
		t.Fatal(c)
	}
	noScope := DevStaticAuthenticator{Token: "t", Principal: Principal{TenantID: tenantA, Scopes: []string{"astryon:query:read"}}}
	h := (&Handler{Auth: noScope, Planner: &Planner{}}).Routes()
	r := httptest.NewRequest("POST", "/v1/replays", strings.NewReader(body("", true)))
	r.Header.Set("Authorization", "Bearer t")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	if w.Code != 403 {
		t.Fatal(w.Code)
	}
}

func TestValidation(t *testing.T) {
	f := newFixture()
	bad := []string{
		`{"source_id":"nope","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","pipeline_version":1}`,
		`{"from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","pipeline_version":1}`,
		`{"source_id":"` + srcX + `","from":"2026-01-01T00:00:00Z","to":"2026-02-05T00:00:00Z","pipeline_version":1}`,
		`{"source_id":"` + srcX + `","from":"2026-01-02T00:00:00Z","to":"2026-01-01T00:00:00Z","pipeline_version":1}`,
		`{"source_id":"` + srcX + `","from":"2026-01-01T00:00:00Z","to":"2026-01-02T00:00:00Z","pipeline_version":0}`,
		`{"source_id":"` + srcX + `","pipeline_version":1}`,
		`not json`,
	}
	for _, b := range bad {
		if c := f.do("tok", b).Code; c != 400 {
			t.Errorf("%s -> %d", b, c)
		}
	}
	// exactly 31 days is allowed
	ok := `{"source_id":"` + srcX + `","from":"2026-01-01T00:00:00Z","to":"2026-02-01T00:00:00Z","pipeline_version":1,"dry_run":true}`
	if c := f.do("tok", ok).Code; c != 200 {
		t.Fatal(c)
	}
	if f.pub.Count() != 0 {
		t.Fatal("published")
	}
}

// Tenant can never come from the body: an explicit tenant_id is rejected, and the job always
// carries the principal's tenant.
func TestTenantOnlyFromPrincipal(t *testing.T) {
	f := newFixture()
	if c := f.do("tok", body(`,"tenant_id":"`+tenantB+`"`, false)).Code; c != 400 {
		t.Fatalf("body tenant accepted: %d", c)
	}
	f.alloc.Seed(tenantB, srcX, 7) // another tenant's history must not influence us
	w := f.do("tok", body("", false))
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if len(f.pub.Jobs) != 1 || f.pub.Jobs[0].TenantID != tenantA || f.pub.Jobs[0].ProcessingVersion != 1 {
		t.Fatalf("%+v", f.pub.Jobs)
	}
	if v, _ := f.alloc.Peek(context.Background(), tenantB, srcX); v != 7 {
		t.Fatal("other tenant touched")
	}
}

func TestDryRunNoSideEffects(t *testing.T) {
	f := newFixture()
	f.alloc.Seed(tenantA, srcX, 4)
	for i := 0; i < 3; i++ {
		w := f.do("tok", body("", true))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"processing_version":5`) ||
			!strings.Contains(w.Body.String(), `"estimated_events":42`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if v, _ := f.alloc.Peek(context.Background(), tenantA, srcX); v != 4 {
		t.Fatalf("dry run reserved a version: %d", v)
	}
	if f.pub.Count() != 0 {
		t.Fatal("dry run published")
	}
}

func TestNeverReusesExistingVersion(t *testing.T) {
	f := newFixture()
	f.alloc.Seed(tenantA, srcX, 9)
	for want := uint32(10); want < 13; want++ {
		if c := f.do("tok", body("", false)).Code; c != 202 {
			t.Fatal(c)
		}
		if got := f.pub.Jobs[len(f.pub.Jobs)-1].ProcessingVersion; got != want {
			t.Fatalf("got %d want %d", got, want)
		}
	}
}

func TestConcurrentAllocationsDistinct(t *testing.T) {
	a := NewMemAllocator()
	const n = 200
	var wg sync.WaitGroup
	res := make(chan uint32, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, _ := a.Allocate(context.Background(), tenantA, srcX)
			res <- v
		}()
	}
	wg.Wait()
	close(res)
	seen := map[uint32]bool{}
	for v := range res {
		if seen[v] {
			t.Fatalf("duplicate %d", v)
		}
		seen[v] = true
	}
}

func TestPublishFailureBurnsVersion(t *testing.T) {
	f := newFixture()
	f.pub.Err = context.DeadlineExceeded
	if c := f.do("tok", body("", false)).Code; c != 502 {
		t.Fatal(c)
	}
	f.pub.Err = nil
	f.do("tok", body("", false))
	if f.pub.Jobs[0].ProcessingVersion != 2 {
		t.Fatal("version reused after failed publish")
	}
}

func TestDayRanges(t *testing.T) {
	f := time.Date(2026, 1, 1, 6, 0, 0, 0, time.UTC)
	r := DayRanges(f, f.Add(48*time.Hour))
	if len(r) != 3 || !r[0].To.Equal(time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)) || !r[2].To.Equal(f.Add(48*time.Hour)) {
		t.Fatalf("%v", r)
	}
}
