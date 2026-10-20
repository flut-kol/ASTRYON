package query

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	srcA = "11111111-1111-1111-1111-111111111111"
	srcB = "22222222-2222-2222-2222-222222222222"
)

// mapResolver is a fake that keys on tenant+source, like RLS does.
type mapResolver struct {
	mu    sync.Mutex
	v     map[[2]string]uint32
	calls int32
	err   error
}

func (m *mapResolver) Resolve(_ context.Context, tn, src string) (uint32, error) {
	atomic.AddInt32(&m.calls, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.err != nil {
		return 0, m.err
	}
	if v, ok := m.v[[2]string{tn, src}]; ok {
		return v, nil
	}
	return 0, ErrNoActiveVersion
}

const rng = `"from":"2026-10-20T00:00:00Z","to":"2026-10-21T00:00:00Z"`

// setupSrv is like setup but with a VersionResolver on the query server.
func setupSrv(t *testing.T, r VersionResolver) (*httptest.Server, *fake) {
	t.Helper()
	f := &fake{}
	ch := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, rq *http.Request) {
		b, _ := io.ReadAll(rq.Body)
		f.query, f.params = string(b), rq.URL.Query()
	}))
	t.Cleanup(ch.Close)
	s := &Server{CHURL: ch.URL, Auth: DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tenant, Scopes: []string{ScopeQueryRead}}}}
	if r != nil {
		s.Versions = r
	}
	api := httptest.NewServer(s.Handler())
	t.Cleanup(api.Close)
	return api, f
}

func TestResolveVersionRules(t *testing.T) {
	res := &mapResolver{v: map[[2]string]uint32{{tenant, srcA}: 3, {tenantB, srcB}: 9}}
	s := &Server{Versions: res}
	p := Principal{TenantID: tenant}
	pv := func(v uint32) *uint32 { return &v }
	cases := []struct {
		name string
		f    Filter
		code string
		want uint32
	}{
		{"explicit wins", Filter{ProcessingVersion: pv(7), SourceID: srcA}, "", 7},
		{"explicit wins without source and without lookup", Filter{ProcessingVersion: pv(2)}, "", 2},
		{"active of source", Filter{SourceID: srcA}, "", 3},
		{"no source", Filter{}, "invalid_request", 0},
		{"bad source", Filter{SourceID: "x"}, "invalid_request", 0},
		{"no row -> no_active_version", Filter{SourceID: "33333333-3333-3333-3333-333333333333"}, "no_active_version", 0},
		{"other tenant's source never resolves", Filter{SourceID: srcB}, "no_active_version", 0},
	}
	for _, c := range cases {
		f := c.f
		f.From, f.To = time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), time.Date(2026, 10, 21, 0, 0, 0, 0, time.UTC)
		err := s.resolveVersion(context.Background(), p, &f)
		if c.code != "" {
			var ae *apiError
			if !errors.As(err, &ae) || ae.Code != c.code || ae.status != 400 {
				t.Fatalf("%s: %v", c.name, err)
			}
			continue
		}
		if err != nil || f.ProcessingVersion == nil || *f.ProcessingVersion != c.want {
			t.Fatalf("%s: %v %v", c.name, err, f.ProcessingVersion)
		}
	}
	// explicit version never touches the resolver
	before := atomic.LoadInt32(&res.calls)
	f := Filter{ProcessingVersion: pv(1), SourceID: srcA}
	_ = s.resolveVersion(context.Background(), p, &f)
	if atomic.LoadInt32(&res.calls) != before {
		t.Fatal("resolver consulted despite explicit version")
	}
}

func TestNoResolverKeepsVersionRequired(t *testing.T) {
	s := &Server{}
	f := Filter{SourceID: srcA, From: time.Now().Add(-time.Hour), To: time.Now()}
	var ae *apiError
	if err := s.resolveVersion(context.Background(), Principal{TenantID: tenant}, &f); !errors.As(err, &ae) || ae.Code != "invalid_request" {
		t.Fatalf("%v", err)
	}
}

func TestBackendErrorIsNotNoActiveVersion(t *testing.T) {
	s := &Server{Versions: &mapResolver{err: errors.New("pg down")}}
	f := Filter{SourceID: srcA, From: time.Now().Add(-time.Hour), To: time.Now()}
	err := s.resolveVersion(context.Background(), Principal{TenantID: tenant}, &f)
	var ae *apiError
	if err == nil || errors.As(err, &ae) {
		t.Fatalf("want plain backend error, got %v", err)
	}
}

func TestCachingResolver(t *testing.T) {
	inner := &mapResolver{v: map[[2]string]uint32{{tenant, srcA}: 1}}
	now := time.Unix(1000, 0)
	c := &CachingResolver{Inner: inner, TTL: 5 * time.Second, Now: func() time.Time { return now }}
	ctx := context.Background()
	get := func(tn, src string) (uint32, error) { return c.Resolve(ctx, tn, src) }
	if v, _ := get(tenant, srcA); v != 1 {
		t.Fatal(v)
	}
	inner.mu.Lock()
	inner.v[[2]string{tenant, srcA}] = 2 // cutover happens
	inner.mu.Unlock()
	if v, _ := get(tenant, srcA); v != 1 || inner.calls != 1 {
		t.Fatalf("expected cached 1, got %d calls=%d", v, inner.calls)
	}
	now = now.Add(5 * time.Second)
	if v, _ := get(tenant, srcA); v != 2 || inner.calls != 2 {
		t.Fatalf("expected refreshed 2, got %d calls=%d", v, inner.calls)
	}
	// keyed by tenant: another tenant asking for the same source is a separate (and negative) entry
	if _, err := get(tenantB, srcA); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("cross-tenant: %v", err)
	}
	if v, _ := get(tenant, srcA); v != 2 {
		t.Fatal("tenant A entry polluted")
	}
	// negative result cached, backend errors not
	n := inner.calls
	_, _ = get(tenantB, srcA)
	if inner.calls != n {
		t.Fatal("negative result not cached")
	}
	inner.mu.Lock()
	inner.err = errors.New("boom")
	inner.mu.Unlock()
	now = now.Add(time.Minute)
	if _, err := get(tenant, srcA); err == nil {
		t.Fatal("want error")
	}
	inner.mu.Lock()
	inner.err = nil
	inner.mu.Unlock()
	if v, err := get(tenant, srcA); err != nil || v != 2 {
		t.Fatalf("error was cached: %d %v", v, err)
	}
}

func TestCachingResolverBounded(t *testing.T) {
	inner := &mapResolver{v: map[[2]string]uint32{}}
	c := &CachingResolver{Inner: inner, MaxEntries: 3}
	for i := 0; i < 20; i++ {
		_, _ = c.Resolve(context.Background(), tenant, fmt.Sprintf("00000000-0000-0000-0000-%012d", i))
	}
	if len(c.m) > 3 {
		t.Fatalf("cache grew to %d", len(c.m))
	}
}

// HTTP level: omitted processing_version on all four endpoints.
func TestHTTPOmittedVersion(t *testing.T) {
	res := &mapResolver{v: map[[2]string]uint32{{tenant, srcA}: 4}}
	api, f := setupSrv(t, res)
	// events + aggregate (POST)
	for _, path := range []string{"/v1/query/events", "/v1/query/aggregate"} {
		extra := ""
		if strings.HasSuffix(path, "aggregate") {
			extra = `,"metrics":["count"]`
		}
		c, b := post(t, api, path, "tok", `{`+rng+`,"source_id":"`+srcA+`"`+extra+`}`)
		if c != 200 || f.params.Get("param_p_pv") != "4" {
			t.Fatalf("%s: %d %s pv=%s", path, c, b, f.params.Get("param_p_pv"))
		}
		// explicit wins
		if c, b := post(t, api, path, "tok", `{`+rng+`,"processing_version":9,"source_id":"`+srcA+`"`+extra+`}`); c != 200 || f.params.Get("param_p_pv") != "9" {
			t.Fatalf("explicit: %d %s %s", c, b, f.params.Get("param_p_pv"))
		}
		f.query = ""
		c, b = post(t, api, path, "tok", `{`+rng+extra+`}`)
		if c != 400 || f.query != "" || !strings.Contains(b, "source_id is required") {
			t.Fatalf("no source: %d %s", c, b)
		}
		c, b = post(t, api, path, "tok", `{`+rng+`,"source_id":"`+srcB+`"`+extra+`}`)
		if c != 400 || !strings.Contains(b, "no_active_version") || f.query != "" {
			t.Fatalf("foreign source: %d %s", c, b)
		}
	}
	// summaries (GET)
	q := "from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z"
	for _, path := range []string{"/v1/metrics/summary", "/v1/quality/summary"} {
		c, b := get(t, api, path+"?"+q+"&source_id="+srcA, "tok")
		if c != 200 || f.params.Get("param_p_pv") != "4" {
			t.Fatalf("%s: %d %s", path, c, b)
		}
		if c, _ := get(t, api, path+"?"+q+"&source_id="+srcA+"&processing_version=2", "tok"); c != 200 || f.params.Get("param_p_pv") != "2" {
			t.Fatalf("%s explicit: %d pv=%s", path, c, f.params.Get("param_p_pv"))
		}
		if c, b := get(t, api, path+"?"+q, "tok"); c != 400 || !strings.Contains(b, "source_id is required") {
			t.Fatalf("%s no source: %d %s", path, c, b)
		}
		if c, b := get(t, api, path+"?"+q+"&source_id="+srcB, "tok"); c != 400 || !strings.Contains(b, "no_active_version") {
			t.Fatalf("%s foreign: %d %s", path, c, b)
		}
		if c, _ := get(t, api, path+"?"+q+"&processing_version=abc&source_id="+srcA, "tok"); c != 400 {
			t.Fatalf("%s bad pv: %d", path, c)
		}
	}
}

func TestHTTPNoResolverStillRequiresVersion(t *testing.T) {
	api, f := setupSrv(t, nil)
	f.query = ""
	if c, _ := post(t, api, "/v1/query/events", "tok", `{`+rng+`,"source_id":"`+srcA+`"}`); c != 400 || f.query != "" {
		t.Fatalf("%d", c)
	}
	if c, _ := get(t, api, "/v1/metrics/summary?from=2026-10-20T00:00:00Z&to=2026-10-21T00:00:00Z", "tok"); c != 400 {
		t.Fatalf("%d", c)
	}
}

// Live PostgreSQL (skipped unless QUERY_PG_IT_URL is set): non-superuser NOBYPASSRLS role, migrations 0001-0010 applied.
func TestPGVersionResolverLive(t *testing.T) {
	url := os.Getenv("QUERY_PG_IT_URL")
	if url == "" {
		t.Skip("QUERY_PG_IT_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(sql string, args ...any) {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, sql, args...); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	sfx := fmt.Sprint(time.Now().UnixNano() % 1_000_000_000)
	var tA, tB, sA, sB string
	adminRow := func(dst *string, sql string, args ...any) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`)
		if err := tx.QueryRow(ctx, sql, args...).Scan(dst); err != nil {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	adminRow(&tA, `INSERT INTO tenants (slug, name) VALUES ($1,'q a') RETURNING id::text`, "qv-a-"+sfx)
	adminRow(&tB, `INSERT INTO tenants (slug, name) VALUES ($1,'q b') RETURNING id::text`, "qv-b-"+sfx)
	adminRow(&sA, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid,'s','http_push') RETURNING id::text`, tA)
	adminRow(&sB, `INSERT INTO sources (tenant_id, name, kind) VALUES ($1::uuid,'s','http_push') RETURNING id::text`, tB)
	t.Cleanup(func() {
		admin(`DELETE FROM source_active_versions WHERE tenant_id IN ($1::uuid,$2::uuid)`, tA, tB)
		admin(`DELETE FROM sources WHERE tenant_id IN ($1::uuid,$2::uuid)`, tA, tB)
		admin(`DELETE FROM tenants WHERE id IN ($1::uuid,$2::uuid)`, tA, tB)
	})
	admin(`INSERT INTO source_active_versions (tenant_id, source_id, active_processing_version) VALUES ($1::uuid,$2::uuid,3),($3::uuid,$4::uuid,8)`, tA, sA, tB, sB)

	r := PGVersionResolver{Pool: pool}
	if v, err := r.Resolve(ctx, tA, sA); err != nil || v != 3 {
		t.Fatalf("A: %d %v", v, err)
	}
	if v, err := r.Resolve(ctx, tB, sB); err != nil || v != 8 {
		t.Fatalf("B: %d %v", v, err)
	}
	// cross-tenant: A asking for B's source (and vice versa) never resolves
	if _, err := r.Resolve(ctx, tA, sB); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("A->sB: %v", err)
	}
	if _, err := r.Resolve(ctx, tB, sA); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("B->sA: %v", err)
	}
	// RLS is real, independent of the WHERE clause: without a tenant the table is empty, and with tenant A's
	// session the predicate for B's row returns nothing.
	tx, _ := pool.BeginTx(ctx, pgx.TxOptions{})
	defer tx.Rollback(ctx)
	var n int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM source_active_versions`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("no-tenant session sees %d rows (%v)", n, err)
	}
	_, _ = tx.Exec(ctx, sqlSetTenant, tA)
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM source_active_versions`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("tenant A sees %d rows (%v)", n, err)
	}
	// unknown source -> no_active_version
	if _, err := r.Resolve(ctx, tA, "33333333-3333-3333-3333-333333333333"); !errors.Is(err, ErrNoActiveVersion) {
		t.Fatalf("unknown: %v", err)
	}
}
