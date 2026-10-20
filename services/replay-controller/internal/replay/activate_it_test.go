package replay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live PostgreSQL cutover test, skipped unless REPLAY_PG_URL is set (non-superuser NOBYPASSRLS role;
// migrations 0001-0010 applied).
func TestPGLiveActivation(t *testing.T) {
	pool, tA, tB, src, srcB := pgFixture(t)
	ctx := context.Background()
	admin := func(sql string, args ...any) string {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlAsAdmin)
		var id string
		if err := tx.QueryRow(ctx, sql, args...).Scan(&id); err != nil && !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return id
	}
	t.Cleanup(func() {
		admin(`WITH d AS (DELETE FROM source_active_versions WHERE tenant_id IN ($1::uuid,$2::uuid) RETURNING 1) SELECT ''`, tA, tB)
	})
	mk := func(tenant, source string, v int, status string) string {
		return admin(`INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status) VALUES ($1::uuid,$2::uuid,$3,$4::replay_status) RETURNING id::text`,
			tenant, source, v, status)
	}
	r1, r2, r3 := mk(tA, src, 1, "COMPLETED"), mk(tA, src, 2, "COMPLETED"), mk(tA, src, 3, "RUNNING")
	rB := mk(tB, srcB, 7, "COMPLETED")
	a := &PGActivator{DB: PoolDB{pool}}

	if _, err := a.Activate(ctx, tA, r3, false, "it", nil); !errors.Is(err, ErrNotCompleted) {
		t.Fatalf("non-completed: %v", err)
	}
	// tenant A cannot touch tenant B's replay; and the reverse
	if _, err := a.Activate(ctx, tA, rB, false, "it", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross tenant: %v", err)
	}
	if _, err := a.Activate(ctx, tB, r1, false, "it", nil); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross tenant 2: %v", err)
	}
	got, err := a.Activate(ctx, tA, r1, false, "it", nil)
	if err != nil || got.Action != "activate" || got.ActiveProcessingVersion != 1 || got.PreviousProcessingVersion != nil || got.SwitchedAt.IsZero() {
		t.Fatalf("%+v %v", got, err)
	}
	if got, err = a.Activate(ctx, tA, r2, false, "it", nil); err != nil || got.ActiveProcessingVersion != 2 || got.PreviousProcessingVersion == nil || *got.PreviousProcessingVersion != 1 {
		t.Fatalf("%+v %v", got, err)
	}
	if _, err := a.Activate(ctx, tA, r1, false, "it", nil); !errors.Is(err, ErrRegression) {
		t.Fatalf("regression: %v", err)
	}
	if got, err = a.Activate(ctx, tA, r2, false, "it", nil); err != nil || got.Action != "noop" || got.PreviousProcessingVersion == nil || *got.PreviousProcessingVersion != 1 {
		t.Fatalf("noop: %+v %v", got, err)
	}
	if _, err := a.Activate(ctx, tA, r1, true, "it", nil); !errors.Is(err, ErrRollbackNothing) {
		t.Fatalf("rollback of inactive: %v", err)
	}
	if got, err = a.Activate(ctx, tA, r2, true, "it", nil); err != nil || got.Action != "rollback" || got.ActiveProcessingVersion != 1 || *got.PreviousProcessingVersion != 2 {
		t.Fatalf("rollback: %+v %v", got, err)
	}
	// tenant B's rows are invisible to tenant A's session; no-tenant session sees nothing
	read := func(tenant string) (n int) {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if tenant != "" {
			_, _ = tx.Exec(ctx, sqlSetTenant, tenant)
		}
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM source_active_versions`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if _, err := a.Activate(ctx, tB, rB, false, "it", nil); err != nil {
		t.Fatal(err)
	}
	if read("") != 0 || read(tA) != 1 || read(tB) != 1 {
		t.Fatalf("RLS: none=%d A=%d B=%d", read(""), read(tA), read(tB))
	}
	// RLS rejects a cross-tenant write even if the application tried
	tx, _ := pool.Begin(ctx)
	_, _ = tx.Exec(ctx, sqlSetTenant, tA)
	if _, err := tx.Exec(ctx, `INSERT INTO source_active_versions (tenant_id, source_id, active_processing_version) VALUES ($1::uuid,$2::uuid,1)`, tB, srcB); err == nil {
		t.Fatal("cross-tenant insert accepted")
	}
	_ = tx.Rollback(ctx)
}

// Two replicas (two pools) race to activate different versions of the same source: exactly one row must remain,
// holding the highest version, with previous < active, and no regression is ever observed.
func TestPGLiveConcurrentActivations(t *testing.T) {
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	pool2, err := pgxpool.New(ctx, os.Getenv("REPLAY_PG_URL"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool2.Close()
	ids := make([]string, 0, 6)
	tx, _ := pool.Begin(ctx)
	_, _ = tx.Exec(ctx, sqlAsAdmin)
	for v := 1; v <= 6; v++ {
		var id string
		if err := tx.QueryRow(ctx, `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status) VALUES ($1::uuid,$2::uuid,$3,'COMPLETED') RETURNING id::text`, tA, src, v).Scan(&id); err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlAsAdmin)
		_, _ = tx.Exec(ctx, `DELETE FROM source_active_versions WHERE tenant_id = $1::uuid`, tA)
		_ = tx.Commit(ctx)
	})
	acts := []*PGActivator{{DB: PoolDB{pool}}, {DB: PoolDB{pool2}}}
	var wg sync.WaitGroup
	var mu sync.Mutex
	errs := map[string]int{}
	for i := 0; i < 36; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := acts[i%2].Activate(ctx, tA, ids[(i*5)%6], false, fmt.Sprint("w", i), nil)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				errs["ok"]++
			case errors.Is(err, ErrRegression):
				errs["regression"]++
			default:
				errs[err.Error()]++
			}
		}(i)
	}
	wg.Wait()
	for k := range errs {
		if k != "ok" && k != "regression" {
			t.Fatalf("unexpected errors: %v", errs)
		}
	}
	tx, _ = pool.Begin(ctx)
	defer tx.Rollback(ctx)
	_, _ = tx.Exec(ctx, sqlSetTenant, tA)
	var n int
	var active int32
	var prev *int32
	if err := tx.QueryRow(ctx, `SELECT count(*), max(active_processing_version), max(previous_processing_version) FROM source_active_versions WHERE source_id = $1::uuid`, src).Scan(&n, &active, &prev); err != nil {
		t.Fatal(err)
	}
	if n != 1 || active != 6 || prev == nil || *prev >= active {
		t.Fatalf("rows=%d active=%d prev=%v outcomes=%v", n, active, prev, errs)
	}
}

// Required audit mode on real PostgreSQL: a failing PreCommit (audit event not publishable) must leave NO active
// version; a retry with a working sink commits. Through the HTTP handler: 503 and still nothing active.
func TestPGLiveRequiredAuditFailureLeavesVersionUnchanged(t *testing.T) {
	pool, tA, _, src, _ := pgFixture(t)
	ctx := context.Background()
	tx, _ := pool.Begin(ctx)
	_, _ = tx.Exec(ctx, sqlAsAdmin)
	var r1 string
	if err := tx.QueryRow(ctx, `INSERT INTO replay_requests (tenant_id, source_id, target_processing_version, status) VALUES ($1::uuid,$2::uuid,1,'COMPLETED') RETURNING id::text`, tA, src).Scan(&r1); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlAsAdmin)
		_, _ = tx.Exec(ctx, `DELETE FROM source_active_versions WHERE tenant_id = $1::uuid`, tA)
		_ = tx.Commit(ctx)
	})
	count := func() (n int) {
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		_, _ = tx.Exec(ctx, sqlSetTenant, tA)
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM source_active_versions WHERE source_id = $1::uuid`, src).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	a := &PGActivator{DB: PoolDB{pool}}
	boom := errors.New("kafka down")
	if _, err := a.Activate(ctx, tA, r1, false, "it", func(context.Context, Activation) error { return fmt.Errorf("%w: %v", ErrAuditUnavailable, boom) }); !errors.Is(err, ErrAuditUnavailable) {
		t.Fatalf("got %v", err)
	}
	if n := count(); n != 0 {
		t.Fatalf("activation committed despite audit failure: %d rows", n)
	}
	// via the handler with a failing Kafka sink
	kf := &kafkaFake{err: boom}
	auth := DevStaticAuthenticator{Token: "tok", Principal: Principal{TenantID: tA, Subject: "alice", Scopes: []string{ScopeReplayWrite}}}
	h := (&Handler{Auth: auth, Planner: &Planner{}, Activator: a, Audit: newKafkaAuditSink(kf, 0), AuditRequired: true}).Routes()
	do := func() int {
		r := httptest.NewRequest("POST", "/v1/replays/"+r1+"/activate", nil)
		r.Header.Set("Authorization", "Bearer tok")
		r.Header.Set("X-Request-Id", "it-1")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		return w.Code
	}
	if c := do(); c != 503 || count() != 0 {
		t.Fatalf("status %d rows %d", c, count())
	}
	kf.err = nil
	if c := do(); c != 200 || count() != 1 || len(kf.vals) != 2 {
		t.Fatalf("retry status rows=%d events=%d", count(), len(kf.vals))
	}
	// the event was published BEFORE commit and carries the principal subject
	var ev AuditEvent
	_ = json.Unmarshal(kf.vals[0], &ev)
	var fin AuditEvent
	_ = json.Unmarshal(kf.vals[1], &fin)
	if ev.Phase != PhasePrepared || fin.Phase != PhaseCommitted || ev.Subject != "alice" || ev.TenantID != tA || ev.RequestID != "it-1" || fin.Outcome != OutcomeSuccess {
		t.Fatalf("%+v", ev)
	}
}
