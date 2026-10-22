package audit

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Live test: AUDIT_IT_PG_URL must point at a PG16 with migrations 0001..0015 applied, connecting as a
// NON-superuser, non-BYPASSRLS role that owns no tables (RLS FORCE covers owners, but not superusers).
func TestPGRoundTripAndRLS(t *testing.T) {
	url := os.Getenv("AUDIT_IT_PG_URL")
	if url == "" {
		t.Skip("AUDIT_IT_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	tenantB := "44444444-4444-4444-8444-444444444444"
	w := NewWriter(pool, Config{FlushInterval: 20 * time.Millisecond})
	a := NewEvent(claims(), CategorySecurity, "auth.denied", OutcomeDenied, Resource{Type: "export", ID: "bucket/key"}).
		WithTrace("0af7651916cd43dd8448eb211c80319c", "req-1").WithDetails(map[string]string{"reason": "missing_scope"})
	b := a
	b.TenantID = tenantB
	for i := 0; i < 3; i++ {
		if err := w.Submit(a); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.WriteSync(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(ctx); err != nil {
		t.Fatal(err)
	}
	recs, _, err := List(ctx, pool, tid, Filter{Category: CategorySecurity, Limit: 2})
	if err != nil || len(recs) != 2 {
		t.Fatalf("%v %d", err, len(recs))
	}
	for _, r := range recs {
		if r.TenantID != tid || r.Resource.ID != "bucket/key" || r.TraceID == "" || r.Details["reason"] != "missing_scope" || r.ACR == "" {
			t.Fatalf("%+v", r)
		}
	}
	all, next, err := List(ctx, pool, tid, Filter{Limit: 100})
	if err != nil || len(all) < 3 || !next.OccurredAt.IsZero() {
		t.Fatalf("%v %d", err, len(all))
	}
	_, next, _ = List(ctx, pool, tid, Filter{Limit: 1})
	if next.ID == "" {
		t.Fatal("expected cursor")
	}
	// Tenant B sees only its own row; append-only trigger blocks UPDATE.
	rb, _, _ := List(ctx, pool, tenantB, Filter{})
	for _, r := range rb {
		if r.TenantID != tenantB {
			t.Fatal("cross-tenant leak")
		}
	}
	tx, _ := pool.Begin(ctx)
	defer tx.Rollback(ctx)
	_, _ = tx.Exec(ctx, sqlSetTenant, tid)
	if _, err := tx.Exec(ctx, `UPDATE audit_log SET action='x.y'`); err == nil {
		t.Fatal("audit_log must be append-only")
	}
}
