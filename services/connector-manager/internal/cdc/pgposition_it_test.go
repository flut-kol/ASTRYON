package cdc

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live: CDC_POS_PG_URL (non-superuser without BYPASSRLS, migrations through 0013 applied).
func TestLivePGPositions(t *testing.T) {
	url := os.Getenv("CDC_POS_PG_URL")
	if url == "" {
		t.Skip("CDC_POS_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(fn func(tx pgx.Tx)) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin','on',true)`); err != nil {
			t.Fatal(err)
		}
		fn(tx)
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	}
	mk := func(slug string) (tenant, conn string) {
		admin(func(tx pgx.Tx) {
			var source string
			must := func(err error) {
				if err != nil {
					t.Fatal(err)
				}
			}
			must(tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("%s-%d", slug, time.Now().UnixNano()%1_000_000_000)).Scan(&tenant))
			must(tx.QueryRow(ctx, `INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','api_pull') RETURNING id::text`, tenant).Scan(&source))
			must(tx.QueryRow(ctx, `INSERT INTO connectors (tenant_id,source_id,name,connector_type,config) VALUES ($1::uuid,$2::uuid,'c','REST_PULL','{"endpoint":"http://x","schema_id":"s"}') RETURNING id::text`, tenant, source).Scan(&conn))
		})
		return
	}
	tA, cA := mk("cdcpos-a")
	tB, _ := mk("cdcpos-b")
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) {
			for _, tn := range []string{tA, tB} {
				tx.Exec(ctx, `DELETE FROM connector_cdc_position WHERE tenant_id=$1::uuid`, tn)
				tx.Exec(ctx, `DELETE FROM connectors WHERE tenant_id=$1::uuid`, tn)
				tx.Exec(ctx, `DELETE FROM sources WHERE tenant_id=$1::uuid`, tn)
				tx.Exec(ctx, `DELETE FROM tenants WHERE id=$1::uuid`, tn)
			}
		})
	})
	a := &PGPositions{Pool: pool, Tenant: tA}
	if err := a.SavePosition(ctx, cA, "lsn:1"); err != nil {
		t.Fatal(err)
	}
	if err := a.SavePosition(ctx, cA, "lsn:2"); err != nil {
		t.Fatal(err)
	}
	if got, err := a.LoadPosition(ctx, cA); err != nil || got != "lsn:2" {
		t.Fatalf("got %q err=%v", got, err)
	}
	// another tenant cannot see or write tenant A's connector row
	b := &PGPositions{Pool: pool, Tenant: tB}
	if got, err := b.LoadPosition(ctx, cA); err != nil || got != "" {
		t.Fatalf("cross-tenant read leaked %q err=%v", got, err)
	}
	if err := b.SavePosition(ctx, cA, "evil"); err == nil {
		t.Fatal("cross-tenant write must fail")
	}
	if got, _ := a.LoadPosition(ctx, cA); got != "lsn:2" {
		t.Fatalf("position changed: %q", got)
	}
}
