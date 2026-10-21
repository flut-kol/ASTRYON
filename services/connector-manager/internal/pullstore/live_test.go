package pullstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live: CONNPULL_PG_URL (non-superuser without BYPASSRLS, migrations 0001-0011 applied).
func TestLiveClaimFenceAndSchedule(t *testing.T) {
	url := os.Getenv("CONNPULL_PG_URL")
	if url == "" {
		t.Skip("CONNPULL_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(fn func(tx pgx.Tx) error) {
		t.Helper()
		tx, _ := pool.Begin(ctx)
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, sqlAdminOn); err != nil {
			t.Fatal(err)
		}
		if err := fn(tx); err != nil {
			t.Fatal(err)
		}
		tx.Commit(ctx)
	}
	var tenant, source, conn string
	admin(func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("pull-it-%d", time.Now().UnixNano()%1_000_000_000)).Scan(&tenant); err != nil {
			return err
		}
		if err := tx.QueryRow(ctx, `INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','api_pull') RETURNING id::text`, tenant).Scan(&source); err != nil {
			return err
		}
		return tx.QueryRow(ctx, `INSERT INTO connectors (tenant_id,source_id,name,connector_type,config) VALUES ($1::uuid,$2::uuid,'c','REST_PULL','{"endpoint":"http://x","schema_id":"s"}') RETURNING id::text`, tenant, source).Scan(&conn)
	})
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) error {
			tx.Exec(ctx, `DELETE FROM connector_pull_state WHERE tenant_id=$1::uuid`, tenant)
			tx.Exec(ctx, `DELETE FROM connectors WHERE tenant_id=$1::uuid`, tenant)
			tx.Exec(ctx, `DELETE FROM sources WHERE tenant_id=$1::uuid`, tenant)
			_, err := tx.Exec(ctx, `DELETE FROM tenants WHERE id=$1::uuid`, tenant)
			return err
		})
	})
	a := &Store{DB: PoolDB{pool}, WorkerID: "a", Lease: time.Minute}
	b := &Store{DB: PoolDB{pool}, WorkerID: "b", Lease: time.Minute}

	if _, err := a.Claim(ctx); !errors.Is(err, ErrNoConnector) {
		t.Fatalf("no state row yet: %v", err)
	}
	if n, err := a.Sync(ctx); err != nil || n < 1 {
		t.Fatalf("sync: %d %v", n, err)
	}
	c1, err := a.Claim(ctx)
	if err != nil || c1.ConnectorID != conn || c1.Fence != 1 || c1.Config["schema_id"] != "s" || c1.Cursor != "" {
		t.Fatalf("claim: %v %+v", err, c1)
	}
	if _, err := b.Claim(ctx); !errors.Is(err, ErrNoConnector) {
		t.Fatalf("leased connector must not be claimable: %v", err)
	}
	if err := a.Renew(ctx, c1); err != nil {
		t.Fatal(err)
	}
	if err := a.Succeed(ctx, c1, "cursor-2", 0, 5); err != nil { // more pages: due immediately
		t.Fatal(err)
	}
	if err := a.Succeed(ctx, c1, "stale", 0, 1); !errors.Is(err, ErrFenced) {
		t.Fatalf("second write with a released lease must be fenced: %v", err)
	}
	c2, err := b.Claim(ctx)
	if err != nil || c2.Cursor != "cursor-2" || c2.Fence != 2 || c2.ConsecutiveFailures != 0 {
		t.Fatalf("reclaim: %v %+v", err, c2)
	}
	if err := a.Fail(ctx, c1, time.Minute, "x"); !errors.Is(err, ErrFenced) {
		t.Fatalf("stale worker failed the connector: %v", err)
	}
	if err := b.Fail(ctx, c2, time.Hour, "http_429"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Claim(ctx); !errors.Is(err, ErrNoConnector) {
		t.Fatalf("failed connector must wait for its retry time: %v", err)
	}
	admin(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE connector_pull_state SET next_attempt_at = now() WHERE connector_id=$1::uuid`, conn)
		return err
	})
	c3, err := a.Claim(ctx)
	if err != nil || c3.ConsecutiveFailures != 1 || c3.Cursor != "cursor-2" {
		t.Fatalf("after retry time: %v %+v", err, c3)
	}
	// lease expiry lets another worker take over; the old owner is fenced
	admin(func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `UPDATE connector_pull_state SET locked_until = now() - interval '1 second' WHERE connector_id=$1::uuid`, conn)
		return err
	})
	c4, err := b.Claim(ctx)
	if err != nil || c4.Fence != c3.Fence+1 {
		t.Fatalf("takeover: %v %+v", err, c4)
	}
	if err := a.Succeed(ctx, c3, "zombie", time.Minute, 9); !errors.Is(err, ErrFenced) {
		t.Fatalf("zombie worker wrote: %v", err)
	}
	// connectors disabled meanwhile are not claimable
	admin(func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `UPDATE connectors SET is_enabled=false WHERE id=$1::uuid`, conn); err != nil {
			return err
		}
		_, err := tx.Exec(ctx, `UPDATE connector_pull_state SET locked_until=NULL, next_attempt_at=now() WHERE connector_id=$1::uuid`, conn)
		return err
	})
	if _, err := a.Claim(ctx); !errors.Is(err, ErrNoConnector) {
		t.Fatalf("disabled connector claimed: %v", err)
	}
}
