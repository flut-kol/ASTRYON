package quota

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/astryon/file-ingestion/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live: FILEQUOTA_PG_URL (non-superuser without BYPASSRLS, migrations applied, DML on tenants, sources, file_objects,
// tenant_quotas).
func TestLiveUsageAndOverrides(t *testing.T) {
	url := os.Getenv("FILEQUOTA_PG_URL")
	if url == "" {
		t.Skip("FILEQUOTA_PG_URL not set")
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
	must := func(_ any, err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	var a, b, srcA string
	admin(func(tx pgx.Tx) {
		n := time.Now().UnixNano() % 1_000_000_000
		must(nil, tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("fq-a-%d", n)).Scan(&a))
		must(nil, tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("fq-b-%d", n)).Scan(&b))
		must(nil, tx.QueryRow(ctx, `INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','s3_files') RETURNING id::text`, a).Scan(&srcA))
		for i, row := range []struct {
			size int64
			done bool
			del  bool
		}{{100, false, false}, {200, true, false}, {400, false, true}} {
			c := "NULL"
			if row.done {
				c = "now()"
			}
			d := "NULL"
			if row.del {
				d = "now()"
			}
			_, err := tx.Exec(ctx, fmt.Sprintf(`INSERT INTO file_objects (tenant_id,source_id,bucket,object_key,size_bytes,committed_at,deleted_at)
				VALUES ($1::uuid,$2::uuid,'b',$3,$4,%s,%s)`, c, d), a, srcA, fmt.Sprintf("k%d", i), row.size)
			must(nil, err)
		}
		_, err := tx.Exec(ctx, `INSERT INTO tenant_quotas (tenant_id,resource,limit_value,period) VALUES ($1::uuid,'upload_open_max',1,'total'),($1::uuid,'upload_bytes',1000,'day')`, a)
		must(nil, err)
	})
	t.Cleanup(func() {
		admin(func(tx pgx.Tx) {
			for _, q := range []string{`DELETE FROM tenant_quotas WHERE tenant_id=$1::uuid`, `DELETE FROM file_objects WHERE tenant_id=$1::uuid`,
				`DELETE FROM sources WHERE tenant_id=$1::uuid`, `DELETE FROM tenants WHERE id=$1::uuid`} {
				tx.Exec(ctx, q, a)
				tx.Exec(ctx, q, b)
			}
		})
	})
	st := PGStore{DB: jobs.PoolDB{Pool: pool}}
	u, err := st.Usage(ctx, a)
	if err != nil || u.Open != 1 || u.Bytes != 300 {
		t.Fatalf("usage %+v %v (want open 1, bytes 300: committed counts as bytes only, deleted counts as nothing)", u, err)
	}
	if u, err := st.Usage(ctx, b); err != nil || u != (Usage{}) {
		t.Fatalf("tenant isolation: %+v %v", u, err)
	}
	ov, err := st.Overrides(ctx, a)
	if err != nil || ov[ResOpenMax] != 1 || ov[ResBytesDay] != 1000 || len(ov) != 2 {
		t.Fatalf("overrides %v %v", ov, err)
	}
	c := Checker{Store: st, Defaults: Limits{MaxFileBytes: 5000}}
	if err := c.Check(ctx, a, 10); err != ErrTooManyOpen {
		t.Fatalf("want open quota, got %v", err)
	}
	if err := c.Check(ctx, b, 6000); err != ErrFileTooLarge {
		t.Fatalf("want too large, got %v", err)
	}
	if err := c.Check(ctx, b, 10); err != nil {
		t.Fatal(err)
	}
}
