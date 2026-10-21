package connstore

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/astryon/connector-manager/internal/adminapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Live: CONNSTORE_IT_PG_URL (non-superuser without BYPASSRLS, migrations 0001-0011 applied).
func TestLiveStore(t *testing.T) {
	url := os.Getenv("CONNSTORE_IT_PG_URL")
	if url == "" {
		t.Skip("CONNSTORE_IT_PG_URL not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	admin := func(fn func(tx pgx.Tx) error) error {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SELECT set_config('astryon.platform_admin', 'on', true)`); err != nil {
			return err
		}
		if err := fn(tx); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	seed := uint32(time.Now().UnixNano())
	mk := func(label string) (tenant, source string) {
		if err := admin(func(tx pgx.Tx) error {
			if err := tx.QueryRow(ctx, `INSERT INTO tenants (slug,name) VALUES ($1,'it') RETURNING id::text`, fmt.Sprintf("cs-%s-%d", label, seed%1_000_000_000)).Scan(&tenant); err != nil {
				return err
			}
			return tx.QueryRow(ctx, `INSERT INTO sources (tenant_id,name,kind) VALUES ($1::uuid,'s','api_pull') RETURNING id::text`, tenant).Scan(&source)
		}); err != nil {
			t.Fatal(err)
		}
		return
	}
	ta, sa := mk("a")
	tb, sb := mk("b")
	t.Cleanup(func() {
		_ = admin(func(tx pgx.Tx) error {
			for _, tn := range []string{ta, tb} {
				for _, q := range []string{`DELETE FROM connectors WHERE tenant_id=$1::uuid`, `DELETE FROM source_credentials WHERE tenant_id=$1::uuid`,
					`DELETE FROM sources WHERE tenant_id=$1::uuid`, `DELETE FROM tenants WHERE id=$1::uuid`} {
					if _, err := tx.Exec(ctx, q, tn); err != nil {
						return err
					}
				}
			}
			return nil
		})
	})
	s := &Store{Pool: pool}
	in := adminapi.CreateInput{SourceID: sa, Name: "c1", Kind: "REST_PULL", SecretRef: "env://TOKEN_A", Config: map[string]string{"endpoint": "http://x"}}

	c, err := s.Create(ctx, ta, in)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if c.ID == "" || c.SourceID != sa || c.Name != "c1" || c.Kind != "REST_PULL" || !c.Enabled || c.SecretRef != "env://TOKEN_A" || c.Config["endpoint"] != "http://x" {
		t.Fatalf("create result: %+v", c)
	}
	if _, err := s.Create(ctx, ta, in); !errors.Is(err, adminapi.ErrConflict) {
		t.Fatalf("duplicate create: %v", err)
	}
	in2 := in
	in2.Name = "c2"
	c2, err := s.Create(ctx, ta, in2)
	if err != nil {
		t.Fatalf("create 2 (failed duplicate must not poison state): %v", err)
	}
	if _, err := s.Create(ctx, ta, adminapi.CreateInput{SourceID: sb, Name: "x", Kind: "REST_PULL", SecretRef: "env://T"}); !errors.Is(err, adminapi.ErrBadReference) {
		t.Fatalf("foreign source: %v", err)
	}
	if _, err := s.Create(ctx, ta, adminapi.CreateInput{SourceID: "00000000-0000-0000-0000-000000000000", Name: "x", Kind: "REST_PULL", SecretRef: "env://T"}); !errors.Is(err, adminapi.ErrBadReference) {
		t.Fatalf("missing source: %v", err)
	}

	got, err := s.Get(ctx, ta, c.ID)
	if err != nil || got.ID != c.ID || got.Config["endpoint"] != "http://x" {
		t.Fatalf("get: %v %+v", err, got)
	}
	// list with pagination
	all, err := s.List(ctx, ta, "", 10)
	if err != nil || len(all) != 2 || all[0].ID >= all[1].ID {
		t.Fatalf("list: %v %+v", err, all)
	}
	page, err := s.List(ctx, ta, all[0].ID, 10)
	if err != nil || len(page) != 1 || page[0].ID != all[1].ID {
		t.Fatalf("list after: %v %+v", err, page)
	}
	if one, err := s.List(ctx, ta, "", 1); err != nil || len(one) != 1 {
		t.Fatalf("list limit: %v %+v", err, one)
	}
	// pause / resume
	p, err := s.SetEnabled(ctx, ta, c.ID, false)
	if err != nil || p.Enabled {
		t.Fatalf("pause: %v %+v", err, p)
	}
	if g, _ := s.Get(ctx, ta, c.ID); g.Enabled {
		t.Fatal("pause not persisted")
	}
	r, err := s.SetEnabled(ctx, ta, c.ID, true)
	if err != nil || !r.Enabled {
		t.Fatalf("resume: %v %+v", err, r)
	}

	// tenant isolation through the store (RLS + explicit filter)
	if _, err := s.Get(ctx, tb, c.ID); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("foreign get: %v", err)
	}
	if _, err := s.SetEnabled(ctx, tb, c.ID, false); !errors.Is(err, adminapi.ErrNotFound) {
		t.Fatalf("foreign pause: %v", err)
	}
	if lb, err := s.List(ctx, tb, "", 10); err != nil || len(lb) != 0 {
		t.Fatalf("foreign list: %v %+v", err, lb)
	}
	if g, _ := s.Get(ctx, ta, c.ID); !g.Enabled {
		t.Fatal("foreign pause leaked")
	}
	_ = c2

	// RLS alone (no explicit tenant filter): tenant A context cannot see or write tenant B rows.
	cb, err := s.Create(ctx, tb, adminapi.CreateInput{SourceID: sb, Name: "cb", Kind: "REST_PULL", SecretRef: "env://TB"})
	if err != nil {
		t.Fatal(err)
	}
	err = s.tx(ctx, ta, func(tx pgx.Tx) error {
		var n int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM connectors WHERE id = $1::uuid`, cb.ID).Scan(&n); err != nil {
			return err
		}
		if n != 0 {
			return fmt.Errorf("RLS leak: %d rows visible", n)
		}
		tag, err := tx.Exec(ctx, `UPDATE connectors SET is_enabled=false WHERE id = $1::uuid`, cb.ID)
		if err != nil {
			return err
		}
		if tag.RowsAffected() != 0 {
			return fmt.Errorf("RLS update leak")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// A failed statement aborts the tx, so the cross-tenant write check runs on its own.
	err = s.tx(ctx, ta, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `INSERT INTO source_credentials (tenant_id, source_id, name, secret_ref) VALUES ($1::uuid,$2::uuid,'z','env://Z')`, tb, sb)
		if err == nil {
			return fmt.Errorf("RLS allowed cross-tenant insert")
		}
		return nil
	})
	if err == nil {
		t.Fatal("expected tx failure after RLS violation")
	}
}
