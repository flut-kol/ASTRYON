package schematest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// withTenant runs fn inside a transaction on conn with astryon.tenant_id and/or
// astryon.platform_admin set via SET LOCAL (so the setting is scoped to the transaction,
// mirroring how a real request handler would set it from validated token claims - see
// AGENTS.md: tenant_id must only ever come from validated claims, never headers/body).
// Commits on success, rolls back and returns the error otherwise. tenantID/platformAdmin
// of "" skip that SET LOCAL.
func withTenantErr(t *testing.T, ctx context.Context, conn *pgx.Conn, tenantID, platformAdmin string, fn func(pgx.Tx) error) error {
	t.Helper()
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	if tenantID != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL astryon.tenant_id = '%s'", tenantID)); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("set astryon.tenant_id: %v", err)
		}
	}
	if platformAdmin != "" {
		if _, err := tx.Exec(ctx, fmt.Sprintf("SET LOCAL astryon.platform_admin = '%s'", platformAdmin)); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("set astryon.platform_admin: %v", err)
		}
	}
	fnErr := fn(tx)
	if fnErr != nil {
		_ = tx.Rollback(ctx)
		return fnErr
	}
	return tx.Commit(ctx)
}

func withTenant(t *testing.T, ctx context.Context, conn *pgx.Conn, tenantID, platformAdmin string, fn func(pgx.Tx) error) {
	t.Helper()
	if err := withTenantErr(t, ctx, conn, tenantID, platformAdmin, fn); err != nil {
		t.Fatalf("tx (tenant=%q platform_admin=%q): %v", tenantID, platformAdmin, err)
	}
}

func countByID(t *testing.T, ctx context.Context, tx pgx.Tx, table, id string) int {
	t.Helper()
	var n int
	// table is always one of the fixed literals in this file's test table, never external
	// input, so string-building the table name here is safe.
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE id = $1", id).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func testSlug(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// TestRLSTenantIsolation covers requirement (c) across three representative RLS-enabled
// tables spanning three different migrations: principals (0001), pipelines (0003) and
// audit_log (0007). It uses a dedicated non-superuser role (see ensureLowPrivRole) since
// RLS is not enforced for superusers/table owners with BYPASSRLS by default, and the dev
// `astryon` role is a superuser.
func TestRLSTenantIsolation(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	tenantA := newUUID(t)
	tenantB := newUUID(t)
	mustExec(t, ctx, admin, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'RLS Test Tenant A')`, tenantA, testSlug("it-rls-a"))
	mustExec(t, ctx, admin, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'RLS Test Tenant B')`, tenantB, testSlug("it-rls-b"))

	// Fixture needed for the pipelines case: pipelines.source_id has a tenant-scoped
	// composite FK to sources, so each tenant needs its own source row.
	sourceA := newUUID(t)
	sourceB := newUUID(t)
	mustExec(t, ctx, admin, `INSERT INTO sources (id, tenant_id, name, kind) VALUES ($1,$2,'rls-src','kafka')`, sourceA, tenantA)
	mustExec(t, ctx, admin, `INSERT INTO sources (id, tenant_id, name, kind) VALUES ($1,$2,'rls-src','kafka')`, sourceB, tenantB)

	cases := []struct {
		table  string
		insert string
		args   func(id, tenant string) []any
	}{
		{
			table:  "principals",
			insert: `INSERT INTO principals (id, tenant_id, aas_sub) VALUES ($1,$2,$3)`,
			args:   func(id, tenant string) []any { return []any{id, tenant, "sub-" + id} },
		},
		{
			table:  "pipelines",
			insert: `INSERT INTO pipelines (id, tenant_id, source_id, name) VALUES ($1,$2,$3,$4)`,
			args: func(id, tenant string) []any {
				src := sourceA
				if tenant == tenantB {
					src = sourceB
				}
				return []any{id, tenant, src, "pl-" + id}
			},
		},
		{
			table:  "audit_log",
			insert: `INSERT INTO audit_log (id, tenant_id, action, entity_type) VALUES ($1,$2,'test.action','test_entity')`,
			args:   func(id, tenant string) []any { return []any{id, tenant} },
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.table, func(t *testing.T) {
			lp := lowPrivConn(t, ctx, admin)

			idA := newUUID(t)
			idB := newUUID(t)

			withTenant(t, ctx, lp, tenantA, "", func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, c.insert, c.args(idA, tenantA)...)
				return err
			})
			withTenant(t, ctx, lp, tenantB, "", func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, c.insert, c.args(idB, tenantB)...)
				return err
			})

			withTenant(t, ctx, lp, tenantA, "", func(tx pgx.Tx) error {
				if n := countByID(t, ctx, tx, c.table, idA); n != 1 {
					t.Errorf("%s: tenant A should see its own row, count=%d", c.table, n)
				}
				if n := countByID(t, ctx, tx, c.table, idB); n != 0 {
					t.Errorf("%s: tenant A must NOT see tenant B's row, count=%d", c.table, n)
				}
				return nil
			})

			withTenant(t, ctx, lp, tenantB, "", func(tx pgx.Tx) error {
				if n := countByID(t, ctx, tx, c.table, idB); n != 1 {
					t.Errorf("%s: tenant B should see its own row, count=%d", c.table, n)
				}
				if n := countByID(t, ctx, tx, c.table, idA); n != 0 {
					t.Errorf("%s: tenant B must NOT see tenant A's row, count=%d", c.table, n)
				}
				return nil
			})

			withTenant(t, ctx, lp, "", "on", func(tx pgx.Tx) error {
				n := countByID(t, ctx, tx, c.table, idA) + countByID(t, ctx, tx, c.table, idB)
				if n != 2 {
					t.Errorf("%s: platform_admin='on' should bypass isolation and see both rows, count=%d", c.table, n)
				}
				return nil
			})

			idC := newUUID(t)
			err := withTenantErr(t, ctx, lp, tenantA, "", func(tx pgx.Tx) error {
				_, err := tx.Exec(ctx, c.insert, c.args(idC, tenantB)...)
				return err
			})
			if err == nil {
				t.Errorf("%s: cross-tenant insert (session tenant=A, row.tenant_id=B) should be rejected by RLS WITH CHECK", c.table)
			} else if code := pgErrCode(err); code != "42501" {
				t.Errorf("%s: expected RLS policy violation (SQLSTATE 42501), got code=%q err=%v", c.table, code, err)
			}
		})
	}
}

func mustExec(t *testing.T, ctx context.Context, conn *pgx.Conn, sql string, args ...any) {
	t.Helper()
	if _, err := conn.Exec(ctx, sql, args...); err != nil {
		t.Fatalf("exec %q: %v", sql, err)
	}
}
