// Package schematest holds integration tests for the ASTRYON PostgreSQL control-plane
// schema (db/postgres/migrations/0001..0008). It talks to a real Postgres instance over
// github.com/jackc/pgx/v5 and is gated on DATABASE_URL, so `go test ./...` / `make test`
// is a safe no-op without a live database (same convention as
// services/clickhouse-sink/internal/sink/integration_test.go and realinfra_test.go).
//
// Run against the dev stack:
//
//	make dev-up                     # or: ./scripts/dev/up.sh postgres
//	export DATABASE_URL="postgres://astryon:astryon_dev_only@127.0.0.1:${POSTGRES_HOST_PORT:-15432}/astryon?sslmode=disable"
//	db/apply.sh postgres            # forward-only; safe/idempotent to re-run
//	go test ./db/postgres/schematest/... -v
//	make dev-down                   # when finished
//
// The dev `astryon` role is a Postgres superuser, which unconditionally bypasses Row Level
// Security regardless of FORCE ROW LEVEL SECURITY. The RLS tests below therefore create (and
// reuse, idempotently) a dedicated non-superuser, non-BYPASSRLS role, schematest_app, and run
// the actual tenant-isolation checks as that role.
package schematest

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// --- shared helpers -------------------------------------------------------

func dsnOrSkip(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		t.Skip("DATABASE_URL not set; skipping Postgres schema integration test")
	}
	return dsn
}

// adminConn connects with the DSN as-is (the dev `astryon` superuser / migration owner).
func adminConn(t *testing.T, ctx context.Context) *pgx.Conn {
	t.Helper()
	conn, err := pgx.Connect(ctx, dsnOrSkip(t))
	if err != nil {
		t.Fatalf("connect as admin: %v", err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

const (
	lowPrivRole     = "schematest_app"
	lowPrivPassword = "schematest_app_pw_dev_only"
)

// ensureLowPrivRole creates (if missing) a non-superuser, non-BYPASSRLS role and grants it
// DML on all public tables. Idempotent: safe to call repeatedly across test runs.
func ensureLowPrivRole(t *testing.T, ctx context.Context, admin *pgx.Conn) {
	t.Helper()

	var exists bool
	if err := admin.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)", lowPrivRole).Scan(&exists); err != nil {
		t.Fatalf("check role existence: %v", err)
	}
	if !exists {
		stmt := fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s' NOSUPERUSER NOBYPASSRLS", lowPrivRole, lowPrivPassword)
		if _, err := admin.Exec(ctx, stmt); err != nil {
			t.Fatalf("create low-priv role: %v", err)
		}
	}

	dbName := admin.Config().Database
	grants := []string{
		fmt.Sprintf("GRANT CONNECT ON DATABASE %s TO %s", dbName, lowPrivRole),
		fmt.Sprintf("GRANT USAGE ON SCHEMA public TO %s", lowPrivRole),
		fmt.Sprintf("GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO %s", lowPrivRole),
	}
	for _, g := range grants {
		if _, err := admin.Exec(ctx, g); err != nil {
			t.Fatalf("grant (%s): %v", g, err)
		}
	}

	// Double check it really is a non-superuser, non-bypassrls role: otherwise the RLS
	// tests below would pass vacuously (see AGENTS.md task: RLS is not enforced for
	// superusers/BYPASSRLS roles by default).
	var isSuper, bypassRLS bool
	if err := admin.QueryRow(ctx, "SELECT rolsuper, rolbypassrls FROM pg_roles WHERE rolname=$1", lowPrivRole).Scan(&isSuper, &bypassRLS); err != nil {
		t.Fatalf("check role attributes: %v", err)
	}
	if isSuper || bypassRLS {
		t.Fatalf("%s must not be superuser/bypassrls for RLS tests to be meaningful (super=%v bypassrls=%v)", lowPrivRole, isSuper, bypassRLS)
	}
}

// lowPrivConn returns a connection authenticated as the dedicated low-privilege role.
func lowPrivConn(t *testing.T, ctx context.Context, admin *pgx.Conn) *pgx.Conn {
	t.Helper()
	ensureLowPrivRole(t, ctx, admin)
	cfg := admin.Config().Copy()
	cfg.User = lowPrivRole
	cfg.Password = lowPrivPassword
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("connect as %s: %v", lowPrivRole, err)
	}
	t.Cleanup(func() { conn.Close(context.Background()) })
	return conn
}

// newUUID returns a fresh, random UUIDv4 string for test rows that need an explicit id
// (to wire up FK relationships deterministically rather than relying on gen_random_uuid()'s
// server-generated default and a round trip to read it back). Avoids an extra dependency.
func newUUID(t *testing.T) string {
	t.Helper()
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		t.Fatalf("generate uuid: %v", err)
	}
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant 10
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// isFKViolation reports whether err is a Postgres foreign_key_violation (SQLSTATE 23503).
func isFKViolation(err error) bool { return pgErrCode(err) == "23503" }

// isUniqueViolation reports whether err is a Postgres unique_violation (SQLSTATE 23505).
func isUniqueViolation(err error) bool { return pgErrCode(err) == "23505" }

func pgErrCode(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
