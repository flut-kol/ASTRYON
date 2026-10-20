package schematest

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
)

// repoApplyScript locates db/apply.sh relative to this test file (db/postgres/schematest/),
// independent of the working directory `go test` happens to run from.
func repoApplyScript(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	// .../db/postgres/schematest/apply_test.go -> apply.sh is two directories up, in db/.
	return filepath.Join(filepath.Dir(file), "..", "..", "apply.sh")
}

// TestApplyIsIdempotent covers requirement (g): re-running db/apply.sh postgres against an
// already-migrated database must not error and must not re-apply (or double-count) any
// migration. Requires `psql` on PATH (db/apply.sh's own dependency); skips cleanly if
// missing rather than failing the whole suite over an environment gap unrelated to the
// schema itself.
func TestApplyIsIdempotent(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)
	dsn := os.Getenv("DATABASE_URL") // non-empty: adminConn() above already skipped otherwise.

	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not found in PATH; db/apply.sh shells out to it - install the postgresql client to run this test")
	}

	var before int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&before); err != nil {
		t.Fatalf("count schema_migrations (before): %v", err)
	}
	if before == 0 {
		t.Fatal("schema_migrations is empty - apply migrations first (see this package's doc comment for setup commands)")
	}

	script := repoApplyScript(t)
	cmd := exec.Command(script, "postgres")
	cmd.Env = append(os.Environ(), "DATABASE_URL="+dsn)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("db/apply.sh postgres (re-run): %v\n%s", err, out)
	}

	var after int
	if err := admin.QueryRow(ctx, "SELECT count(*) FROM schema_migrations").Scan(&after); err != nil {
		t.Fatalf("count schema_migrations (after): %v", err)
	}
	if after != before {
		t.Fatalf("schema_migrations row count changed on idempotent re-apply: before=%d after=%d", before, after)
	}
}
