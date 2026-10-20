package schematest

import (
	"context"
	"testing"
)

// allEntityTables lists the ~25 §6 control-plane entity tables (excludes the
// bookkeeping-only schema_migrations table created by db/apply.sh).
var allEntityTables = []string{
	"tenants", "principals", "roles", "role_bindings", "api_keys",
	"sources", "source_credentials", "source_schemas", "connectors", "connector_runs",
	"pipelines", "pipeline_versions", "processing_rules",
	"ingestion_batches", "file_objects", "jobs", "job_attempts",
	"scheduled_retries", "replay_requests",
	"sink_configs", "sink_checkpoints", "tenant_quotas",
	"audit_log", "outbox", "notifications",
}

// TestAllEntityTablesExistWithPrimaryKeys covers requirement (a): every §6 entity table
// exists and has a primary key.
func TestAllEntityTablesExistWithPrimaryKeys(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	if n := len(allEntityTables); n != 25 {
		t.Fatalf("test fixture bug: allEntityTables has %d entries, want 25", n)
	}

	for _, tbl := range allEntityTables {
		var hasPK bool
		err := admin.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = $1::regclass AND contype = 'p'
			)`, tbl).Scan(&hasPK)
		if err != nil {
			t.Errorf("table %q: query failed (likely missing from schema): %v", tbl, err)
			continue
		}
		if !hasPK {
			t.Errorf("table %q: exists but has no primary key", tbl)
		}
	}
}

// TestForeignKeySpotChecks covers requirement (a)'s FK-graph spot checks, read from the
// actual migration SQL (not assumed), plus regression coverage for the tenant-scope FK gap
// fixed by migration 0008 (see db/postgres/migrations/0008_tenant_scoped_fks.up.sql).
func TestForeignKeySpotChecks(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	cases := []struct {
		name    string
		table   string
		likeDef string
	}{
		{"principals.tenant_id -> tenants.id", "principals", "%REFERENCES tenants(id)%"},
		{"connector_runs.connector_id -> connectors.id (tenant-scoped)", "connector_runs",
			"%(tenant_id, connector_id) REFERENCES connectors(tenant_id, id)%"},
		{"job_attempts.job_id -> jobs.id (tenant-scoped)", "job_attempts",
			"%(tenant_id, job_id) REFERENCES jobs(tenant_id, id)%"},
		{"pipeline_versions.pipeline_id -> pipelines.id (tenant-scoped)", "pipeline_versions",
			"%(tenant_id, pipeline_id) REFERENCES pipelines(tenant_id, id)%"},
		// 0008 bug-fix regression: these three were single-column (not tenant-scoped) FKs
		// before 0008 and must now be composite (tenant_id, <col>) FKs.
		{"connectors.credential_id -> source_credentials.id (0008 fix)", "connectors",
			"%(tenant_id, credential_id) REFERENCES source_credentials(tenant_id, id)%"},
		{"pipeline_versions.source_schema_id -> source_schemas.id (0008 fix)", "pipeline_versions",
			"%(tenant_id, source_schema_id) REFERENCES source_schemas(tenant_id, id)%"},
		{"ingestion_batches.connector_run_id -> connector_runs.id (0008 fix)", "ingestion_batches",
			"%(tenant_id, connector_run_id) REFERENCES connector_runs(tenant_id, id)%"},
	}

	for _, c := range cases {
		var found bool
		err := admin.QueryRow(ctx, `
			SELECT EXISTS (
				SELECT 1 FROM pg_constraint
				WHERE conrelid = $1::regclass AND contype = 'f'
				  AND pg_get_constraintdef(oid) LIKE $2
			)`, c.table, c.likeDef).Scan(&found)
		if err != nil {
			t.Fatalf("%s: query failed: %v", c.name, err)
		}
		if !found {
			t.Errorf("%s: no matching FK found on %q (pattern %q)", c.name, c.table, c.likeDef)
		}
	}
}

// TestForeignKeyEnforcement covers requirement (b): inserting a row referencing a
// non-existent parent id must fail with SQLSTATE 23503 (foreign_key_violation).
func TestForeignKeyEnforcement(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	ghost := newUUID(t) // deliberately never inserted anywhere

	cases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "principal with non-existent tenant_id",
			sql:  `INSERT INTO principals (id, tenant_id, aas_sub) VALUES ($1, $2, 'ghost')`,
			args: []any{newUUID(t), ghost},
		},
		{
			name: "job_attempt with non-existent (tenant_id, job_id)",
			sql:  `INSERT INTO job_attempts (id, tenant_id, job_id, attempt_no) VALUES ($1, $2, $3, 1)`,
			args: []any{newUUID(t), ghost, newUUID(t)},
		},
	}

	for _, c := range cases {
		_, err := admin.Exec(ctx, c.sql, c.args...)
		if err == nil {
			t.Errorf("%s: expected foreign_key_violation, got no error", c.name)
			continue
		}
		if !isFKViolation(err) {
			t.Errorf("%s: expected SQLSTATE 23503 (foreign_key_violation), got: %v (code=%q)", c.name, err, pgErrCode(err))
		}
	}
}
