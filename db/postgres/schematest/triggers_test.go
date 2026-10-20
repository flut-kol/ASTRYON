package schematest

import (
	"context"
	"testing"
	"time"
)

// TestUpdatedAtTriggerFires covers requirement (d): astryon_set_updated_at() must bump
// updated_at on UPDATE. Uses tenants (migration 0001) as the representative table.
func TestUpdatedAtTriggerFires(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	id := newUUID(t)
	var createdUpdated time.Time
	err := admin.QueryRow(ctx,
		`INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'Trigger Test') RETURNING updated_at`,
		id, testSlug("it-trg")).Scan(&createdUpdated)
	if err != nil {
		t.Fatalf("insert tenant: %v", err)
	}

	// Force a visible clock difference regardless of how fast the two statements run.
	if _, err := admin.Exec(ctx, "SELECT pg_sleep(0.02)"); err != nil {
		t.Fatalf("pg_sleep: %v", err)
	}

	var updatedAfter time.Time
	err = admin.QueryRow(ctx,
		`UPDATE tenants SET name = 'Trigger Test Renamed' WHERE id = $1 RETURNING updated_at`,
		id).Scan(&updatedAfter)
	if err != nil {
		t.Fatalf("update tenant: %v", err)
	}

	if !updatedAfter.After(createdUpdated) {
		t.Fatalf("updated_at did not advance on UPDATE: before=%v after=%v", createdUpdated, updatedAfter)
	}
}

// TestSoftDeleteSlugReuse covers requirement (e): tenants.slug has a partial unique index
// (WHERE deleted_at IS NULL), so a slug can be reused by a new row once the old row holding
// it is soft-deleted - but not while the old row is still active.
func TestSoftDeleteSlugReuse(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	slug := testSlug("it-softdel")
	idOld := newUUID(t)
	mustExec(t, ctx, admin, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'Original')`, idOld, slug)

	// Sanity: while idOld is active, the same slug is rejected (confirms the partial
	// unique index is actually doing something, not just assumed).
	idDupe := newUUID(t)
	_, err := admin.Exec(ctx, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'Dupe')`, idDupe, slug)
	if err == nil {
		t.Fatal("expected unique_violation inserting a second active tenant with the same slug, got no error")
	}
	if !isUniqueViolation(err) {
		t.Fatalf("expected SQLSTATE 23505 (unique_violation), got: %v (code=%q)", err, pgErrCode(err))
	}

	// Soft-delete the original.
	mustExec(t, ctx, admin, `UPDATE tenants SET deleted_at = now() WHERE id = $1`, idOld)

	// Now the slug is free again.
	idNew := newUUID(t)
	_, err = admin.Exec(ctx, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'Reincarnated')`, idNew, slug)
	if err != nil {
		t.Fatalf("expected slug reuse after soft-delete to succeed, got: %v", err)
	}

	// Both rows must still exist (soft delete, not hard delete), distinguishable by deleted_at.
	var oldDeletedAt *time.Time
	if err := admin.QueryRow(ctx, `SELECT deleted_at FROM tenants WHERE id = $1`, idOld).Scan(&oldDeletedAt); err != nil {
		t.Fatalf("select old tenant: %v", err)
	}
	if oldDeletedAt == nil {
		t.Error("old tenant row should have deleted_at set")
	}
	var newDeletedAt *time.Time
	if err := admin.QueryRow(ctx, `SELECT deleted_at FROM tenants WHERE id = $1`, idNew).Scan(&newDeletedAt); err != nil {
		t.Fatalf("select new tenant: %v", err)
	}
	if newDeletedAt != nil {
		t.Error("new tenant row should be active (deleted_at IS NULL)")
	}
}
