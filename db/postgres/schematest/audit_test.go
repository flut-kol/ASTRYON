package schematest

import (
	"context"
	"strings"
	"testing"
)

// TestAuditLogImmutable covers requirement (f): audit_log rows must be append-only. The
// astryon_audit_immutable() BEFORE UPDATE OR DELETE trigger (migration 0007) must reject
// both an UPDATE and a DELETE on an existing row. Runs as the admin/superuser connection
// deliberately: BEFORE-row triggers that RAISE EXCEPTION fire regardless of role privilege
// or RLS bypass status, so this is not an RLS test - it is testing the trigger itself.
func TestAuditLogImmutable(t *testing.T) {
	ctx := context.Background()
	admin := adminConn(t, ctx)

	tenantID := newUUID(t)
	mustExec(t, ctx, admin, `INSERT INTO tenants (id, slug, name) VALUES ($1,$2,'Audit Test Tenant')`, tenantID, testSlug("it-audit"))

	id := newUUID(t)
	mustExec(t, ctx, admin,
		`INSERT INTO audit_log (id, tenant_id, action, entity_type) VALUES ($1,$2,'test.create','test_entity')`,
		id, tenantID)

	_, err := admin.Exec(ctx, `UPDATE audit_log SET action = 'test.tampered' WHERE id = $1`, id)
	if err == nil {
		t.Error("expected UPDATE on audit_log to be rejected, got no error")
	} else if !strings.Contains(err.Error(), "append-only") {
		t.Errorf("expected 'append-only' error from astryon_audit_immutable(), got: %v", err)
	}

	_, err = admin.Exec(ctx, `DELETE FROM audit_log WHERE id = $1`, id)
	if err == nil {
		t.Error("expected DELETE on audit_log to be rejected, got no error")
	} else if !strings.Contains(err.Error(), "append-only") {
		t.Errorf("expected 'append-only' error from astryon_audit_immutable(), got: %v", err)
	}

	// The row must still exist, untouched, after both rejected attempts.
	var action string
	if err := admin.QueryRow(ctx, `SELECT action FROM audit_log WHERE id = $1`, id).Scan(&action); err != nil {
		t.Fatalf("select audit_log row after rejected mutations: %v", err)
	}
	if action != "test.create" {
		t.Errorf("audit_log row was mutated despite rejected UPDATE: action=%q", action)
	}
}
