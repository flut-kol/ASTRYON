package replay

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Cutover: POST /v1/replays/{id}/activate makes the replay's target processing_version the version readers
// (services/query-api) use by default for that (tenant, source). Table source_active_versions (migration 0010).
//
// Rules (decide):
//   - the replay row must belong to the principal's tenant and be COMPLETED, else 404 / 409 replay_not_completed;
//   - activating a version LOWER than the current active one is refused (409 version_regression): use ?rollback=true;
//   - activating the already active version is an idempotent no-op (previous_processing_version is kept);
//   - ?rollback=true undoes THIS replay's activation: it requires the replay's target to be the currently active
//     version and a recorded previous version, restores previous as active and records the rolled-back version as
//     the new previous (so re-activating the replay is a normal, higher activation). Else 409 rollback_unavailable.

const StatusCompleted = "COMPLETED"

var (
	ErrNotFound          = errors.New("replay not found")
	ErrNotCompleted      = errors.New("replay is not COMPLETED")
	ErrRegression        = errors.New("target version is lower than the active version")
	ErrRollbackNothing   = errors.New("rollback unavailable")
	ErrActivationInvalid = errors.New("invalid activation request")
	// ErrAuditUnavailable: the audit event could not be made durable, so (required mode) the switch was NOT committed.
	ErrAuditUnavailable = errors.New("audit unavailable")
)

// PreCommit runs inside the activation transaction after the new state is written and before COMMIT (never for a
// noop). If it returns an error nothing is committed. It must be idempotent: a failed COMMIT afterwards leaves the
// caller to retry.
type PreCommit func(ctx context.Context, a Activation) error

// Activation is the result of a cutover or rollback.
type Activation struct {
	ReplayID                  string    `json:"replay_id"`
	SourceID                  string    `json:"source_id"`
	Action                    string    `json:"action"` // activate | rollback | noop
	ActiveProcessingVersion   int32     `json:"active_processing_version"`
	PreviousProcessingVersion *int32    `json:"previous_processing_version"`
	SwitchedAt                time.Time `json:"switched_at"`
}

// Activator performs the cutover. tenantID must be the verified principal's tenant; actor is the principal subject (audit only).
// pre may be nil.
type Activator interface {
	Activate(ctx context.Context, tenantID, replayID string, rollback bool, actor string, pre PreCommit) (Activation, error)
}

// replayRow / activeRow are the facts decide works on.
type replayRow struct {
	SourceID string
	Version  int32
	Status   string
}
type activeRow struct {
	Active   int32
	Previous *int32
}

// decide is the pure cutover rule shared by the PostgreSQL and in-memory activators. cur is nil when the source has no row yet.
func decide(r replayRow, cur *activeRow, rollback bool) (action string, active int32, prev *int32, err error) {
	if rollback {
		if cur == nil || cur.Active != r.Version || cur.Previous == nil {
			return "", 0, nil, ErrRollbackNothing
		}
		old := cur.Active
		return "rollback", *cur.Previous, &old, nil
	}
	if r.Status != StatusCompleted {
		return "", 0, nil, ErrNotCompleted
	}
	if cur == nil {
		return "activate", r.Version, nil, nil
	}
	switch {
	case r.Version < cur.Active:
		return "", 0, nil, ErrRegression
	case r.Version == cur.Active:
		return "noop", cur.Active, cur.Previous, nil
	}
	old := cur.Active
	return "activate", r.Version, &old, nil
}

const (
	sqlReplayShare = `SELECT source_id::text, target_processing_version, status::text FROM replay_requests
WHERE id = $1::uuid AND tenant_id = $2::uuid AND deleted_at IS NULL FOR SHARE`
	// Different hash namespace than allocation (sqlLock) so cutover never blocks version allocation.
	sqlActLock = `SELECT pg_advisory_xact_lock(hashtextextended('active:' || $1::text || $2::text, 0))`
	sqlCurrent = `SELECT active_processing_version, previous_processing_version FROM source_active_versions
WHERE tenant_id = $1::uuid AND source_id = $2::uuid FOR UPDATE`
	sqlUpsert = `INSERT INTO source_active_versions (tenant_id, source_id, active_processing_version, previous_processing_version, switched_by)
VALUES ($1::uuid, $2::uuid, $3, $4, $5)
ON CONFLICT (tenant_id, source_id) DO UPDATE SET active_processing_version = EXCLUDED.active_processing_version,
  previous_processing_version = EXCLUDED.previous_processing_version, switched_at = now(), switched_by = EXCLUDED.switched_by
RETURNING switched_at`
	sqlSwitchedAt = `SELECT switched_at FROM source_active_versions WHERE tenant_id = $1::uuid AND source_id = $2::uuid`
)

// PGActivator is the PostgreSQL Activator: one transaction = RLS tenant -> replay row FOR SHARE (blocks status changes)
// -> per-(tenant, source) advisory lock -> current row FOR UPDATE -> decide -> upsert. Concurrent activations
// serialize on the advisory lock, so exactly one row per source remains and the regression rule is race-free.
type PGActivator struct{ DB DB }

func (a *PGActivator) Activate(ctx context.Context, tenantID, replayID string, rollback bool, actor string, pre PreCommit) (Activation, error) {
	if !uuidRe.MatchString(tenantID) {
		return Activation{}, fmt.Errorf("%w: tenant", ErrActivationInvalid)
	}
	if !uuidRe.MatchString(replayID) {
		return Activation{}, ErrNotFound
	}
	tx, err := a.DB.Begin(ctx)
	if err != nil {
		return Activation{}, err
	}
	defer tx.Rollback(ctx) // no-op after Commit
	if err := tx.Exec(ctx, sqlSetTenant, tenantID); err != nil {
		return Activation{}, err
	}
	var r replayRow
	if err := tx.QueryRow(ctx, sqlReplayShare, replayID, tenantID).Scan(&r.SourceID, &r.Version, &r.Status); err != nil {
		if isNoRows(err) {
			return Activation{}, ErrNotFound
		}
		return Activation{}, err
	}
	if err := tx.Exec(ctx, sqlActLock, tenantID, r.SourceID); err != nil {
		return Activation{}, err
	}
	var cur *activeRow
	var c activeRow
	switch err := tx.QueryRow(ctx, sqlCurrent, tenantID, r.SourceID).Scan(&c.Active, &c.Previous); {
	case err == nil:
		cur = &c
	case !isNoRows(err):
		return Activation{}, err
	}
	action, active, prev, err := decide(r, cur, rollback)
	if err != nil {
		return Activation{}, err
	}
	out := Activation{ReplayID: replayID, SourceID: r.SourceID, Action: action, ActiveProcessingVersion: active, PreviousProcessingVersion: prev}
	if action == "noop" {
		if err := tx.QueryRow(ctx, sqlSwitchedAt, tenantID, r.SourceID).Scan(&out.SwitchedAt); err != nil {
			return Activation{}, err
		}
	} else {
		var pv any
		if prev != nil {
			pv = *prev
		}
		if err := tx.QueryRow(ctx, sqlUpsert, tenantID, r.SourceID, active, pv, nullable(actor)).Scan(&out.SwitchedAt); err != nil {
			return Activation{}, err
		}
	}
	if pre != nil && action != "noop" {
		if err := pre(ctx, out); err != nil {
			return Activation{}, err // deferred Rollback: the switch is not committed
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return Activation{}, err
	}
	return out, nil
}

func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// ---- in-memory fake (tests and single-process dev; same rules through decide) ----

// MemReplay is a replay row known to MemActivator.
type MemReplay struct {
	TenantID, SourceID, Status string
	Version                    int32
}

// MemActivator is the in-memory Activator fake.
type MemActivator struct {
	mu      sync.Mutex
	Replays map[string]MemReplay // by replay id
	active  map[[2]string]activeRow
	Now     func() time.Time
	// CommitErr, when set, simulates a failing COMMIT: pre has run, nothing is stored (tests).
	CommitErr error
}

// Active returns the stored row for tests.
func (m *MemActivator) Active(tenantID, sourceID string) (active int32, previous *int32, ok bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.active[[2]string{tenantID, sourceID}]
	return r.Active, r.Previous, ok
}

func (m *MemActivator) Activate(_ context.Context, tenantID, replayID string, rollback bool, _ string, pre PreCommit) (Activation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rp, ok := m.Replays[replayID]
	if !ok || rp.TenantID != tenantID { // another tenant's replay looks exactly like a missing one
		return Activation{}, ErrNotFound
	}
	if m.active == nil {
		m.active = map[[2]string]activeRow{}
	}
	k := [2]string{tenantID, rp.SourceID}
	var cur *activeRow
	if c, ok := m.active[k]; ok {
		cur = &c
	}
	action, active, prev, err := decide(replayRow{rp.SourceID, rp.Version, rp.Status}, cur, rollback)
	if err != nil {
		return Activation{}, err
	}
	now := time.Now
	if m.Now != nil {
		now = m.Now
	}
	out := Activation{ReplayID: replayID, SourceID: rp.SourceID, Action: action, ActiveProcessingVersion: active,
		PreviousProcessingVersion: prev, SwitchedAt: now().UTC()}
	if action != "noop" {
		if pre != nil {
			if err := pre(context.Background(), out); err != nil {
				return Activation{}, err
			}
		}
		if m.CommitErr != nil {
			return Activation{}, m.CommitErr
		}
		m.active[k] = activeRow{Active: active, Previous: prev}
	}
	return out, nil
}
