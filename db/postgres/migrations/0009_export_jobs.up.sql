-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE export_jobs CASCADE.
--   Prod: new forward migration; objects already written under exports/<tenant_id>/ expire via the S3 lifecycle rule.
-- NUMBERING: follows 0008_tenant_scoped_fks (copied verbatim from branch flut_kol/ASTRYON). If that branch adds its own 0009+ before merge, renumber this file.
-- Group: export jobs (services/export-worker). Status is per export job, never per event.
-- Leases: a replica that owns a non-terminal job (locked_by) must keep lease_expires_at in the future
--   (heartbeat). Dispatcher flow: Enqueue (QUEUED, unowned) -> Claim (FOR UPDATE SKIP LOCKED; still QUEUED, owned,
--   started_at NULL) -> Start (RUNNING, started_at set) -> Finish. Recovery of an expired lease:
--   claimed-but-not-started (QUEUED, started_at NULL) -> back to QUEUED, attempts + 1 (max 3 attempts, then FAILED
--   'max_attempts'); started (RUNNING) -> FAILED 'worker_lost', never re-run (it may have written output).
-- NOTE: `attempts` was added to this still-unreleased, untracked migration in place (no further migration).

CREATE TABLE export_jobs (
  id          uuid PRIMARY KEY,
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  requested_by text,
  status      text NOT NULL DEFAULT 'QUEUED' CHECK (status IN ('QUEUED','RUNNING','SUCCEEDED','FAILED','CANCELLED')),
  spec        jsonb NOT NULL,
  spec_hash   text NOT NULL,
  object_key  text,
  rows        bigint NOT NULL DEFAULT 0 CHECK (rows >= 0),
  bytes       bigint NOT NULL DEFAULT 0 CHECK (bytes >= 0),
  error_code  text,
  attempts    int NOT NULL DEFAULT 0 CHECK (attempts >= 0),  -- claims lost before start (re-queues)
  locked_by   text,                      -- worker/replica id that owns the job while non-terminal
  lease_expires_at timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz,
  expires_at  timestamptz,
  deleted_at  timestamptz,
  updated_at  timestamptz NOT NULL DEFAULT now(),
  UNIQUE (tenant_id, id)
);
CREATE INDEX export_jobs_tenant_created_ix ON export_jobs (tenant_id, created_at DESC);
CREATE INDEX export_jobs_claimable_ix ON export_jobs (created_at) WHERE status = 'QUEUED';
CREATE INDEX export_jobs_active_ix ON export_jobs (tenant_id) WHERE status IN ('QUEUED','RUNNING');
CREATE INDEX export_jobs_lease_ix ON export_jobs (lease_expires_at) WHERE status IN ('QUEUED','RUNNING');
CREATE TRIGGER export_jobs_updated BEFORE UPDATE ON export_jobs FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('export_jobs');
