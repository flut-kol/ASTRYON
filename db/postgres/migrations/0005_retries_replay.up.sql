-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE replay_requests, scheduled_retries CASCADE; DROP TYPE replay_status.
--   Prod: new forward migration.
-- Group: retries and replay. Replay goes to a NEW processing_version so aggregates are not doubled (§5.3).

CREATE TYPE replay_status AS ENUM ('REQUESTED','APPROVED','RUNNING','COMPLETED','FAILED','CANCELLED');

CREATE TABLE scheduled_retries (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  job_id      uuid NOT NULL,
  retry_at    timestamptz NOT NULL,
  reason      text,
  backoff_ms  bigint,
  created_at  timestamptz NOT NULL DEFAULT now(),
  fired_at    timestamptz,
  cancelled_at timestamptz,
  FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, id)
);
CREATE INDEX scheduled_retries_due_ix ON scheduled_retries (retry_at) WHERE fired_at IS NULL AND cancelled_at IS NULL;
CREATE INDEX scheduled_retries_job_ix ON scheduled_retries (tenant_id, job_id);

CREATE TABLE replay_requests (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  pipeline_id uuid,
  target_pipeline_version_id uuid,
  target_processing_version integer NOT NULL CHECK (target_processing_version > 0),
  time_from   timestamptz,
  time_to     timestamptz,
  batch_ids   uuid[],
  reason      text,
  status      replay_status NOT NULL DEFAULT 'REQUESTED',
  requested_by uuid,
  approved_by uuid,
  job_id      uuid,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  FOREIGN KEY (tenant_id, pipeline_id) REFERENCES pipelines (tenant_id, id),
  FOREIGN KEY (tenant_id, target_pipeline_version_id) REFERENCES pipeline_versions (tenant_id, id),
  FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, id),
  FOREIGN KEY (tenant_id, requested_by) REFERENCES principals (tenant_id, id),
  FOREIGN KEY (tenant_id, approved_by) REFERENCES principals (tenant_id, id),
  CHECK (time_to IS NULL OR time_from IS NULL OR time_to > time_from)
);
CREATE INDEX replay_requests_source_ix ON replay_requests (tenant_id, source_id, created_at DESC);
CREATE INDEX replay_requests_status_ix ON replay_requests (tenant_id, status) WHERE status IN ('REQUESTED','APPROVED','RUNNING');
CREATE INDEX replay_requests_job_ix ON replay_requests (tenant_id, job_id);
CREATE INDEX replay_requests_pv_ix ON replay_requests (tenant_id, target_pipeline_version_id);
CREATE TRIGGER replay_requests_updated BEFORE UPDATE ON replay_requests FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('scheduled_retries');
SELECT astryon_enable_rls('replay_requests');
