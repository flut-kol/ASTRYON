-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE job_attempts, jobs, file_objects, ingestion_batches CASCADE; DROP TYPE job_status, batch_status.
--   Prod: new forward migration. NOTE: there is intentionally NO per-event status table; only batch/file/job metadata and aggregates.
-- Group: ingestion batches, file objects, jobs.

CREATE TYPE batch_status AS ENUM ('OPEN','SEALED','PROCESSING','COMPLETED','FAILED','CANCELLED');
CREATE TYPE job_status   AS ENUM ('NEW','QUEUED','RUNNING','RETRYING','SUCCEEDED','FAILED','CANCELLED');

CREATE TABLE ingestion_batches (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  connector_run_id uuid REFERENCES connector_runs(id),
  external_batch_id text,                    -- producer-supplied id (idempotency)
  status      batch_status NOT NULL DEFAULT 'OPEN',
  -- aggregates only
  events_accepted bigint NOT NULL DEFAULT 0,
  events_rejected bigint NOT NULL DEFAULT 0,
  bytes_total bigint NOT NULL DEFAULT 0,
  rejection_samples jsonb,                   -- small bounded sample of rejected events (deviations), not a full log
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  sealed_at   timestamptz,
  completed_at timestamptz,
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX ingestion_batches_ext_uq ON ingestion_batches (tenant_id, source_id, external_batch_id) WHERE external_batch_id IS NOT NULL;
CREATE INDEX ingestion_batches_source_ix ON ingestion_batches (tenant_id, source_id, created_at DESC);
CREATE INDEX ingestion_batches_status_ix ON ingestion_batches (tenant_id, status) WHERE status IN ('OPEN','SEALED','PROCESSING');
CREATE INDEX ingestion_batches_run_ix ON ingestion_batches (connector_run_id);
CREATE TRIGGER ingestion_batches_updated BEFORE UPDATE ON ingestion_batches FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE file_objects (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  batch_id    uuid,
  source_id   uuid NOT NULL,
  bucket      text NOT NULL,
  object_key  text NOT NULL,
  object_version text,
  size_bytes  bigint CHECK (size_bytes >= 0),
  content_type text,
  checksum_sha256 text,
  committed_at timestamptz,                  -- set only after S3 commit completed (see §5.3)
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  FOREIGN KEY (tenant_id, batch_id) REFERENCES ingestion_batches (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX file_objects_loc_uq ON file_objects (tenant_id, bucket, object_key, COALESCE(object_version, ''));
CREATE INDEX file_objects_batch_ix ON file_objects (tenant_id, batch_id);
CREATE INDEX file_objects_source_ix ON file_objects (tenant_id, source_id, created_at DESC);

CREATE TABLE jobs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  kind        text NOT NULL,                 -- e.g. process_file, process_batch, replay
  status      job_status NOT NULL DEFAULT 'NEW',
  priority    smallint NOT NULL DEFAULT 0,
  source_id   uuid,
  batch_id    uuid,
  file_object_id uuid,
  pipeline_version_id uuid,
  idempotency_key text,
  payload     jsonb NOT NULL DEFAULT '{}',
  attempt_count integer NOT NULL DEFAULT 0,
  max_attempts integer NOT NULL DEFAULT 5 CHECK (max_attempts > 0),
  run_after   timestamptz NOT NULL DEFAULT now(),
  locked_by   text,
  locked_until timestamptz,
  events_processed bigint NOT NULL DEFAULT 0, -- aggregates
  events_failed bigint NOT NULL DEFAULT 0,
  last_error  text,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  started_at  timestamptz,
  finished_at timestamptz,
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  FOREIGN KEY (tenant_id, batch_id) REFERENCES ingestion_batches (tenant_id, id),
  FOREIGN KEY (tenant_id, file_object_id) REFERENCES file_objects (tenant_id, id),
  FOREIGN KEY (tenant_id, pipeline_version_id) REFERENCES pipeline_versions (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX jobs_idem_uq ON jobs (tenant_id, kind, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX jobs_claim_ix ON jobs (run_after, priority DESC) WHERE status IN ('QUEUED','RETRYING');
CREATE INDEX jobs_tenant_status_ix ON jobs (tenant_id, status, created_at DESC);
CREATE INDEX jobs_batch_ix ON jobs (tenant_id, batch_id);
CREATE INDEX jobs_file_ix ON jobs (tenant_id, file_object_id);
CREATE INDEX jobs_pipeline_version_ix ON jobs (tenant_id, pipeline_version_id);
CREATE INDEX jobs_lease_ix ON jobs (locked_until) WHERE status = 'RUNNING';
CREATE TRIGGER jobs_updated BEFORE UPDATE ON jobs FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE job_attempts (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  job_id      uuid NOT NULL,
  attempt_no  integer NOT NULL CHECK (attempt_no > 0),
  status      job_status NOT NULL DEFAULT 'RUNNING',
  worker_id   text,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  error_code  text,
  error_message text,
  FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, id),
  UNIQUE (tenant_id, job_id, attempt_no)
);

SELECT astryon_enable_rls('ingestion_batches');
SELECT astryon_enable_rls('file_objects');
SELECT astryon_enable_rls('jobs');
SELECT astryon_enable_rls('job_attempts');
