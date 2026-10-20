-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE connector_runs, connectors, source_schemas, source_credentials, sources CASCADE;
--   DROP TYPE connector_run_status, source_kind. Prod: new forward migration + PITR restore if data loss.
-- Group: sources and connectors.

CREATE TYPE source_kind AS ENUM ('kafka','http_push','s3_files','sftp','database','api_pull');
CREATE TYPE connector_run_status AS ENUM ('RUNNING','SUCCEEDED','FAILED','CANCELLED');

CREATE TABLE sources (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  name        text NOT NULL,
  kind        source_kind NOT NULL,
  config      jsonb NOT NULL DEFAULT '{}',   -- non-secret config only
  is_enabled  boolean NOT NULL DEFAULT true,
  created_by  uuid,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX sources_tenant_name_uq ON sources (tenant_id, name) WHERE deleted_at IS NULL;
CREATE TRIGGER sources_updated BEFORE UPDATE ON sources FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE source_credentials (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  name        text NOT NULL,
  secret_ref  text NOT NULL,                 -- reference into secret manager (e.g. vault://path#key). NEVER a secret value.
  secret_version text,
  rotated_at  timestamptz,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  CHECK (secret_ref ~ '^[a-z0-9+.-]+://')
);
CREATE UNIQUE INDEX source_credentials_name_uq ON source_credentials (tenant_id, source_id, name) WHERE deleted_at IS NULL;
CREATE TRIGGER source_credentials_updated BEFORE UPDATE ON source_credentials FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE source_schemas (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  version     integer NOT NULL CHECK (version > 0),
  format      text NOT NULL DEFAULT 'json_schema',
  schema_def  jsonb NOT NULL,
  is_active   boolean NOT NULL DEFAULT false,
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  UNIQUE (tenant_id, source_id, version)
);
CREATE UNIQUE INDEX source_schemas_active_uq ON source_schemas (tenant_id, source_id) WHERE is_active AND deleted_at IS NULL;

CREATE TABLE connectors (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  credential_id uuid REFERENCES source_credentials(id),
  name        text NOT NULL,
  connector_type text NOT NULL,
  schedule_cron text,
  config      jsonb NOT NULL DEFAULT '{}',
  is_enabled  boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX connectors_name_uq ON connectors (tenant_id, source_id, name) WHERE deleted_at IS NULL;
CREATE INDEX connectors_cred_ix ON connectors (credential_id);
CREATE TRIGGER connectors_updated BEFORE UPDATE ON connectors FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

-- Run-level metadata and aggregate counters only; never per-event rows.
CREATE TABLE connector_runs (
  id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  connector_id uuid NOT NULL,
  status       connector_run_status NOT NULL DEFAULT 'RUNNING',
  started_at   timestamptz NOT NULL DEFAULT now(),
  finished_at  timestamptz,
  cursor_state jsonb,
  records_read bigint NOT NULL DEFAULT 0,
  bytes_read   bigint NOT NULL DEFAULT 0,
  error_code   text,
  error_message text,
  FOREIGN KEY (tenant_id, connector_id) REFERENCES connectors (tenant_id, id)
);
CREATE INDEX connector_runs_connector_ix ON connector_runs (tenant_id, connector_id, started_at DESC);
CREATE INDEX connector_runs_running_ix ON connector_runs (tenant_id, status) WHERE status = 'RUNNING';
CREATE INDEX sources_created_by_ix ON sources (created_by);

SELECT astryon_enable_rls('sources');
SELECT astryon_enable_rls('source_credentials');
SELECT astryon_enable_rls('source_schemas');
SELECT astryon_enable_rls('connectors');
SELECT astryon_enable_rls('connector_runs');
