-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE tenant_quotas, sink_checkpoints, sink_configs CASCADE; DROP TYPE sink_kind.
--   Prod: new forward migration; checkpoints loss means sink re-reads from last committed offset (idempotent sink tolerates this).
-- Group: sinks and quotas.

CREATE TYPE sink_kind AS ENUM ('clickhouse','s3','kafka');

CREATE TABLE sink_configs (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  pipeline_id uuid NOT NULL,
  name        text NOT NULL,
  kind        sink_kind NOT NULL,
  config      jsonb NOT NULL DEFAULT '{}',   -- non-secret settings; credentials via secret refs in config
  is_enabled  boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, pipeline_id) REFERENCES pipelines (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX sink_configs_name_uq ON sink_configs (tenant_id, pipeline_id, name) WHERE deleted_at IS NULL;
CREATE TRIGGER sink_configs_updated BEFORE UPDATE ON sink_configs FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

-- Per-partition/batch commit position (e.g. Kafka partition offset or batch id), NOT per event.
CREATE TABLE sink_checkpoints (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  sink_id     uuid NOT NULL,
  checkpoint_key text NOT NULL,              -- e.g. 'topic/partition' or 'batch:<uuid>'
  processing_version integer NOT NULL DEFAULT 1,
  position    text NOT NULL,
  rows_committed bigint NOT NULL DEFAULT 0,
  insert_token text,                         -- last ClickHouse insert_deduplication_token used
  updated_at  timestamptz NOT NULL DEFAULT now(),
  FOREIGN KEY (tenant_id, sink_id) REFERENCES sink_configs (tenant_id, id),
  UNIQUE (tenant_id, sink_id, checkpoint_key, processing_version)
);
CREATE TRIGGER sink_checkpoints_updated BEFORE UPDATE ON sink_checkpoints FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

CREATE TABLE tenant_quotas (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  resource    text NOT NULL,                 -- e.g. events_per_day, bytes_per_day, sources, api_requests_per_min
  limit_value bigint NOT NULL CHECK (limit_value >= 0),
  period      text NOT NULL DEFAULT 'day' CHECK (period IN ('minute','hour','day','month','total')),
  is_hard     boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz
);
CREATE UNIQUE INDEX tenant_quotas_uq ON tenant_quotas (tenant_id, resource, period) WHERE deleted_at IS NULL;
CREATE TRIGGER tenant_quotas_updated BEFORE UPDATE ON tenant_quotas FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('sink_configs');
SELECT astryon_enable_rls('sink_checkpoints');
SELECT astryon_enable_rls('tenant_quotas');
