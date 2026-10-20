-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE processing_rules, pipeline_versions, pipelines CASCADE; DROP TYPE pipeline_version_state.
--   Prod: new forward migration; processing_version values referenced in ClickHouse must stay resolvable.
-- Group: pipelines and versioned processing rules.

CREATE TYPE pipeline_version_state AS ENUM ('DRAFT','ACTIVE','RETIRED');

CREATE TABLE pipelines (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  source_id   uuid NOT NULL,
  name        text NOT NULL,
  description text,
  is_enabled  boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now(),
  updated_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX pipelines_name_uq ON pipelines (tenant_id, name) WHERE deleted_at IS NULL;
CREATE INDEX pipelines_source_ix ON pipelines (tenant_id, source_id);
CREATE TRIGGER pipelines_updated BEFORE UPDATE ON pipelines FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

-- Immutable once ACTIVE: version = processing_version / pipeline_version recorded on normalized events.
CREATE TABLE pipeline_versions (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  pipeline_id uuid NOT NULL,
  version     integer NOT NULL CHECK (version > 0),
  state       pipeline_version_state NOT NULL DEFAULT 'DRAFT',
  source_schema_id uuid REFERENCES source_schemas(id),
  definition  jsonb NOT NULL DEFAULT '{}',
  created_by  uuid,
  created_at  timestamptz NOT NULL DEFAULT now(),
  activated_at timestamptz,
  retired_at  timestamptz,
  FOREIGN KEY (tenant_id, pipeline_id) REFERENCES pipelines (tenant_id, id),
  UNIQUE (tenant_id, pipeline_id, version),
  UNIQUE (tenant_id, id)
);
CREATE UNIQUE INDEX pipeline_versions_active_uq ON pipeline_versions (tenant_id, pipeline_id) WHERE state = 'ACTIVE';
CREATE INDEX pipeline_versions_schema_ix ON pipeline_versions (source_schema_id);

CREATE TABLE processing_rules (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  pipeline_version_id uuid NOT NULL,
  ordinal     integer NOT NULL CHECK (ordinal >= 0),
  rule_type   text NOT NULL,                 -- e.g. map, validate, mask_pii, tokenize, filter
  config      jsonb NOT NULL DEFAULT '{}',
  is_enabled  boolean NOT NULL DEFAULT true,
  created_at  timestamptz NOT NULL DEFAULT now(),
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, pipeline_version_id) REFERENCES pipeline_versions (tenant_id, id)
);
CREATE UNIQUE INDEX processing_rules_ord_uq ON processing_rules (tenant_id, pipeline_version_id, ordinal) WHERE deleted_at IS NULL;

SELECT astryon_enable_rls('pipelines');
SELECT astryon_enable_rls('pipeline_versions');
SELECT astryon_enable_rls('processing_rules');
