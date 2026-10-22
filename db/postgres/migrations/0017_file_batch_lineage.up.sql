-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE file_batch_lineage.
--   Prod: ship a forward migration instead; deleting this table removes the only durable
--   file batch -> output trace and must follow a backup/retention decision.
-- Group: file-ingestion durable batch lineage (no per-event status).

CREATE TABLE file_batch_lineage (
  tenant_id       uuid NOT NULL REFERENCES tenants(id),
  batch_id        text NOT NULL,
  job_id          uuid NOT NULL,
  source_file_id  uuid NOT NULL,
  source_checksum text NOT NULL,
  pipeline_version text NOT NULL,
  destination     text NOT NULL CHECK (destination IN ('PARQUET_S3','CLICKHOUSE_BULK')),
  first_row       bigint NOT NULL CHECK (first_row >= 1),
  last_row        bigint NOT NULL CHECK (last_row >= first_row),
  valid_rows      integer NOT NULL CHECK (valid_rows >= 0),
  invalid_rows    integer NOT NULL CHECK (invalid_rows >= 0),
  output_key      text NOT NULL DEFAULT '',
  created_at      timestamptz NOT NULL DEFAULT now(),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, batch_id),
  FOREIGN KEY (tenant_id, job_id) REFERENCES jobs (tenant_id, id),
  FOREIGN KEY (tenant_id, source_file_id) REFERENCES file_objects (tenant_id, id)
);
CREATE INDEX file_batch_lineage_job_ix ON file_batch_lineage (tenant_id, job_id, first_row);
CREATE INDEX file_batch_lineage_source_ix ON file_batch_lineage (tenant_id, source_file_id, first_row);
CREATE TRIGGER file_batch_lineage_updated BEFORE UPDATE ON file_batch_lineage FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('file_batch_lineage');
