-- ROLLBACK PLAN: forward-only. Non-prod: drop the composite FKs this migration adds
--   (connectors_tenant_credential_fkey, pipeline_versions_tenant_source_schema_fkey,
--   ingestion_batches_tenant_connector_run_fkey) and the UNIQUE(tenant_id, id) constraints
--   added on source_credentials/source_schemas/connector_runs, then re-add the single-column
--   FKs dropped below (connectors_credential_id_fkey, pipeline_versions_source_schema_id_fkey,
--   ingestion_batches_connector_run_id_fkey) if reverting this fix is ever required.
--   Prod: new forward migration only; no data is deleted by this change (constraints are
--   re-validated against existing rows, which satisfy them already since the parent rows'
--   tenant_id always equals the child row's tenant_id in practice for all current data).
-- Group: bug fix - tenant-scope FK gap found while writing §6 integration tests.
--   connectors.credential_id -> source_credentials(id), pipeline_versions.source_schema_id ->
--   source_schemas(id), and ingestion_batches.connector_run_id -> connector_runs(id) were each
--   declared as single-column FKs referencing only the parent's PRIMARY KEY, unlike every other
--   cross-table reference in 0001-0007 which uses a composite (tenant_id, id) FK against a
--   composite UNIQUE(tenant_id, id) on the parent. A single-column FK only checks that the
--   referenced id exists *somewhere*, not that it belongs to the same tenant as the child row -
--   so e.g. a connector in tenant A could be made to reference a source_credentials row owned by
--   tenant B, silently violating the tenant-scope requirement in DEVELOPMENT_PLAN.md §6. This
--   migration adds the missing UNIQUE(tenant_id, id) constraints on the three parent tables and
--   replaces the three loose FKs with tenant-scoped composite FKs, matching the rest of the schema.

ALTER TABLE source_credentials ADD CONSTRAINT source_credentials_tenant_id_uq UNIQUE (tenant_id, id);
ALTER TABLE source_schemas ADD CONSTRAINT source_schemas_tenant_id_uq UNIQUE (tenant_id, id);
ALTER TABLE connector_runs ADD CONSTRAINT connector_runs_tenant_id_uq UNIQUE (tenant_id, id);

ALTER TABLE connectors DROP CONSTRAINT connectors_credential_id_fkey;
ALTER TABLE connectors ADD CONSTRAINT connectors_tenant_credential_fkey
  FOREIGN KEY (tenant_id, credential_id) REFERENCES source_credentials (tenant_id, id);

ALTER TABLE pipeline_versions DROP CONSTRAINT pipeline_versions_source_schema_id_fkey;
ALTER TABLE pipeline_versions ADD CONSTRAINT pipeline_versions_tenant_source_schema_fkey
  FOREIGN KEY (tenant_id, source_schema_id) REFERENCES source_schemas (tenant_id, id);

ALTER TABLE ingestion_batches DROP CONSTRAINT ingestion_batches_connector_run_id_fkey;
ALTER TABLE ingestion_batches ADD CONSTRAINT ingestion_batches_tenant_connector_run_fkey
  FOREIGN KEY (tenant_id, connector_run_id) REFERENCES connector_runs (tenant_id, id);
