-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE source_active_versions CASCADE.
--   Prod: new forward migration. Dropping the table makes readers fall back to the explicit processing_version
--   (query-api answers 400 no_active_version when it is omitted); no ClickHouse data is touched.
-- NUMBERING: follows 0009_export_jobs and 0008_tenant_scoped_fks (copied from branch flut_kol/ASTRYON). Renumber on merge if that branch adds its own 0009+.
-- Group: replay cutover. Per (tenant, source) the processing_version that readers use by default.
--   Written only by services/replay-controller (POST /v1/replays/{id}/activate); read by services/query-api.
--   One row per source: state of the source, never per event.

CREATE TABLE source_active_versions (
  tenant_id                   uuid NOT NULL REFERENCES tenants(id),
  source_id                   uuid NOT NULL,
  active_processing_version   integer NOT NULL CHECK (active_processing_version > 0),
  previous_processing_version integer CHECK (previous_processing_version > 0),
  switched_at                 timestamptz NOT NULL DEFAULT now(),
  switched_by                 text,
  PRIMARY KEY (tenant_id, source_id),
  FOREIGN KEY (tenant_id, source_id) REFERENCES sources (tenant_id, id)
);

SELECT astryon_enable_rls('source_active_versions');
