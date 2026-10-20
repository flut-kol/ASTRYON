-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE processing_errors_distributed.
-- Prod: keep the compatibility table until every reader has moved to a newer
-- version, then remove it in a separately reviewed migration.
--
-- This read surface complements processing_errors_local from migration 0002.
-- It lets tenant-scoped quarantine/quality readers see every shard without
-- querying local tables directly. The shard key matches normalized events so
-- a tenant/source pair has the same placement across both datasets.

CREATE TABLE IF NOT EXISTS processing_errors_distributed __ON_CLUSTER__
AS processing_errors_local
ENGINE = Distributed('__CLUSTER__', currentDatabase(), processing_errors_local, cityHash64(tenant_id, source_id));
