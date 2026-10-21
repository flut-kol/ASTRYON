-- ROLLBACK PLAN: forward-only. Non-prod/prod rollback: DROP INDEX replay_requests_version_uq; (safe, only removes the guard).
-- Group: retries and replay. Defence in depth for §5.3: one replay_requests row per (tenant, source, target version).
-- The replay-controller allocator already serializes versions with an advisory lock; this index stops any writer that
-- bypasses it. NOT partial on purpose: soft-deleted and FAILED/CANCELLED rows keep their version burned forever
-- (the allocator takes MAX over all rows), so a version is never reused. Fails to apply if duplicates already exist;
-- resolve them first (they would mean two replays wrote the same processing_version).
-- Pre-check on an existing database (must return zero rows before applying):
--   SELECT tenant_id, source_id, target_processing_version, count(*)
--   FROM replay_requests GROUP BY 1,2,3 HAVING count(*) > 1;

CREATE UNIQUE INDEX replay_requests_version_uq ON replay_requests (tenant_id, source_id, target_processing_version);
