-- ROLLBACK PLAN: forward-only. audit_log is append-only compliance data and is retained in prod; non-prod only: DROP the added columns/indexes/constraints below (ALTER TABLE audit_log DROP COLUMN ...).
-- Group: DPP-053 audit actions + security events. Extends audit_log (0007; already tenant-RLS + append-only trigger); no new table.
-- Existing columns map: entity_type/entity_id = resource type/id, actor_aas_sub = actor subject, details = masked metadata (no secrets/PII).

ALTER TABLE audit_log
  ADD COLUMN category text NOT NULL DEFAULT 'audit',     -- 'audit' (state change/read of data) | 'security' (authn/authz/session events)
  ADD COLUMN outcome  text NOT NULL DEFAULT 'success',   -- success | denied | failure
  ADD COLUMN trace_id text,                              -- W3C trace id for correlation with logs/traces
  ADD COLUMN resource_ref text,                          -- non-UUID resource id (e.g. object key, scope); entity_id stays uuid
  ADD COLUMN actor_client_id text,                       -- AAS client_id (service accounts)
  ADD COLUMN auth_acr text;                              -- acr claim of the session (2FA evidence, enforced by IdP)

ALTER TABLE audit_log
  ADD CONSTRAINT audit_log_category_ck CHECK (category IN ('audit','security')),
  ADD CONSTRAINT audit_log_outcome_ck CHECK (outcome IN ('success','denied','failure')),
  ADD CONSTRAINT audit_log_details_size_ck CHECK (pg_column_size(details) <= 8192);

CREATE INDEX audit_log_category_time_ix ON audit_log (tenant_id, category, occurred_at DESC);
CREATE INDEX audit_log_trace_ix ON audit_log (tenant_id, trace_id) WHERE trace_id IS NOT NULL;
