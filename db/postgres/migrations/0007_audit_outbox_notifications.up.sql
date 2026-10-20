-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE notifications, outbox, audit_log CASCADE; DROP FUNCTION astryon_audit_immutable().
--   Prod: audit_log must be retained (compliance); new forward migration only.
-- Group: audit, transactional outbox, notifications.

CREATE TABLE audit_log (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  occurred_at timestamptz NOT NULL DEFAULT now(),
  actor_principal_id uuid,
  actor_aas_sub text,
  action      text NOT NULL,                 -- e.g. source.create, apikey.revoke
  entity_type text NOT NULL,
  entity_id   uuid,
  request_id  text,
  details     jsonb NOT NULL DEFAULT '{}'    -- PII must be masked before writing
);
CREATE INDEX audit_log_tenant_time_ix ON audit_log (tenant_id, occurred_at DESC);
CREATE INDEX audit_log_entity_ix ON audit_log (tenant_id, entity_type, entity_id);
CREATE INDEX audit_log_actor_ix ON audit_log (tenant_id, actor_principal_id);

CREATE FUNCTION astryon_audit_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'audit_log is append-only'; END $$;
CREATE TRIGGER audit_log_no_update BEFORE UPDATE OR DELETE ON audit_log FOR EACH ROW EXECUTE FUNCTION astryon_audit_immutable();

CREATE TABLE outbox (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  aggregate_type text NOT NULL,
  aggregate_id uuid NOT NULL,
  event_type  text NOT NULL,
  topic       text NOT NULL,
  message_key text,
  payload     jsonb NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  available_at timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts    integer NOT NULL DEFAULT 0,
  last_error  text
);
CREATE INDEX outbox_pending_ix ON outbox (available_at, created_at) WHERE published_at IS NULL;
CREATE INDEX outbox_tenant_ix ON outbox (tenant_id, created_at DESC);
CREATE INDEX outbox_aggregate_ix ON outbox (tenant_id, aggregate_type, aggregate_id);

CREATE TABLE notifications (
  id          uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  tenant_id   uuid NOT NULL REFERENCES tenants(id),
  principal_id uuid,                         -- NULL = tenant-wide
  channel     text NOT NULL DEFAULT 'in_app' CHECK (channel IN ('in_app','email','webhook')),
  severity    text NOT NULL DEFAULT 'info' CHECK (severity IN ('info','warning','error','critical')),
  title       text NOT NULL,
  body        text,
  entity_type text,
  entity_id   uuid,
  created_at  timestamptz NOT NULL DEFAULT now(),
  sent_at     timestamptz,
  read_at     timestamptz,
  deleted_at  timestamptz,
  FOREIGN KEY (tenant_id, principal_id) REFERENCES principals (tenant_id, id)
);
CREATE INDEX notifications_principal_ix ON notifications (tenant_id, principal_id, created_at DESC);
CREATE INDEX notifications_unsent_ix ON notifications (created_at) WHERE sent_at IS NULL AND deleted_at IS NULL;

SELECT astryon_enable_rls('audit_log');
SELECT astryon_enable_rls('outbox');
SELECT astryon_enable_rls('notifications');
