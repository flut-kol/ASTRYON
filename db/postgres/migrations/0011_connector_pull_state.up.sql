-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE connector_pull_state. Prod: new forward migration (cursor state is
--   re-derivable only by re-pulling; take a backup before dropping).
-- Group: REST pull scheduling state. One row per pull connector: durable cursor, retry schedule and a fenced lease so several
-- connector-manager replicas never pull the same connector concurrently. Run-level counters only, never per-event rows.

CREATE TABLE connector_pull_state (
  tenant_id            uuid NOT NULL REFERENCES tenants(id),
  connector_id         uuid NOT NULL,
  cursor               text NOT NULL DEFAULT '',
  cursor_updated_at    timestamptz,
  consecutive_failures integer NOT NULL DEFAULT 0 CHECK (consecutive_failures >= 0),
  next_attempt_at      timestamptz NOT NULL DEFAULT now(),
  locked_by            text,
  locked_until         timestamptz,
  fence                bigint NOT NULL DEFAULT 0,      -- incremented on every claim; stale workers cannot write
  last_error           text,
  records_total        bigint NOT NULL DEFAULT 0,
  updated_at           timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, connector_id),
  FOREIGN KEY (tenant_id, connector_id) REFERENCES connectors (tenant_id, id)
);
CREATE INDEX connector_pull_state_due_ix ON connector_pull_state (next_attempt_at);
CREATE TRIGGER connector_pull_state_updated BEFORE UPDATE ON connector_pull_state FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('connector_pull_state');
