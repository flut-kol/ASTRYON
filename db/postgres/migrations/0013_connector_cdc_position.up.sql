-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE connector_cdc_position. Prod: new forward migration. The row is
--   diagnostic only (last source position emitted); the authoritative resume point is the Kafka consumer-group commit.
-- Group: CDC consumer monitoring. One row per CDC connector, overwritten on each batch. Never per-event rows.

CREATE TABLE connector_cdc_position (
  tenant_id    uuid NOT NULL REFERENCES tenants(id),
  connector_id uuid NOT NULL,
  position     text NOT NULL,
  updated_at   timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (tenant_id, connector_id),
  FOREIGN KEY (tenant_id, connector_id) REFERENCES connectors (tenant_id, id)
);
CREATE TRIGGER connector_cdc_position_updated BEFORE UPDATE ON connector_cdc_position FOR EACH ROW EXECUTE FUNCTION astryon_set_updated_at();

SELECT astryon_enable_rls('connector_cdc_position');
