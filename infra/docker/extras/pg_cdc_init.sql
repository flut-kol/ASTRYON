-- DEV ONLY demo source for Debezium (separate PostgreSQL with wal_level=logical).
CREATE TABLE demo_items (id bigint PRIMARY KEY, name text NOT NULL, qty integer NOT NULL DEFAULT 0, updated_at timestamptz NOT NULL DEFAULT now());
ALTER TABLE demo_items REPLICA IDENTITY FULL;
CREATE PUBLICATION astryon_cdc_pub FOR TABLE demo_items;
INSERT INTO demo_items (id, name, qty) VALUES (1, 'first', 10);
