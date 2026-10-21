# CDC: snapshot + tail synchronization and schema evolution (DPP-039)

Status: design, grounded in the dev extras and `services/connector-manager/internal/cdc` as they are in this checkout.
Plan reference: DEVELOPMENT_PLAN.md Sprint 08 (DPP-037, DPP-039, gate "restart/failover CDC keeps position and replays events in a controlled way"), ADR-013 (Debezium/Kafka Connect, no own WAL reader).

## 1. What is actually configured

| Piece | Where | Facts |
|---|---|---|
| Source DB | `infra/docker/docker-compose.extras.yml` service `pg_cdc` (`astryon_pg_cdc`) | postgres:16, `wal_level=logical`, `max_wal_senders=4`, `max_replication_slots=4`, separate from the main DB. DEV ONLY. |
| Demo table | `infra/docker/extras/pg_cdc_init.sql` | `public.demo_items(id PK, name, qty, updated_at)`, `REPLICA IDENTITY FULL`, publication `astryon_cdc_pub FOR TABLE demo_items`, one seed row. |
| Connect | service `connect`, `quay.io/debezium/connect:2.7.3.Final` | JSON converters with schemas disabled (`CONNECT_*_SCHEMAS_ENABLE=false`); internal topics `astryon.connect.{configs,offsets,status}` (compacted, RF=1, created by `scripts/dev/extras-up.sh` because Kafka auto-create is off). |
| Connector | `infra/docker/extras/debezium-demo.json` | `PostgresConnector`, `plugin.name=pgoutput`, slot `astryon_cdc_slot`, publication `astryon_cdc_pub`, `topic.prefix=astryon_cdc`, `table.include.list=public.demo_items`, `snapshot.mode=initial`, `tombstones.on.delete=true`. Topic `astryon_cdc.public.demo_items` (1 partition) is pre-created by `extras-up.sh`. |
| Consumer | `services/connector-manager/internal/cdc` | Debezium-JSON consumer, optional via `ASTRYON_CDC_TOPIC` (+ `_GROUP`, `_CONNECTOR_ID`, `_SOURCE_ID`, `_TENANT_ID`, `_SCHEMA_ID`, `_PRIMARY_KEY`). Maps changes to the canonical envelope and emits to `raw.events.v1`. |
| Position store | migration `0013 connector_cdc_position`, `cdc.PGPositions` | last emitted source position per connector, RLS. Diagnostic only. |

Not configured: MySQL connector (DPP-037 names it; only the `Source` struct has binlog fields), signal table / incremental snapshots, schema registry, `schema.history` topic (not used by the Postgres connector), heartbeat, a dead-letter topic for Connect, any production Connect deployment. Evidence in the repo states the Debezium snapshot/insert/update path was verified live on the demo table (PROJECT_INDEX, release notes) and the consumer was live-tested against Kafka, but not run against the live Debezium topic.

## 2. Snapshot + tail model

Debezium with `snapshot.mode=initial` does the synchronization itself:

1. On first start the connector creates the replication slot (`astryon_cdc_slot`) and records the slot's consistent point (LSN), then reads every row of the included tables in a consistent snapshot transaction and emits them with `op="r"` and `source.snapshot="true"` (`"last"` on the final record).
2. After the snapshot it streams the WAL from the slot's consistent point (`op` = `c`/`u`/`d`, `source.snapshot="false"`). Because the slot was created before the snapshot read, no change committed during the snapshot is lost; a row changed during the snapshot can appear twice (as `r` and as a tail event). Convergence relies on last-write-wins per primary key, not on exactly-once.
3. Connect stores the source offset (LSN) in `astryon.connect.offsets`, committed after Kafka acknowledges the records. After a Connect restart the connector resumes from the stored LSN; on re-delivery the same change is produced again (at-least-once). The slot retains WAL until the connector confirms it.

ASTRYON side (`cdc.Consumer.RunOnce`): poll a bounded batch, `ParseChange`, `Map` to an envelope, quarantine malformed input (`ErrMalformed`, never retried), `Emit`, save position, then commit the Kafka offset. Any error leaves the batch uncommitted and the batch is re-delivered. So the whole chain is at-least-once, and duplicates are made harmless by deterministic identity:

- `EventID` = UUIDv5-shaped hash of `connector + source.Position() + table + entity key + op`. A re-delivered change gets the identical `event_id`, so the processing dedup (`tenant_id, source_id, event_id, processing_version`, see `services/processing/README.md`) and the ClickHouse `LIMIT 1 BY` dedup absorb it.
- `Position()` for log events is `lsn;txId;file;pos;row;gtid`; snapshot rows have none of these and use `snap:<source.ts_ms>`. This is stable across redelivery only as long as Debezium reports the same `source.ts_ms` for every row of one snapshot run and for a re-delivered record; this has not been checked on the live topic and should be before relying on it (open item A).
- `tenant_id`/`source_id`/schema come from the connector row (`MapConfig`), never from the payload or Kafka headers.
- Deletes become events with `op="d"`, `deleted=true` and the key only (no row image); the Debezium tombstone (null value) is counted and skipped (`Tombstones` metric). This depends on `REPLICA IDENTITY FULL` (or at least PK) for the `before` image; the demo table uses FULL.

### Ordering and the "snapshot vs tail" boundary

The consumer does not distinguish phases in the data path: `r`, `c` and `u` all carry `after` and are mapped the same way. Ordering per primary key is preserved only because the demo topic has one partition and Debezium keys records by PK; with more partitions per-key order is still kept (same key, same partition), cross-key order is not. Consumers of the processed events must therefore resolve state by (entity key, source position), not by arrival order. `event_time` is the Debezium `ts_ms`, which is the connector processing time, not the commit time of the source transaction; do not use it to order two changes of one row. Ordering by the LSN inside the stored payload is a follow-up (open item B): today the envelope payload does not carry the LSN, only the `event_id` hash.

### Phase state machine

`cdc.Phase` / `cdc.Transition` define NEW -> SNAPSHOTTING -> TAILING, with PAUSED and FAILED. They are tested (`state_test.go`) but not wired to the consumer or to PostgreSQL in this checkout, and Debezium itself does not report a phase to ASTRYON. Proposed derivation, using only data already consumed:

- SNAPSHOTTING while records have `source.snapshot` in {`true`,`last`};
- TAILING after the first record with `snapshot="false"` (or after the one with `"last"`);
- PAUSED when the connector row is paused (existing pause/resume API);
- FAILED on a Connect task `FAILED` state (via `GET /connectors/{name}/status`) or persistent quarantine growth.

Persisting the phase needs a forward-only migration with a rollback note (AGENTS.md rules); not done here.

## 3. Restart and failover cases

| Event | Behaviour (from the configuration) |
|---|---|
| Consumer restarts mid-batch | Offset not committed -> batch re-polled -> same `event_id`s re-emitted -> deduplicated downstream. Live-tested for the consumer against Kafka (restart/quarantine) per PROJECT_INDEX. |
| Connect worker restarts | Resumes from `astryon.connect.offsets`. Duplicates possible, no loss. Internal topics have RF=1 in dev: a broker loss loses the offsets (dev only). |
| Source DB restarts | Slot is persistent; WAL is retained. |
| Connector is deleted and re-created | The offset is lost; with `snapshot.mode=initial` a new snapshot is taken (rows re-emitted as `r`, same entity keys, **different** `snap:` position -> different `event_id` -> NOT deduplicated). The slot must also be dropped or re-used deliberately. Treat as a controlled replay: use a new `processing_version` (replay-controller semantics), do not rely on dedup. |
| Consumer down for a long time | The slot is independent of the consumer; Kafka retention on the data topic bounds how long the consumer can be down. Connect keeps producing. An inactive slot blocks WAL cleanup in the source: monitor `pg_replication_slots` retained WAL. No alert exists yet (open item C). |
| Re-snapshot wanted (data repair) | Not supported without recreating the connector: no signal table / incremental snapshot is configured (open item D). |

## 4. Schema evolution

What Debezium does (pgoutput): the connector learns table structure from relation messages in the WAL stream, so adding or dropping a column mid-stream is picked up without restart and the next events carry the new column set; type changes surface as changed value types. The data topic carries plain JSON (schemas disabled), so there is no registry-enforced compatibility. Table renames/new tables are not captured unless `table.include.list` and the publication are changed (the publication is `FOR TABLE demo_items` only).

What ASTRYON does today:

- The row image travels as `payload.after`, a JSON object with whatever columns exist; it is not interpreted. Therefore an added column flows through without code change. A removed column simply stops appearing. A type change flows through as a different JSON type and is not detected.
- `SchemaID`/`SchemaVersion` are fixed in `MapConfig` per connector (`ASTRYON_CDC_SCHEMA_ID`, schema version from the connector row). The consumer does not derive a version from the row shape.
- `cdc.Compatible(previous, next map[string]string)` implements the intended rule: additive only; a removed field or changed type is incompatible. It is unit-tested but not called by any production path.

Proposed policy (design, not implemented):

1. Additive column: compatible, no action, event keeps the connector's `schema_version`. The change is allowed because the payload is an opaque object.
2. Dropped column or changed type: incompatible. The operator creates a new connector version (new `schema_version`, and `processing_version` bump if history is to be re-derived), then deploys it. Until then, events keep flowing; to avoid silent corruption the consumer should compare the key set / JSON types of `after` with the last seen shape (a bounded fingerprint stored with the position) and, on an incompatible change, quarantine with a fixed reason (new `ERR_*` code, added to the allow-list in the processing metrics) and raise an alert, rather than guess. Requires `Compatible` to be fed from a shape fingerprint; shape detection is the missing piece.
3. Primary key change: the entity key changes, so `entity_key` and `event_id` change; treat as incompatible (new connector version).
4. Table added to the capture: requires editing `table.include.list` and the publication; Debezium then snapshots only per its `snapshot.mode` rules, so a new table needs either `snapshot.mode=initial` on a new connector or an incremental-snapshot signal (open item D).
5. Data topic format stays JSON with schemas disabled until a registry is chosen; if Avro/Protobuf is adopted, compatibility mode `BACKWARD` matches the additive-only rule, but processing currently quarantines non-JSON payloads (`ERR_NORMALIZATION_UNSUPPORTED`).

## 5. Verification plan (not yet run)

Against the dev extras (`make extras-up`), with the consumer enabled (`ASTRYON_CDC_TOPIC=astryon_cdc.public.demo_items`):

1. Snapshot: seed N rows before the connector is created; assert N `op=r` events, then one `snapshot=last`. Concurrently insert/update during the snapshot; assert final per-key state matches the table.
2. Tail: insert/update/delete; assert `c/u/d`, one delete event plus one skipped tombstone.
3. Restart: kill the consumer between emit and commit; assert re-delivered events have identical `event_id`; kill Connect; assert resume without loss.
4. Snapshot `ts_ms` stability (open item A): compare `source.ts_ms` across all `r` records of one run and across a Connect restart during a snapshot.
5. Schema: `ALTER TABLE ... ADD COLUMN`, then `DROP COLUMN`, then `ALTER ... TYPE`, with traffic running; record what the events look like and what the connector does. Only after this can section 4 be treated as verified.
6. Slot lag: measure retained WAL while the consumer is stopped.

## 6. Open items

- A. Verify snapshot position/`ts_ms` stability (event id determinism for snapshot rows).
- B. Carry the source position (LSN) in the envelope payload for per-key ordering.
- C. Replication-slot retained-WAL alert and Connect task status alert.
- D. Signal table + incremental snapshot for re-snapshots and added tables.
- E. Wire `cdc.Phase` and shape fingerprint/`Compatible` into the consumer with a forward-only migration (rollback note).
- F. MySQL connector config (not present in extras).
