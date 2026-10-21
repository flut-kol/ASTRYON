# Dev extras (Redis, Kafka Connect + Debezium, ch-aggregator, Flink processing)

DEV ONLY, placeholder credentials, ports on 127.0.0.1. Own compose project `astryon-extras` on the dev network
`astryon-dev_default` (so `scripts/dev/up.sh --remove-orphans` never removes them). Requires the dev stack (`scripts/dev/up.sh`).

| What | Start | Stop | Endpoint |
|---|---|---|---|
| Redis 7 (`astryon_redis`) | `scripts/dev/extras-up.sh` | `scripts/dev/extras-down.sh [--volumes]` | `redis://127.0.0.1:16379` (in-network `redis://redis:6379`) |
| CDC PostgreSQL 16 `wal_level=logical` (`astryon_pg_cdc`, separate from `astryon_postgres`) | same | same | `127.0.0.1:15433`, db `cdcdemo`, user `cdc`, table `demo_items` |
| Kafka Connect + Debezium 2.7.3 (`astryon_connect`), demo connector `astryon-demo-cdc` | same | same | REST `http://127.0.0.1:18484`, topic `astryon_cdc.public.demo_items` |
| ch-aggregator one-shot | `scripts/dev/aggregator-run.sh [reconcile ...]` | exits | against `astryon_clickhouse` / `astryon_postgres` |
| Flink processing job (`astryon_processing`, profile `processing`) | `docker compose -f infra/docker/docker-compose.extras.yml --profile processing up -d --build processing` | `... stop processing` | Flink UI `http://127.0.0.1:18481` |

`extras-up.sh` creates the Connect internal topics and the demo topic first (Kafka auto-create is off on the dev broker; RF=1) and
registers the demo connector once. To feed the connector-manager CDC consumer set `ASTRYON_CDC_TOPIC=astryon_cdc.public.demo_items`,
`ASTRYON_CDC_PRIMARY_KEY=id`, `ASTRYON_CDC_CONNECTOR_ID/_TENANT_ID/_SOURCE_ID/_SCHEMA_ID` (not wired into the apps container here).
Debezium messages use the JSON converter without schemas (envelope `before/after/source/op`), which the consumer parser accepts
(`internal/cdc/debezium.go` handles both bare and `{schema,payload}`; parsing a live message was NOT run).

## Verified (2026-10-21)
- Redis: `QUERY_REDIS_IT_URL=redis://127.0.0.1:16379 go test ./internal/query -run TestLiveRedis` in `services/query-api`: single-node case passes (rate not doubled, concurrency cap, killed holder expiry, tenant isolation).
- Debezium: connector and task RUNNING; snapshot row, an insert and an update (with `before`) arrived in `astryon_cdc.public.demo_items` (read with the Kafka console consumer).
- ch-aggregator: image built from `services/ch-aggregator/Dockerfile` (golang:1.27 available); a run wrote 9 `source_metrics_hourly`, 3 `pipeline_metrics_hourly`, 5 `processing_quality_daily` rows (1350 events). `reconcile` reports 4 `unregistered` persistent batches: the loadgen batch ids are not in `ingestion_batches`, expected for synthetic data.
- Processing (Java 17, Flink 1.20.3): `mvn package` in `maven:3.9-eclipse-temurin-17` passes 17 unit tests and builds the shaded jar; the image builds, the mini cluster starts, the job is submitted and reaches RUNNING with the S3 plugin and checkpoint bucket `flink-checkpoints` in s3mock (create it first: `curl -X PUT http://127.0.0.1:19200/flink-checkpoints`).

## Known blocker: processing job does not process records
On the first record from `raw.events.v1` the job fails and loops in RESTARTING:
`UnsupportedOperationException: can't get field offset on a record class: ... RawRecord.topic` / `Unable to create serializer
"com.esotericsoftware.kryo.serializers.FieldSerializer" for class io.astryon.processing.RawRecord`. `RawRecord` is a Java record that Flink
falls back to Kryo for. Nothing reached `validated.events.v1`/`processed.events.v1`/`events.quarantine.v1`. Fix belongs in `services/processing`
(give Flink a proper type: `TypeInformation` for the record or a POJO, instead of generic Kryo), then re-run the check above. Until then
`clickhouse-sink` keeps reading `raw.events.v1` directly in the dev stack.
