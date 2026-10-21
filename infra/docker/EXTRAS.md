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
- Processing (Java 17, Flink 1.20.3): `mvn package` in `maven:3.9-eclipse-temurin-17` passes the unit tests (19 after the POJO fix) and builds the shaded jar; the image builds, the mini cluster starts, the job is submitted and reaches RUNNING with the S3 plugin and checkpoint bucket `flink-checkpoints` in s3mock (create it first: `curl -X PUT http://127.0.0.1:19200/flink-checkpoints`).

## Processing job (fixed 2026-10-21)

Former blocker: `RawRecord`/`OutputRecord`/`AcceptedRecord` were Java records; Flink 1.20 does not analyse records and fell back to Kryo, which
failed on the first element (`can't get field offset on a record class`). They are now public POJOs (`services/processing`, one class per file) and
`RecordTypesTest` fails if any of them becomes a generic type again (checked: it fails on the old records, passes on the POJOs; 19 tests total).

Verified live on the dev stack (Flink 1.20.3 mini cluster, checkpoints in s3mock):
- the existing 1351 events of `raw.events.v1` went through: `processed.events.v1` read_committed = 1351, no job exceptions;
- a new valid event reached `processed.events.v1` (`raw_ref kafka:raw.events.v1/0/216`);
- an event with a valid tenant and `schema_version: 0` produced a `QuarantineRecordV1` (`stage flink-processing`, `ERR_SCHEMA_VERSION_INVALID`)
  on `events.quarantine.v1` and nothing on `processed`.
Note: topics are written transactionally; offset counts include commit markers, so count with `--isolation-level read_committed`, not with `kafka-get-offsets`.

Also fixed: `entrypoint.sh` appended to `config.yaml` on every start, so `docker stop`/`start` produced duplicate YAML keys and an unhealthy container;
it now rebuilds the file from a pristine copy (stop/start verified healthy).

### Open finding (not changed): a record without a valid `tenant_id` stalls the job
`EnvelopeProcessor.reject` throws `IllegalStateException("cannot quarantine without a validated tenant_id")` (asserted by `EnvelopeProcessorTest`).
One such message (e.g. `{"not":"an envelope"}`) on `raw.events.v1` makes the job restart forever on that partition, which conflicts with the plan's
sprint 09 gate "a bad message does not block a partition forever". It needs a product decision (route to `events.dlq.v1` with a bounded original and
Kafka provenance, or a tenantless quarantine path). Recovery used here: stop the container, `kafka-consumer-groups.sh --group astryon-processing-v1
--topic raw.events.v1 --reset-offsets --to-latest --execute`, start again (a fresh mini cluster does not restore old checkpoints).
