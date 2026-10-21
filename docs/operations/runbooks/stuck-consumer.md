# Runbook: clickhouse-sink stuck or lagging

Covers `AstryonSinkInsertFailures`, `AstryonSinkCommitFailures`, `AstryonSinkStalled` and
`AstryonKafkaConsumerLagGrowing` for the `processed.events.v1` consumer group.

## Symptoms

- `astryon_clickhouse_sink_insert_failures_total` or `_commit_failures_total` increases;
- `astryon_clickhouse_sink_consumed_total` is flat while `kafka_consumergroup_lag` > 0;
- `astryon_clickhouse_sink_insert_retries_total` grows steadily (ClickHouse slow or rejecting).

## Safe diagnosis

Metrics are on `CHSINK_METRICS_ADDR` (`/metrics`, `/debug/vars`). Do not copy event payloads or
raw Kafka messages into tickets; use counters, offsets and error codes only.

1. Sink logs: `<step> failed (attempt N)` lines carry the ClickHouse HTTP status. 4xx on every retry means a
   schema/mapping problem (not transient); 5xx/timeouts mean ClickHouse load, disk or Keeper.
2. ClickHouse health: free disk (`AstryonClickHouseDiskHigh`), `SELECT * FROM system.replication_queue`
   on replicated tables, Keeper availability.
3. Kafka: group state and committed offsets per partition. Commit failures with healthy inserts mean
   the broker or group coordinator is the problem; rows are already stored.

## Recovery

1. Fix the cause (free disk, restore Keeper/replica, restore broker connectivity or credentials).
   Never bypass TLS/SASL (`CHSINK_KAFKA_TLS*`, `CHSINK_KAFKA_SASL_*`).
2. Restart the sink normally. A retried chunk carries the same `insert_deduplication_token`
   (hash of topic, partition, first and last offset), so ClickHouse drops it as a duplicate; this is
   verified on a single node, on 2 shards and across 2 replicas with Keeper
   (`internal/sink/integration_test.go`).
3. If restart can re-cut chunks differently, overlapping rows may be stored twice. Enable
   `CHSINK_INTENT_TABLE=sink_chunk_intents` (migration 0004) to persist chunk boundaries; readers
   dedup with `LIMIT 1 BY` either way, and `ch-aggregator reconcile` reports remaining duplicates.
4. Persistent 4xx: events go to quarantine only for mapping errors; a systematic rejection by
   ClickHouse needs a schema fix, not a replay loop. Escalate to the schema owner.

## Validation

- `consumed_total` and `inserted_rows_total` increase again; consumer lag trends to zero.
- `insert_failures_total` and `commit_failures_total` stop increasing.
- `ch-aggregator reconcile` shows no new duplicate rows for the affected window.
