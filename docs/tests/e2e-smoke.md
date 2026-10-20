# E2E smoke: loadgen -> gateway -> Kafka -> clickhouse-sink -> ClickHouse

Run 2026-10-20 on the dev host. Throwaway compose project `astryon-e2e`; only kafka + clickhouse started
(PostgreSQL not needed for this slice; `postgres:16-alpine` is not pulled locally; MinIO skipped).
Ports used: Kafka 19092, ClickHouse 18124/19010, gateway 18080, sink metrics 18081 (all verified free beforehand).

Note: nothing in the repo yet moves `raw.events.v1` -> `processed.events.v1` (no normalizer), so the sink was pointed at
`raw.events.v1` with `CHSINK_TOPIC` (the sink accepts a plain envelope).

## Commands
```bash
export COMPOSE_PROJECT_NAME=astryon-e2e GOTOOLCHAIN=local
scripts/dev/up.sh kafka clickhouse
make topics
CH_CLIENT="docker exec -i astryon-e2e-clickhouse-1 clickhouse-client" CH_USER=astryon CH_PASSWORD=astryon_dev_only db/apply.sh clickhouse

(cd services/gateway && go build -o $S/gateway ./cmd/gateway)
(cd services/clickhouse-sink && go build -o $S/sink ./cmd/clickhouse-sink)
(cd tests/load && go build -o $S/loadgen .)

ASTRYON_DEV_AUTH=1 ASTRYON_DEV_TOKEN=e2e-dev-token ASTRYON_DEV_TENANT_ID=0a1b2c3d-1111-4222-8333-444455556666 \
  ASTRYON_GATEWAY_ADDR=127.0.0.1:18080 ASTRYON_KAFKA_BROKERS=127.0.0.1:19092 $S/gateway &
CHSINK_TOPIC=raw.events.v1 CHSINK_CH_PASSWORD=astryon_dev_only CHSINK_METRICS_ADDR=127.0.0.1:18081 $S/sink &

LOADGEN_TOKEN=e2e-dev-token LOADGEN_TENANT_ID=0a1b2c3d-1111-4222-8333-444455556666 \
  $S/loadgen -url http://127.0.0.1:18080/v1/events/batch -rate 500 -duration 20s -batch 50 -concurrency 4 \
  -dup-ratio 0.1 -invalid-ratio 0.05 -ids-out ids.txt -json-out report.json

# teardown
kill -TERM <sink> <gateway>; scripts/dev/down.sh --volumes
```
Verification: `clickhouse-client -q "select count(), uniqExact(event_id) from events_normalized_local"`,
`kafka-consumer-groups.sh --describe --group clickhouse-sink`, `kafka-get-offsets.sh --topic ...`,
`kafka-console-consumer.sh --topic events.quarantine.v1 --from-beginning`.

## Results (observed)
loadgen: 10000 sent in 20.0 s (200 requests), accepted 9517, quarantined 483, failed 0; 500.0 ev/s sent,
0.467 MB/s; ack latency p50 28.6 ms, p95 45.8 ms, p99 214.2 ms (max 422.6 ms); HTTP 200 x17, 207 x183.
Unique accepted event_ids 8473, repeat accepts (duplicates) 1044.

| Check | Result |
|---|---|
| ClickHouse `count()` | 9517 (= accepted incl. duplicates) |
| ClickHouse `uniqExact(event_id)` | 8473 = loadgen unique accepted; `diff` of the two id sets: empty |
| `raw.events.v1` end offsets (6 partitions) | sum 9517 |
| `events.quarantine.v1` end offsets / messages read | 483 / 483 (codes: EVENT_TIME_MISSING 111, SCHEMA_VERSION_INVALID 100, PAYLOAD_XOR_OBJECT_REF 99, ENVELOPE_MALFORMED 87, EVENT_ID_INVALID 86) |
| consumer group `clickhouse-sink` lag | 0 on all 6 partitions (current == log-end) |
| sink metrics | consumed 9517, inserted_rows 9517, commits 11, insert/commit failures 0 |
| shutdown | sink "stopped cleanly" on SIGTERM; gateway exited |

Finding (not a bug in this run, but the "row count == unique accepted" criterion does NOT hold literally): duplicate
events (same event_id re-posted) are all stored, so the table has 9517 rows for 8473 unique ids. This matches the design in
migration 0001 (block-level insert dedup only; readers use `LIMIT 1 BY` / reconciliation). Row-level dedup is not
performed by the sink or by ClickHouse here.

Teardown confirmed: 0 containers, 0 volumes, 0 networks for `astryon-e2e`, nothing listening on the used ports, no gateway/sink processes.

## Not exercised
PostgreSQL migrations, MinIO, TLS/SASL, crash/restart of the sink mid-batch, rates above 500 ev/s, multi-tenant skew
against a real gateway (dev auth has a single tenant; skew is covered by unit tests only).
