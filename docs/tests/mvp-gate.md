# MVP gate (DPP-030): real end-to-end chain

Run 2026-10-20. Chain under test, all four services built from their modules and run as subprocesses:
`gateway (dev auth) -> raw.events.v1 -> stream-processor -> processed.events.v1 -> clickhouse-sink -> ClickHouse -> query-api`.
Code: `tests/e2e` (module `github.com/astryon/e2e`). Performance numbers: `performance-report-v0.md`.

## How to run
```bash
# 1. dev stack (own compose project + non-default ports so a concurrent stack is never touched)
export COMPOSE_PROJECT_NAME=astryon-mvp KAFKA_HOST_PORT=29092 KAFKA_TLS_HOST_PORT=29093 POSTGRES_HOST_PORT=25432 \
       CLICKHOUSE_HTTP_HOST_PORT=28124 CLICKHOUSE_NATIVE_HOST_PORT=29010
scripts/dev/up.sh kafka clickhouse postgres && make topics
CH_CLIENT="docker exec -i astryon-mvp-clickhouse-1 clickhouse-client" CH_USER=astryon CH_PASSWORD=astryon_dev_only db/apply.sh clickhouse
# 2. tests (skipped unless ASTRYON_E2E=1; defaults point at the standard dev ports 19092/18124/15432)
cd tests/e2e && ASTRYON_E2E=1 ASTRYON_E2E_KAFKA=127.0.0.1:29092 ASTRYON_E2E_CH_URL=http://127.0.0.1:28124 \
  ASTRYON_E2E_PG_DSN='postgres://astryon:astryon_dev_only@127.0.0.1:25432/astryon?sslmode=disable' go test -v -run E2E ./...
# teardown: scripts/dev/down.sh --volumes (with the same COMPOSE_PROJECT_NAME)
```
Isolation: each run uses fresh random tenant UUIDs, per-tenant gateway + query-api (dev auth is single-tenant), and fresh
consumer groups whose offsets are pre-committed to the log end (no old data is reprocessed, nothing is deleted).
Gateway publishes to fixed topic names, so topics are shared; isolation is by tenant.

## Results (all passing, 3 consecutive runs)
| Test | Assertions |
|---|---|
| `TestE2EPipeline` | tenant A posts 200 unique + 20 byte-identical re-posts + 10 invalid; tenant B 50 unique. Gateway: A accepted 220 / quarantined 10 / failed 0. ClickHouse `count()==uniqExact==200` for A (duplicates dropped by stream-processor, not just hidden at query time), 50 for B. query-api returns exactly the 200 / 50 expected event_ids (cursor paging). Aggregate `events`=200. Quarantine topic holds the 10 gateway rejections and 1 stream-processor quarantine (a record injected straight into `raw.events.v1` with `schema_version=0`); none stored in ClickHouse. Isolation: B never sees A ids (and vice versa); A's token on B's query-api -> 401/403; posting an event with A's `tenant_id` to B's gateway -> 403 and B row count unchanged. |
| `TestE2EReplay` | 100 events at `processing_version` 1. (a) same events re-published at pv=1: stream-processor drops all 100 (metric), ClickHouse stays 100 rows, query 100. (b) re-published at pv=2 with a changed payload: ClickHouse 200 rows, query-api returns 100 events all at pv=2 with the new payload, aggregate `events`=100 (not 200), `processing_version=1` filter still returns the 100 old events (rollback view), group_by processing_version = {1:100, 2:100}. (c) pv=2 again: dropped, still 200 rows / 100 query results. |

`make test` passes for all modules (the e2e tests skip without `ASTRYON_E2E=1`).

## Findings
- Duplicate semantics now hold end to end. The earlier smoke (`e2e-smoke.md`, sink fed straight from raw) stored duplicates;
  with stream-processor in the chain the stored rows equal unique event ids.
- Dedup is a bounded window (default 1 h / 2M entries). A re-post after the window, or across a processor restart with
  `STREAMPROC_WARM_START=false`, reaches ClickHouse as a second physical row; reads stay correct only because query-api
  uses `LIMIT 1 BY`. Not exercised here (TTL eviction / restart are covered by processor unit tests only).
- Aggregate response columns are unprefixed (`events`, group key by its name), not `m_`/`g_` as the SQL aliases suggest.
- query-api refuses to start without `DATABASE_URL` even though `/v1/query/events` never touches PostgreSQL.
- gateway `handleBatch` publishes each event with its own synchronous produce (`ProduceSync`), so ack latency grows with batch size and the gateway was the first thing to saturate (see performance report).

## Not covered
TLS/SASL between services, real AAS tokens (dev static auth), crash/restart of any service mid-run, multi-instance consumers,
Flink (stream-processor is the Go replacement, see its README), PostgreSQL-backed endpoints (`/v1/sources`, `/v1/jobs`), aggregate tables (no producer yet).
