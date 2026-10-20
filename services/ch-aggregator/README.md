# ch-aggregator (DPP-028 / 044)

Recomputes the ClickHouse aggregate tables from migration `0003_aggregates` over a trailing window:
`source_metrics_hourly`, `pipeline_metrics_hourly`, `processing_quality_daily`.

- Each run writes a new `revision` (unix ms). Readers take `argMax(.., revision)`; nothing relies on merge-time dedup.
- Events are de-duplicated with `LIMIT 1 BY (tenant_id, source_id, event_id, processing_version)`; replays use a new
  `processing_version`, so they never double metrics.
- Rejected events (`processing_errors_local`) have no `pipeline_id`; they are attributed to the nil-UUID pipeline.
- `ch-aggregator reconcile` runs three read-only checks over the window: duplicate rows per batch (`count()` vs
  `uniqExact(event_id)`), drift between the latest stored `source_metrics_hourly.events_count` and a fresh count of
  de-duplicated events per hour (missing and orphan buckets count), and empty hours inside each source's active range
  (`WITH FILL`). Exit code 2 on duplicates or aggregate drift; gaps are informational unless `AGG_FAIL_ON_GAPS=1`.
- Revision is the writer's clock in ms and the reader takes the highest: a writer with a clock ahead of the aggregator
  shadows later recomputes (seen while testing with a future-dated row). Keep writers on NTP; `reconcile` reports it.

## Run
Env: `CH_URL` (http://localhost:8123), `CH_DATABASE` (astryon), `CH_USER`, `CH_PASSWORD`, `AGG_LOOKBACK` (3h, covers late
data), `AGG_INTERVAL` (0 = run once; otherwise repeat). Schedule it as a Kubernetes CronJob or with `AGG_INTERVAL`.

    docker build -f services/ch-aggregator/Dockerfile -t astryon/ch-aggregator .   # context = repo root

Verified: unit tests; recompute SQL and idempotent revisions on ClickHouse 24.8; image builds and fails cleanly when
ClickHouse is unreachable. Live check on dev ClickHouse 24.8 (2026-10-20): 100 clean + 50 events with 20 forced duplicate rows -> `reconcile` flagged only the duplicated batch (70 rows / 50 unique, exit 2) and the aggregate counted 150 events. Not verified: multi-shard tables.
