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
- Registry reconciliation (needs `REGISTRY_PG_URL`; otherwise skipped with a notice): per (tenant, source, batch,
  processing_version) the de-duplicated ClickHouse count (whole batch, for batches that received data in the window or
  were created in it) is compared with `ingestion_batches.events_accepted`. `missing` (CH < accepted) is a data-loss
  candidate, `extra` (CH > accepted) duplicates or foreign rows, `unregistered` a batch id/tenant/source pair unknown to the
  registry. Batches OPEN/SEALED/PROCESSING or updated within `REGISTRY_GRACE` (15m) are `transient` (reported, exit 0);
  the rest are `persistent` (exit 2). Replays write a new `processing_version` for the same batch, so a replay that
  legitimately accepts fewer/more events than the original shows up per version.
- Checksums: per (tenant, source, event_date, processing_version) over de-duplicated rows (window widened to whole UTC
  days): `count`, `sum(cityHash64(event_id, event_time, batch_id, event_type, entity_id, payload))` (UInt64, order
  independent) and `max_ingested_ms` (the revision). `--snapshot <file|->` writes them as JSON (`-` = stdout, logs go to
  stderr); `--compare <file>` diffs a previous snapshot over the common date range: a changed checksum with unchanged
  count and revision is `SILENT` (corruption candidate, exit 2); a changed checksum with a changed count/revision is
  listed as explained; partitions that vanished are informational.
- Revision is the writer's clock in ms and the reader takes the highest: a writer with a clock ahead of the aggregator
  shadows later recomputes (seen while testing with a future-dated row). Keep writers on NTP; `reconcile` reports it.

## Reconcile
    ch-aggregator reconcile [--snapshot snap.json|-] [--compare previous.json]

Env (besides `CH_*`): `REGISTRY_PG_URL` (read-only role with SELECT on `ingestion_batches`; NOBYPASSRLS is fine: each query
runs in a READ ONLY transaction that sets the constant `astryon.platform_admin='on'` locally, same switch as the
export-worker's cross-tenant paths, plus a 30s statement_timeout), `REGISTRY_GRACE` (15m), `AGG_CHECKSUM_LOOKBACK`
(= `AGG_LOOKBACK`), `AGG_QUERY_TIMEOUT` (120s, server-side `max_execution_time`; those queries run with `readonly=2` since
ClickHouse forbids setting changes under `readonly=1`, still no writes), `AGG_FAIL_ON_GAPS`. Each list is bounded (1000
rows; "partial" is logged when hit). Exit: 0 clean, 2 duplicates / aggregate drift / persistent registry mismatch / silent
checksum change (gaps and transient mismatches informational), 1 a check could not run. Example output lines:
`registry-mismatch kind=missing class=persistent ... ch_events=40 registry_accepted=50`,
`checksum-change SILENT (corruption candidate) ... events=50->50 checksum=A->B`.

## Run
Env: `CH_URL` (http://localhost:8123), `CH_DATABASE` (astryon), `CH_USER`, `CH_PASSWORD`, `AGG_LOOKBACK` (3h, covers late
data), `AGG_INTERVAL` (0 = run once; otherwise repeat). Schedule it as a Kubernetes CronJob or with `AGG_INTERVAL`.

    docker build -f services/ch-aggregator/Dockerfile -t astryon/ch-aggregator .   # context = repo root

Verified: unit tests; recompute SQL and idempotent revisions on ClickHouse 24.8; image builds and fails cleanly when
ClickHouse is unreachable. Live check on dev ClickHouse 24.8 (2026-10-20): 100 clean + 50 events with 20 forced duplicate rows -> `reconcile` flagged only the duplicated batch (70 rows / 50 unique, exit 2) and the aggregate counted 150 events. Registry/checksum checks: `scripts/live-reconcile.sh` (throwaway ClickHouse 24.8 + postgres:16-alpine, all migrations applied, NOBYPASSRLS role; removes its containers) passed 12/12 on 2026-10-20: clean = 0, deleted rows = missing, foreign/extra rows = extra/unregistered, payload tampered with equal count = SILENT on `--compare`, open or young batch = transient exit 0, same batch after grace = persistent. Not verified: multi-shard tables, large volumes (the checksum query scans the day-aligned window), PG with multiple replicas, replays in the registry comparison.
