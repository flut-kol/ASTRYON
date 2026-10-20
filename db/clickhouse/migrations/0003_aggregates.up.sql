-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE processing_quality_daily, source_metrics_hourly, pipeline_metrics_hourly.
--   Prod: add new tables; aggregates can be rebuilt from the primary registry via reconciliation.
-- Template tokens: see 0001.
-- Aggregates are NOT materialized views on the raw insert path (an MV would double-count if a retried block were
-- not deduplicated, and replay would add on top). They are written by the aggregation/reconciliation job from the
-- primary registry (PostgreSQL batch aggregates + events_normalized_*), keyed by processing_version, so a replay
-- into a new processing_version produces a separate row set rather than doubling. A recompute of a bucket writes
-- with a higher `revision`; readers take argMax(.., revision) per key (explicit revision model, §5.3), they never
-- depend on background-merge dedup. SummingMergeTree is deliberately avoided for the same reason.

CREATE TABLE IF NOT EXISTS processing_quality_daily __ON_CLUSTER__
(
    tenant_id          UUID,
    source_id          UUID,
    pipeline_id        UUID,
    processing_version UInt32,
    day                Date,
    revision           UInt64,                          -- monotonically increasing per recompute
    events_total       UInt64,
    events_valid       UInt64,
    events_rejected    UInt64,
    events_with_flags  UInt64,
    flag               LowCardinality(String) DEFAULT '', -- '' = totals row
    flag_count         UInt64 DEFAULT 0,
    computed_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(day)
ORDER BY (tenant_id, source_id, day, processing_version, pipeline_id, flag, revision);

CREATE TABLE IF NOT EXISTS source_metrics_hourly __ON_CLUSTER__
(
    tenant_id          UUID,
    source_id          UUID,
    processing_version UInt32,
    hour               DateTime('UTC'),
    revision           UInt64,
    events_count       UInt64,
    bytes_count        UInt64,
    batches_count      UInt32,
    errors_count       UInt64,
    max_ingest_lag_ms  UInt64,
    computed_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, source_id, hour, processing_version, revision);

CREATE TABLE IF NOT EXISTS pipeline_metrics_hourly __ON_CLUSTER__
(
    tenant_id          UUID,
    pipeline_id        UUID,
    pipeline_version   UInt32,
    processing_version UInt32,
    hour               DateTime('UTC'),
    revision           UInt64,
    events_in          UInt64,
    events_out         UInt64,
    events_failed      UInt64,
    p50_latency_ms     Float32,
    p95_latency_ms     Float32,
    computed_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC')
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(hour)
ORDER BY (tenant_id, pipeline_id, hour, pipeline_version, processing_version, revision);
