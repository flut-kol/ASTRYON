-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE processing_errors_local. Prod: add new table instead.
-- Rejected/failed events with quality errors (deviations only; successful events are not listed here).
-- Template tokens: see 0001. Idempotent via insert_deduplication_token (batch_id, chunk_no, processing_version).
-- TTL bounds retention; raw originals stay in S3 (raw_ref).

CREATE TABLE IF NOT EXISTS processing_errors_local __ON_CLUSTER__
(
    tenant_id          UUID,
    source_id          UUID,
    batch_id           UUID,
    job_id             UUID,
    event_id           UUID,
    processing_version UInt32,
    pipeline_version   UInt32,
    occurred_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    stage              LowCardinality(String),
    error_code         LowCardinality(String),
    error_message      String CODEC(ZSTD(3)),           -- must not contain unmasked PII
    raw_ref            String,
    sample_payload     String DEFAULT '' CODEC(ZSTD(3)) -- bounded/masked sample, optional
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (tenant_id, source_id, toDate(occurred_at), error_code, event_id)
TTL toDateTime(occurred_at) + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192,
         non_replicated_deduplication_window = 10000;
