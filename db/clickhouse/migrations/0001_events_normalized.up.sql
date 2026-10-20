-- ROLLBACK PLAN: forward-only. Non-prod: DROP TABLE events_normalized_distributed; DROP TABLE events_normalized_local.
--   Prod: never drop; add a new table/version and migrate. Backups are independent of replication (§7).
--
-- Template tokens, substituted by db/apply.sh (so the same file works on single node and on a cluster):
--   __ON_CLUSTER__  -> "ON CLUSTER <CH_CLUSTER>" when CH_CLUSTER is set, otherwise empty (single node).
--   __RMT__         -> CH_REPLICATED=1: ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')
--                      (needs <macros> shard/replica + Keeper); otherwise plain MergeTree().
--   __CLUSTER__     -> cluster name for Distributed (CH_CLUSTER, default 'default').
--
-- IDEMPOTENT SINK / DEDUP STRATEGY (§5.3). We do NOT rely on ReplacingMergeTree: it dedups only at background
-- merge time, so queries may see duplicates at any moment.
--  1. Identity: (tenant_id, source_id, event_id, processing_version). event_id is stable across retries of the
--     same logical event (never regenerated).
--  2. Insert-level idempotence: the sink inserts in deterministic blocks and sets
--     insert_deduplication_token = hash(tenant_id, source_id, batch_id, chunk_no, processing_version)
--     (with insert_deduplicate=1). A retried identical chunk is dropped by ClickHouse block dedup. This table sets
--     replicated_deduplication_window / non_replicated_deduplication_window so tokens are remembered long enough;
--     window size must exceed the max number of chunks in flight during a retry horizon (tuned by tests).
--     Chunks must be re-cut identically on retry (sink persists chunk boundaries in sink_checkpoints.insert_token).
--  3. Commit tracking: sink_checkpoints (PostgreSQL) records last committed chunk; sink resumes from it.
--  4. Replay: replay writes a NEW processing_version; readers select the active processing_version, so replay
--     never doubles metrics. Old versions are dropped by partition/TTL after cut-over.
--  5. Reconciliation: periodic check comparing count() vs uniqExact(event_id) per (tenant, source, batch,
--     processing_version) and against the ingestion_batches aggregates; mismatches are repaired by re-insert of
--     the batch under the same token set or by version re-run. Exact reads that must not see duplicates use
--     LIMIT 1 BY (tenant_id, source_id, event_id, processing_version), not FINAL promises.
-- Rationale for ORDER BY: tenant first (all queries are tenant scoped), then source, day, event_id (dedup/lookup).
-- Partition: monthly, initial hypothesis from §7; to be confirmed by workload tests.

CREATE TABLE IF NOT EXISTS events_normalized_local __ON_CLUSTER__
(
    tenant_id          UUID,
    source_id          UUID,
    event_id           UUID,
    batch_id           UUID,
    processing_version UInt32,
    pipeline_id        UUID,
    pipeline_version   UInt32,
    event_type         LowCardinality(String),
    event_time         DateTime64(3, 'UTC'),
    event_date         Date MATERIALIZED toDate(event_time, 'UTC'),
    ingested_at        DateTime64(3, 'UTC') DEFAULT now64(3, 'UTC'),
    entity_id          String DEFAULT '',
    attributes         Map(String, String),
    payload            String CODEC(ZSTD(3)),           -- normalized body; PII masked/tokenized upstream
    raw_ref            String,                           -- pointer to original (s3://bucket/key#offset or kafka topic/partition/offset)
    quality_flags      Array(LowCardinality(String)),   -- data quality issues found during normalization
    INDEX idx_event_type event_type TYPE set(100) GRANULARITY 4,
    INDEX idx_entity entity_id TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(event_time)
ORDER BY (tenant_id, source_id, toDate(event_time), event_id, processing_version)
SETTINGS index_granularity = 8192,
         non_replicated_deduplication_window = 10000;

-- Single node: Distributed over the one-node cluster is optional but keeps query code identical.
-- Shard key: cityHash64(tenant_id, source_id) -- to be validated against tenant skew (§7); revisit if hot tenants.
CREATE TABLE IF NOT EXISTS events_normalized_distributed __ON_CLUSTER__
AS events_normalized_local
ENGINE = Distributed('__CLUSTER__', currentDatabase(), events_normalized_local, cityHash64(tenant_id, source_id));
