-- ROLLBACK PLAN: forward-only, additive. Non-prod: DROP TABLE sink_chunk_intents_local (the sink then runs without
--   durable chunk boundaries, falling back to token dedup on identical re-cuts only). Prod: set CHSINK_INTENT_TABLE empty.
-- Template tokens: see 0001.
--
-- DURABLE CHUNK BOUNDARIES. ClickHouse block dedup only drops a retried block whose insert_deduplication_token matches.
-- The sink derives the token from the offset range [first,last] of a chunk, so after a crash between insert and offset
-- commit a restart that cuts different boundaries would store overlapping rows twice. Before inserting a chunk the sink
-- records its (topic, partition, first_offset, last_offset) here; a restarted sink re-cuts the first chunk of a partition
-- exactly to the latest recorded range, so the token matches and the block is dropped. Opt-in: CHSINK_INTENT_TABLE.
-- Only the latest row per (topic, partition) is ever read; old rows are removed by TTL.

CREATE TABLE IF NOT EXISTS sink_chunk_intents_local __ON_CLUSTER__
(
    topic        LowCardinality(String),
    partition    Int32,
    first_offset Int64,
    last_offset  Int64,
    recorded_at  DateTime64(6, 'UTC') DEFAULT now64(6, 'UTC')
)
ENGINE = __RMT__
PARTITION BY toYYYYMM(recorded_at)
ORDER BY (topic, partition, recorded_at)
TTL toDateTime(recorded_at) + INTERVAL 7 DAY DELETE;
