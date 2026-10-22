-- ROLLBACK PLAN: forward-only. Non-prod: ALTER TABLE events_normalized_local REMOVE INDEX idx_payload_text.
-- Prod: keep the index once applied; if it must be retired, use a separately reviewed forward migration
--       with REMOVE INDEX and allow existing parts to be queried without this skipping index.
--
-- The index is added to the local table because Distributed tables do not store data-skipping indexes.
-- Queries against events_normalized_distributed are still accelerated: the Distributed engine pushes the
-- predicate to each shard, where ClickHouse can use this index on events_normalized_local. __ON_CLUSTER__
-- keeps the DDL consistent with the other ClickHouse migrations when CH_CLUSTER is configured.

ALTER TABLE events_normalized_local __ON_CLUSTER__
    ADD INDEX IF NOT EXISTS idx_payload_text payload
    TYPE tokenbf_v1(10240, 3, 0)
    GRANULARITY 4;
