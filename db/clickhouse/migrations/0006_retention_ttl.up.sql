-- ROLLBACK PLAN: forward-only. Retention can be lifted without data loss with
--   ALTER TABLE <t> REMOVE TTL;  (expired rows already dropped are not recoverable: restore from backup / S3 raw).
-- Template tokens: see 0001.
-- DPP-046 / §7: hot ClickHouse tables are bounded by retention; raw originals and historical snapshots live in S3.
-- ttl_only_drop_parts=1 drops a whole part only when every row in it has expired. With monthly partitions this
-- makes expiry a cheap part drop instead of a row-level rewrite.
-- The day counts below are DEFAULTS pending the retention decision of DPP-003; change them with a new forward
-- migration (ALTER ... MODIFY TTL), never by editing this file once applied.
--   events_normalized_local : 400 days after event_time (a full year plus a quarter for year-over-year queries and
--                             replay windows)
--   aggregates              : 800 days (small, needed for trend dashboards; rebuildable from the primary registry)
-- processing_errors_local (90 d) and sink_chunk_intents_local (7 d) already carry TTL in 0002 / 0004.
-- Not done on purpose: no materialized views on the raw insert path (see 0003: a retried block or a replay would
-- double-count); aggregates stay job-written with an explicit revision.

ALTER TABLE events_normalized_local __ON_CLUSTER__ MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE events_normalized_local __ON_CLUSTER__ MODIFY TTL toDateTime(event_time) + INTERVAL 400 DAY DELETE;

ALTER TABLE processing_quality_daily __ON_CLUSTER__ MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE processing_quality_daily __ON_CLUSTER__ MODIFY TTL toDateTime(day) + INTERVAL 800 DAY DELETE;

ALTER TABLE source_metrics_hourly __ON_CLUSTER__ MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE source_metrics_hourly __ON_CLUSTER__ MODIFY TTL toDateTime(hour) + INTERVAL 800 DAY DELETE;

ALTER TABLE pipeline_metrics_hourly __ON_CLUSTER__ MODIFY SETTING ttl_only_drop_parts = 1;
ALTER TABLE pipeline_metrics_hourly __ON_CLUSTER__ MODIFY TTL toDateTime(hour) + INTERVAL 800 DAY DELETE;
