# ADR-005: ClickHouse for analytics

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Need OLAP, aggregations, TTL and sharding for very high insert rates.

## Decision
ClickHouse, initial model 2 shards x 2 replicas with Keeper; batched inserts; partition/ORDER BY by workload tests; backups independent of replication. Idempotence via insert dedup token and tested dedup, not a promise that ReplacingMergeTree removes duplicates instantly.

## Consequences
Eventual dedup visibility must be handled in queries; small inserts must be batched in the sink; reshuffle needs a migration tool.
