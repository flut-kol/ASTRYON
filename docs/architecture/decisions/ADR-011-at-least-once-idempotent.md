# ADR-011: At-least-once across stores with idempotent sinks

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Flink-style exactly-once state does not give exactly-once into ClickHouse/OpenSearch.

## Decision
Deliver at-least-once end to end; sinks must be idempotent (e.g. ClickHouse insert token = sha256(topic, partition, first/last offset), commit after insert). Do not claim declarative end-to-end exactly-once without sink tests.

## Consequences
Duplicates possible on retry and must be absorbed by dedup key `event_id`+`pipeline_version`+`tenant_id`; reconciliation required.
