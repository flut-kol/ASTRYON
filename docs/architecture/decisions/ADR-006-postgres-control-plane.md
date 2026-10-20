# ADR-006: PostgreSQL HA for control plane only

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Tenants, sources, jobs and config need transactions; the event log is far too large for it.

## Decision
PostgreSQL stores control-plane entities only (§6). No per-event status in PostgreSQL; only aggregates, job/batch metadata, rejections, requested samples. Forward-only migrations with rollback notes; RLS for tenant scope.

## Consequences
Event volume never hits PG. Replay/retry state at scale lives in Kafka/ClickHouse/S3.
