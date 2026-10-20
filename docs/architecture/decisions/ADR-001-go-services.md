# ADR-001: Go for ingestion/control/query/sink services

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Services need high concurrency, low per-request overhead and simple deployment.

## Decision
Implement gateway, control-plane, query-api, sinks and connectors in Go 1.22 (go.work, one module per service/lib).

## Consequences
Single static binaries, simple release. Team needs Go skills; JVM-only ecosystems (Flink) are separate (see ADR-003/015).
