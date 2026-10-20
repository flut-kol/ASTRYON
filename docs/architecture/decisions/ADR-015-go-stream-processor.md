# ADR-015: Stream processing as Go service `services/stream-processor` instead of Flink (MVP)

Status: Accepted for MVP; **revisit later** (amends ADR-003)

## Context
ADR-003 plans Apache Flink (Java) for validation/normalization/dedup. The Flink/Java toolchain is not available in the current dev environment, which would block the vertical slice (raw.events.v1 -> processed.events.v1 -> ClickHouse).

## Decision
Implement the processing step as a Go service, `services/stream-processor`, consuming `raw.events.v1` and producing `processed.events.v1` (and quarantine/DLQ), keeping the same event-contract and topics so a Flink job can replace it. It must honour ADR-011 (at-least-once, idempotent downstream) and carry `pipeline_version`.

## Consequences
- MVP unblocked using the existing Go toolchain and `libs/event-contracts`.
- No Flink checkpoints/event-time window semantics; late-data, large-state dedup and windowing guarantees from §11 Flink tests are not provided by this service and must be explicitly scoped or built later.
- Dedup must use a bounded window/strategy (never unbounded in-memory UUID sets, §5.3).
- Scaling is via Kafka consumer-group partitions; throughput is unmeasured and must go through PERF tests.
- Revisit when a Flink toolchain is available or when stateful/windowed requirements exceed what the Go service can safely do; the topic contract is the migration boundary. Update ADR-003 status then.
