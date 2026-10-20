# ADR-003: Apache Flink (Java) for stateful realtime processing

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Realtime needs event-time, windows, state and checkpoints.

## Decision
Plan: Flink jobs in Java under `flink/` (validation, normalize, dedup).

## Consequences
Powerful state/checkpointing but adds JVM stack and ops. **Amended for MVP by ADR-015** (Go stream-processor used instead).
