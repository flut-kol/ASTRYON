# ADR-013: CDC via Debezium / Kafka Connect

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Reading WAL/binlog ourselves is high-risk.

## Decision
Use Debezium on Kafka Connect; do not write a custom CDC reader in the first iteration (§17).

## Consequences
Connect operations burden; schema mapping from Debezium to the envelope is ours.
