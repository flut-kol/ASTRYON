# ADR-004: Go/Python workers, Spark only if needed, for batch

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Heavy files, Parquet and parallel backfills must not all pass through Kafka.

## Decision
Batch via Go/Python workers (file-parser, batch-transform, ocr-adapter) orchestrated by batch-orchestrator; add Spark only when workers prove insufficient.

## Consequences
Avoids a Spark cluster initially; large Parquet bypasses Kafka and meets streaming path at the normalized-record contract.
