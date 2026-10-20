# ADR-014: Data Lake / Iceberg only after PoC

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Added complexity without a proven need.

## Decision
Defer Iceberg/Data Lake; raw facts/snapshots stay as S3 objects (Parquet) for now.

## Consequences
Revisit after POC with concrete query/retention scenarios.
