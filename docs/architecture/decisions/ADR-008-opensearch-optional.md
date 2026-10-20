# ADR-008: OpenSearch is optional

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Full-text search is costly to run and not yet a confirmed need.

## Decision
Add OpenSearch (search-sink) only when queries require full-text; ClickHouse covers structured filters/aggregations.

## Consequences
Smaller first release; possible later sink addition behind the same processed-event contract.
