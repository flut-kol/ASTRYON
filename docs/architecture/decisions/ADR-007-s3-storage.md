# ADR-007: S3-compatible storage (Ceph RGW)

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
Need immutable raw files, archive and snapshots.

## Decision
Use S3 API (Ceph RGW in prod, MinIO in dev) for raw files, archive, snapshots/checkpoints. Ceph HDD is not a base disk for Kafka/ClickHouse (§17).

## Consequences
Portable S3 contract; bucket access controls and SSRF protections needed.
