# Operational runbooks

This directory contains incident response procedures for ASTRYON operational
components. Runbooks must preserve tenant isolation, use approved access paths,
avoid copying payloads or credentials into tickets, and record material manual
actions in the platform audit trail.

## Available runbooks

| Component | Runbook | Use when |
| --- | --- | --- |
| Retry controller | [retry-controller.md](retry-controller.md) | Due retries are not released, retry jobs remain stuck, or the controller health endpoint is unavailable. |
| Gateway | [ingestion-failures.md](ingestion-failures.md) | `AstryonGatewayAckErrors`: event writes answered with 5xx or `PUBLISH_FAILED`. |
| clickhouse-sink | [stuck-consumer.md](stuck-consumer.md) | Sink insert/commit failures, stalled consumption or growing consumer lag on `processed.events.v1`. |
| Replay | [replay.md](replay.md) | Replay stuck/failed, bad cutover, audit gaps after a crash. |
| Secrets | [secret-rotation.md](secret-rotation.md) | Planned rotation of Kafka certs/credentials, or suspected secret compromise. |
| Chaos | [chaos-gamedays.md](chaos-gamedays.md) | Planning or recording fault-injection drills (`scripts/chaos/`) and reconciliation after them (DPP-061/063). |
| Backup and restore | [backup-restore.md](backup-restore.md) | PostgreSQL/ClickHouse/S3 restore procedure and restore tests (`scripts/backup/`) (DPP-062). |
| Disaster recovery | [disaster-recovery.md](disaster-recovery.md) | Site or multi-component loss; RTO/RPO drill record (DPP-065, not yet confirmed). |
| Maintenance | [rolling-upgrade-and-reshard.md](rolling-upgrade-and-reshard.md) | Rolling upgrades, Kafka partition increase, ClickHouse shard add, migration rollout, service restarts (DPP-064, draft, not rehearsed). |
| Release | [../../release/readiness-review.md](../../release/readiness-review.md) | Production readiness status (currently NO-GO) and evidence gaps. |
| Handover | [../../release/handover.md](../../release/handover.md) | Component owners (TODO), dev stack start, env inventory by name. |

## Required runbooks before production operation

The following procedures are required before the related production capability
is enabled. They are intentionally not represented by placeholder operational
steps: each needs service-specific metrics, ownership, approved access methods,
and recovery validation.

| Area | Minimum incident scope |
| --- | --- |
| Kafka | Broker/topic unavailability, consumer lag, ACL or TLS failure, and safe replay boundaries. |
| PostgreSQL | Availability, connection exhaustion, migration failure, backup/restore, and tenant-safe diagnosis. |
| S3/object storage | Upload/download failure, retention or lifecycle error, encryption/access denial, and object recovery. |
| ClickHouse | Insert/query failure, replica or disk pressure, schema migration issue, and query-cost containment. |
| Replay | Authorization, tenant-scoped input selection, idempotency, rate limits, audit evidence, and stop/rollback conditions. |
| Secret compromise | Credential containment, revocation/rotation, blast-radius assessment, validation, notification, and evidence handling. |

Do not improvise destructive database, topic, object, or secret operations from
this index. Escalate to the named component owner until the corresponding
reviewed runbook exists.
