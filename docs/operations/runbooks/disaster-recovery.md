# Disaster recovery (DPP-065)

Status: **plan not confirmed.** The single-node dev stack cannot establish cross-site RPO/RTO
(`docs/architecture/failure-model.md`). This runbook defines the drill and the fields to fill with measured numbers.

## Scenarios

| Scenario | Primary mechanism | Depends on |
| --- | --- | --- |
| Loss of one component (broker, CH node, PG primary) | HA failover; see `chaos-gamedays.md` | replicas in staging |
| Loss of a site | Restore into the DR site from backups + Kafka replay | `backup-restore.md`, cross-site copy (TODO) |
| Logical corruption / bad deploy | PITR (PG), `replay.md` (CH via new processing_version), `../release-rollback.md` | backups, retention |

## Recovery order (draft, to be proven in a drill)

1. Secrets and identity: secret manager reachable; external AAS reachable (`docs/architecture/authentication.md`). Auth is fail-closed, so no ingestion until AAS works.
2. PostgreSQL control plane from pgBackRest; verify migrations, RLS, tenants/sources.
3. Kafka cluster and topics (`scripts/dev/create-topics.sh` is dev only; production topics from IaC); decide offset/replay boundary.
4. ClickHouse from backup; then re-ingest or replay events after the backup point.
5. Flink from the last checkpoint/savepoint (S3), then sinks; clickhouse-sink resumes from `sink_checkpoints`.
6. Gateway and APIs last. Run `ch-aggregator reconcile` (snapshot compare) before declaring recovery.

## Drill record

| Field | Value |
| --- | --- |
| Drill date / environment | TODO (not performed) |
| RTO target | TODO (plan §2 SLO to be confirmed) |
| RTO measured | TODO |
| RPO target | TODO |
| RPO measured | TODO |
| Data gaps found by reconcile | TODO |
| Participants / sign-off | TODO |

Gate (plan Sprint 13): all critical failover and restore runbooks executed and signed. Not met until the TODO rows
above and in `chaos-gamedays.md` / `backup-restore.md` are filled from real drills.
