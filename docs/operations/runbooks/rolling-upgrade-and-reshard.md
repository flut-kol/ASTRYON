# Rolling upgrade and reshard/rebalance (DPP-064)

Status: **draft procedure, never executed.** Secret rotation already has its own reviewed runbook:
[secret-rotation.md](secret-rotation.md). General rollback rules: `../release-rollback.md`.

## Preconditions for any maintenance

- Fresh backups verified (`backup-restore.md`); error budget and on-call known; no open Sev1/Sev2.
- `ch-aggregator reconcile --snapshot pre.json` taken. Tenant isolation checks unchanged by the change.
- Migrations are forward-only with a rollback note; app versions must be compatible with schema N and N+1.

## Rolling upgrade order (draft)

| Step | Component | Gate before next step |
| --- | --- | --- |
| 1 | PostgreSQL (Patroni): upgrade replicas, switchover, upgrade ex-primary | replication lag ~0, app connects via PgBouncer |
| 2 | Kafka brokers one at a time (controllers per KRaft rules) | no under-replicated partitions, consumer lag recovers |
| 3 | ClickHouse replicas one at a time | replication queue empty, merges not backlogged |
| 4 | Flink: savepoint, deploy new job version, restore from savepoint | checkpoints succeed, no sustained backpressure |
| 5 | Stateless services (gateway, sinks, APIs, workers) with canary | error rate and ack errors flat |

Abort condition at every step: alert from §13 fires or reconcile diverges from the pre-snapshot; roll back that step only.

## Reshard / rebalance

- Kafka partition reassignment: throttled, one topic at a time, never during a replay; verify partition skew alert clears.
- ClickHouse reshard: new shard added empty; no row movement for existing partitions unless planned; Distributed
  tables (`events_normalized_distributed`) and the `__CLUSTER__` templating in `db/clickhouse` define the topology.
  Verify with `reconcile` including checksum compare. TODO: tested procedure for moving partitions.
- Flink parallelism change: savepoint and restore with new parallelism; keyed state redistribution checked against reconcile.

## Results

| Item | Value |
| --- | --- |
| Rolling upgrade rehearsed (staging) | unrun, TODO |
| Reshard/rebalance rehearsed | unrun, TODO |
| Maintenance-window RTO impact | TODO (measure) |
