# Rolling upgrade and reshard/rebalance (DPP-064)

Status: **draft procedure, derived from repo code/config, not rehearsed anywhere.** Every step below is marked
`not rehearsed` unless a dated repo test is cited. Secret rotation has its own runbook: [secret-rotation.md](secret-rotation.md).
Rollback rules and schema compatibility: [../release-rollback.md](../release-rollback.md). Restore: [backup-restore.md](backup-restore.md).

## Preconditions for any maintenance

- Fresh backups verified (`backup-restore.md`); error budget and on-call known; no open Sev1/Sev2 (`../incident-management.md`).
- Pre-snapshot: `ch-aggregator reconcile --snapshot pre.json` (read-only; see `services/ch-aggregator/README.md`). After the change run
  `reconcile --compare pre.json`; any new finding aborts the change.
- No replay in flight (`replay.md`) and no `retry-controller` backlog growing (`astryon_retry_backlog_pending`).
- Record consumer lag for `sink` group on `processed.events.v1` and the Flink group (`PROCESSING_KAFKA_GROUP_ID`, default
  `astryon-processing-v1`) before starting.
- Abort condition at every step: an alert from `infra/observability/prometheus-rules.yaml` fires or reconcile diverges; revert that step only.

## 1. Migration rollout (PostgreSQL and ClickHouse)

Rules come from `release-rollback.md` (expand -> migrate -> contract; code works with schema N and N-1).

1. Gate in CI and locally: `scripts/check-migrations.sh`. It enforces, for `db/postgres/migrations` and `db/clickhouse/migrations`:
   contiguous `NNNN_` numbering, first line `-- ROLLBACK PLAN:`, and no `DROP TABLE/COLUMN/INDEX/TYPE`, `TRUNCATE`, `DELETE FROM`
   outside comments unless the file contains `-- DESTRUCTIVE-OK: <reason>` (the script still prints the error in that case but does not fail).
   (CI job `repo-gates`; not yet run on GitHub.)
2. Take a backup (`backup-restore.md`). Ship the schema **before** the code that needs it; expand-only migrations first.
3. Apply with `db/apply.sh` (env only; no secrets in repo):
   - `db/apply.sh postgres`: per migration one transaction (`BEGIN; <file>; INSERT INTO schema_migrations; COMMIT`), skips names already
     recorded. A failed migration rolls back fully and stops (`ON_ERROR_STOP=1`); fix forward with a new numbered file. Do not edit an applied file.
   - `db/apply.sh clickhouse`: **not transactional** (DDL, then the `schema_migrations` insert is a separate statement). A crash between them
     leaves the object created but unrecorded; the DDL is `IF NOT EXISTS`, so rerun is expected to be safe but check the file before rerun.
     On a cluster set `CH_CLUSTER` (adds `ON CLUSTER`) and `CH_REPLICATED=1` (ReplicatedMergeTree; needs `{shard}`/`{replica}` macros and Keeper).
     `schema_migrations` is created per node by `CREATE TABLE IF NOT EXISTS ... MergeTree` without ON CLUSTER: apply from one place and
     verify all nodes have the new objects (`SELECT hostName() ... FROM clusterAllReplicas('<cluster>', system.tables)`).
4. Roll services (section 4) only after the migration is verified on every node. Rolling services back does not require a schema rollback.
5. Contract (drop old objects) only in a later release with `DESTRUCTIVE-OK` and an explicit review.
6. Prod schema rollback = new forward migration; `DROP` in ROLLBACK PLAN lines is non-prod only.

Notes from the repo: the Postgres tenant RLS is forced, and the application role must not be superuser/BYPASSRLS; run migrations with the
migration role, not the app role. `CHSINK_INTENT_TABLE` (migration 0004) is opt-in; turning it on needs the table on every shard the sink writes to.
Status: `not rehearsed` against HA PostgreSQL (Patroni) or a multi-node ClickHouse cluster; `apply.sh` was validated on single-node containers only (`db/README.md`).

## 2. Kafka: partition increase and broker upgrade

Facts from code/config:

- Topics: `infra/kafka/topics.yaml` (dev: 6/3 partitions) and `libs/event-contracts/topics.go` (target: 96 for raw/validated/processed, 24 for the rest;
  hypothesis "to be set after POC benchmarks"). Partition count only grows; Kafka cannot shrink it.
- Message key = `tenant_id 0x1f entity_key`, or `tenant_id 0x1f event_id` when `entity_key` is empty (`event-contracts.PartitionKey`,
  used by the gateway for `raw.events.v1`; Flink keeps `raw.key()` when present, else recomputes the same key for `processed.events.v1`).
  Partitioning is `hash(key) mod N`: **raising N remaps keys to different partitions.**
- Ordering is guaranteed only per entity_key within one partition. After the increase, events of one entity written before and after the change can
  sit in different partitions, so cross-boundary ordering for entities with `entity_key` is lost for the moment the old partition still has a backlog.
  Events without `entity_key` have no ordering promise.
- Dedup does not depend on the partition:
  - gateway retries use the same `Idempotency-Key` (stable `event_id`); a retry that lands in a different partition after the change is not removed by
    Kafka's idempotent producer, but by Flink's `BoundedDeduplicateFunction` (`keyBy` on `tenant_id|source_id|event_id|processing_version`,
    fixed window `PROCESSING_DEDUP_WINDOW`, default 24h; state survives savepoints).
  - clickhouse-sink dedup token is `sha256(topic, partition, first, last offsets)` (per-partition), so new partitions get new tokens; old partitions keep theirs.
    The same logical event written twice to different partitions is NOT caught by the token; readers must use `LIMIT 1 BY (tenant_id, source_id, event_id, processing_version)`
    and `reconcile` duplicate check covers the rest.
  - `raw_ref` is `kafka:<topic>/<partition>/<offset>`; it records the original coordinates and does not change for existing rows.
- Consumers: clickhouse-sink uses a franz-go consumer group, auto-commit disabled, offsets committed after a successful CH insert; a rebalance redelivers
  uncommitted messages and `sink_chunk_intents_local` (if `CHSINK_INTENT_TABLE` is on) keeps re-cut boundaries per `(topic, partition)`, so the token matches. Flink `KafkaSource` starts from
  `committedOffsets(EARLIEST)`; the code sets no `partition.discovery.interval.ms`, so **verify the connector default before relying on it**:
  if discovery is off, a restart (from savepoint) is required for Flink to read new partitions.

Procedure to add partitions (`not rehearsed`):

1. Decide the new count from benchmark evidence (`docs/benchmarks/`), not the plan hypothesis. Prefer a multiple of the current count only for operator clarity; Kafka remaps either way.
2. Pick a low-traffic window. Not during replay. Stop or pause nothing for the increase itself, but expect a rebalance of the sink group.
3. Increase the topic upstream-first? Order: `raw.events.v1` first (gateway producers; Flink consumes), then `validated.events.v1` if used, then `processed.events.v1`
   (Flink producer; sink consumes). Update `topics.yaml` and `topics.go` to match in the same change (they are the declarative source).
4. After each topic: confirm every consumer group has assignments for new partitions (Flink: restart from savepoint if discovery is off, section 4);
   sink lag per partition returns to normal; no partition skew alert.
5. Run `ch-aggregator reconcile --compare pre.json`. Expect no new `missing`; duplicates are reported by the duplicate check if the dedup window was exceeded.
6. Rollback: partitions cannot be removed. The mitigation is to stop producing the new key distribution (not available as a switch) or to create a new topic
   and cut over via the replay mechanism; treat the increase as one-way.

Reassigning partitions across brokers (rebalance) and broker rolling upgrade (`not rehearsed`; chaos only killed brokers in dev, see `chaos-gamedays.md`):
one broker at a time, wait for zero under-replicated partitions and for ISR >= `min.insync.replicas` (prod 2, RF 3 per `topics.yaml`) before the next;
reassignments throttled, one topic at a time. Producers use `acks=all` and sink producer `AllISRAcks`, so a lost ISR shows as gateway `PUBLISH_FAILED` 503
(`ingestion-failures.md`), clients retry with the same `Idempotency-Key`.

## 3. ClickHouse: shard add and rebalance

Facts from code/config:

- Tables: `events_normalized_local` + `events_normalized_distributed`, `processing_errors_local` + `processing_errors_distributed`
  (migrations 0001, 0005). Distributed sharding key: `cityHash64(tenant_id, source_id)` over cluster `__CLUSTER__` (`CH_CLUSTER`, default `default`).
  The 0001 header says the key is "to be validated against tenant skew".
- The sink writes with a per-chunk `insert_deduplication_token`; the table keeps a dedup window (`non_replicated_deduplication_window = 10000`; replicated windows via ReplicatedMergeTree defaults).
  Multi-shard dedup was verified only on a 2-shard non-replicated cluster (`TestIntegrationDedupTokenMultiShard`, 2026-10-21) and replicas on 2 replicas + Keeper (2026-10-21).
- Consequence of adding a shard: `cityHash64(...) % total_weight` changes, so **new rows for an existing (tenant, source) may route to a different shard than its old rows**.
  A retry of an already-inserted chunk (same token) that now routes to another shard is not deduplicated by that shard (dedup is per-shard-table); reads via the Distributed table
  then need `LIMIT 1 BY` (already the contract). Avoid cutting over during a sink backlog or retry loop; drain lag first.

Procedure (`not rehearsed`; no multi-shard rebalance has ever been run):

1. Provision the new shard (empty), with macros (`{shard}`, `{replica}`), Keeper access and the same users/profiles. Add it to the `remote_servers` cluster definition.
   Whether sink HTTP endpoints / `CHSINK_*` connection settings need change depends on deployment; the sink inserts through the Distributed table or a node configured by env, check the active config first.
2. Create schema on the new shard: rerun `CH_CLUSTER=<cluster> CH_REPLICATED=1 db/apply.sh clickhouse` is **not enough** for a node that joined later (migrations are marked applied
   in `schema_migrations`). Create the local tables and Distributed tables by hand from the migration DDL (substitute the tokens as `apply.sh` does) or copy the DDL via `SHOW CREATE TABLE`
   from an existing shard. Verify with `system.tables` on every node.
3. Weights: for new data only, keep the new shard at a low weight at first if the cluster config supports it; do not move existing partitions unless planned.
4. Existing data stays where it is; reads go through the Distributed table so no data move is needed for correctness. Rebalancing history is optional and, if done,
   happens by `INSERT INTO ... SELECT` into `events_normalized_distributed` for a closed partition (month, `toYYYYMM(event_time)`) followed by `ALTER ... DROP PARTITION` on the source.
   That procedure is untested and the drop is destructive: require a fresh backup, a `DESTRUCTIVE-OK`-style review, and reconcile checksums before and after. The repo has no rebalance script.
5. Verify: `reconcile --compare pre.json` (note: multi-shard tables were not covered by earlier reconcile validation), per-shard row counts, duplicate check, ack/lag alerts.
6. Replica add/upgrade within a shard: one replica at a time, wait for an empty replication queue (`system.replication_queue`) and non-growing merges.

## 4. Service rolling restarts: offset and lease semantics

| Service | State/semantics (from code) | Safe restart | Status |
| --- | --- | --- | --- |
| gateway | stateless; clients retry `PUBLISH_FAILED` with same `Idempotency-Key`; SIGTERM shuts HTTP down with a deadline (`cmd` main) | helm `RollingUpdate maxUnavailable: 0, maxSurge: 1` (`infra/kubernetes/charts/astryon-services/templates/services.yaml`); PDB `maxUnavailable: 1` when replicas > 1 | not rehearsed |
| Flink `processing` | checkpoints to S3 (`PROCESSING_CHECKPOINT_URI`), EXACTLY_ONCE checkpoints, sink to Kafka AT_LEAST_ONCE, start `committedOffsets(EARLIEST)`; dedup state keyed (24h window) | stop with savepoint, deploy, restore from savepoint; parallelism change = same, key-group state is redistributed. Keep operator `uid`s (`validate-normalize-v1`) unchanged. Duplicate re-emission after restart is absorbed by dedup and the sink | savepoint cancel/restore verified once in dev 2026-10-22 (`services/processing/README.md`); parallelism change and TaskManager kill not rehearsed |
| clickhouse-sink | consumer group, manual commit after insert; SIGTERM: stop polling, flush with fresh context within `CHSINK_SHUTDOWN_TIMEOUT` (30s), unflushed messages are not committed | rolling one pod at a time; partitions move on rebalance; at most one batch per partition is redelivered and the intent table (opt-in) or the token dedups it. Wait for lag to recover before the next pod | kill -9 and crash tests in dev (`docs/tests/sink-crash-restart.md`); k8s rolling not rehearsed |
| file-ingestion worker | PostgreSQL job lease (`FILE_LEASE_SECONDS` 60, heartbeat lease/4), `attempt_count` is the fence, checkpoint saved only after sink accepted; shutdown leaves the lease to expire, `Sweep` re-queues from the checkpoint | rolling restart is safe; expect up to one lease of delay for in-flight files; stale worker gets `ErrFenced` | PG16 live test of fencing/resume; rolling not rehearsed |
| file-api | stateless; SIGTERM stops intake and drains audit writer | rolling | not rehearsed |
| retry-controller | fenced lease per claim (`locked_by`, `locked_until`, 60s); `FOR UPDATE SKIP LOCKED` | rolling; a killed pod's claims become reclaimable after the lease | smoke-tested in dev only |
| connector-manager | fenced lease per connector (`Claim`/`Renew`, `ErrFenced`); cursor kept on failure | rolling; takeover after lease expiry | PG16 live-proven for lease takeover; rolling not rehearsed |
| export-worker | per-job lease (`EXPORT_LEASE_SECONDS` 60); SIGTERM waits 25s, then cancels and marks this replica's unfinished jobs FAILED `shutdown`; QUEUED unstarted jobs re-queued up to 3 attempts, a started job is never re-run (FAILED `worker_lost`) | drain before restart; tell users running exports may fail | not rehearsed |
| replay-worker / replay-controller | job claim is atomic RUNNING; `REPLAYW_RUNNING_LEASE` (1h) before a crashed job can be re-claimed | **do not restart during a replay**; wait for COMPLETED/FAILED (`replay.md`) | not rehearsed |

Order for stateless/consumer services (after sections 1 to 3 are done): canary one instance, watch error rate/ack errors/lag, then roll the rest.
Producers before or after consumers does not matter for the event path because the envelope contract is additive-only (`release-rollback.md` rule 5).
Config toggles (`CHSINK_INTENT_TABLE`, `FILE_RETRY_MODE`, etc.) are env changes on restart, not runtime flags.

## Rolling upgrade order (draft)

| Step | Component | Gate before next step |
| --- | --- | --- |
| 1 | PostgreSQL (Patroni): upgrade replicas, switchover, upgrade ex-primary; then migrations (section 1) | replication lag ~0, app connects via PgBouncer |
| 2 | Kafka brokers one at a time (controllers per KRaft rules) | no under-replicated partitions, consumer lag recovers |
| 3 | ClickHouse replicas one at a time; then schema (section 1) | replication queue empty, merges not backlogged |
| 4 | Flink: savepoint, deploy new job version, restore from savepoint | checkpoints succeed, no sustained backpressure |
| 5 | Stateless services and consumers (section 4) with canary | error rate, ack errors and lag flat |

HA components (Patroni, PgBouncer, KRaft quorum, Ceph) are not deployed from this repo in a tested form; their steps are generic and `not rehearsed`.

## Results

| Item | Value |
| --- | --- |
| Rolling upgrade rehearsed (staging) | unrun, TODO |
| Kafka partition increase rehearsed | unrun, TODO (verify Flink partition discovery effective value first) |
| ClickHouse shard add/rebalance rehearsed | unrun, TODO (no multi-shard rebalance tooling in repo) |
| Migration rollout on HA PG / multi-node CH | unrun, TODO |
| Maintenance-window RTO impact | TODO (measure) |
