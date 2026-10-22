# Backup and restore (DPP-062)

Scope: PostgreSQL control plane (pgBackRest), ClickHouse (native BACKUP/RESTORE), critical S3 metadata. Restores
happen only in a clean, isolated environment first; never restore over a live data directory or database.
Backups are independent of replication (plan §7): replicas do not replace them.

## Restore-test scripts (throwaway containers, dry-run by default)

| Script | Covers |
| --- | --- |
| `scripts/backup/pg-restore-test.sh [--execute]` | pgBackRest stanza `astryon` (config mirrors `infra/postgres/pgbackrest.conf.tmpl`, but `repo1-type=posix`), full backup, post-backup WAL replay, restore into a new data dir, per-table row counts and RLS-table count compared |
| `scripts/backup/ch-restore-test.sh [--execute]` | `BACKUP DATABASE astryon` to a file target, `RESTORE` into a fresh empty ClickHouse 24.8, count + hash per MergeTree table; sample rows read-only from dev `astryon_clickhouse` |

Both use no published ports, random passwords, and remove their containers/volumes on exit.

## PostgreSQL production procedure (pgBackRest, S3 repo)

1. Render `pgbackrest.conf` through the secret manager (placeholders in the template are not defaults).
2. Provision an isolated host/namespace with no production clients; `pgbackrest --stanza=astryon info` to pick the set.
3. `pgbackrest --stanza=astryon --pg1-path=<new dir> restore` (add `--type=time --target=...` for PITR), start without
   client access, wait for recovery to end.
4. Smoke: migration version (`schema_migrations`), RLS enabled on tenant tables, a NOBYPASSRLS app role sees only its
   tenant, `ingestion_batches` / `sink_checkpoints` readable.
5. Record backup timestamp, recovered LSN, elapsed time, gaps. Destroy the environment after sign-off.

## ClickHouse production procedure

1. Back up with `BACKUP DATABASE astryon TO S3(...)` (or a configured backup disk); credentials from the secret manager.
2. Restore into a clean cluster/namespace with `RESTORE DATABASE astryon FROM ...`.
3. Run `ch-aggregator reconcile` against the restored data and the restored PostgreSQL registry; differences larger than
   the data written after the backup point are findings (events after the backup point are recovered by Kafka replay
   within retention, see `replay.md`).

## S3 critical metadata

TODO: define which buckets/keys are critical (file manifests, flink checkpoints, export artifacts), versioning/replication
policy and a restore drill. Unrun.

## Results

| Test | Date | Result |
| --- | --- | --- |
| `pg-restore-test.sh --execute` (postgres:16-alpine, pgBackRest 2.58.0, posix repo) | 2026-10-21 | PASS: 30 tables equal, 29 RLS tables equal, post-backup marker row recovered via WAL replay; full backup 28 s, restore+start 4 s on a ~31 MB database |
| `ch-restore-test.sh --execute` (ClickHouse 24.8, File() target) | 2026-10-21 | PASS: 1366 sample rows, count+hash equal on all MergeTree tables; backup and restore each under 1 s (second granularity) |
| PostgreSQL restore from the S3 repository | unrun, TODO (no S3 pgBackRest repo in dev; s3mock compatibility untested) |
| ClickHouse backup to S3/Ceph, multi-shard/replicated tables | unrun, TODO |
| S3 metadata restore | unrun, TODO |

| Item | Value |
| --- | --- |
| Backup frequency / retention (full 2, diff 7 from template) | as templated; schedule TODO |
| RPO (PG: WAL archive lag; CH: backup interval) | TODO (measure) |
| RTO (restore of production-size data) | TODO (measure; the dev timings above are not representative) |
