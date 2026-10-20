# ASTRYON database schemas

Implements DEVELOPMENT_PLAN §6 (PostgreSQL control plane), §7 (ClickHouse) and §5.3 (data semantics). DPP-028.

## Layout
- `postgres/migrations/NNNN_name.up.sql` forward-only; each file starts with a `-- ROLLBACK PLAN:` header.
- `clickhouse/migrations/NNNN_name.up.sql` forward-only, with template tokens (see below).
- `seeds/` dev/test seed data only (no secrets).
- `apply.sh [postgres|clickhouse|seeds|all]` applies pending migrations in order; tracked in a `schema_migrations` table in each DB. Config only from env: libpq vars or `DATABASE_URL`; `CH_HOST CH_PORT CH_USER CH_PASSWORD CH_DATABASE CH_CLUSTER CH_REPLICATED`.

## PostgreSQL tables
| Migration | Tables |
|---|---|
| 0001 tenancy/identity | tenants, principals, roles, role_bindings, api_keys |
| 0002 sources/connectors | sources, source_credentials, source_schemas, connectors, connector_runs |
| 0003 pipelines | pipelines, pipeline_versions, processing_rules |
| 0004 batches/files/jobs | ingestion_batches, file_objects, jobs, job_attempts |
| 0005 retries/replay | scheduled_retries, replay_requests |
| 0006 sinks/quotas | sink_configs, sink_checkpoints, tenant_quotas |
| 0007 audit/outbox | audit_log (append-only), outbox, notifications |

## PostgreSQL conventions
- uuid PKs (`gen_random_uuid()`), all timestamps `timestamptz` (UTC).
- `tenant_id` on every tenant-scoped table; cross-table FKs are composite `(tenant_id, x_id)` so a row cannot reference another tenant's parent. Every FK column is indexed.
- Row-level security is enabled and forced on all tables via `astryon_enable_rls()`, keyed on `current_setting('astryon.tenant_id')`. Set it per transaction (`SET LOCAL`) from validated token claims only, never from headers/body. `astryon.platform_admin='on'` is a trusted-worker escape hatch (migrations, seeds, outbox relay). The application DB role must not be a superuser or have BYPASSRLS.
- Soft delete (`deleted_at`) on configuration entities; partial unique indexes ignore deleted rows. Operational/history tables are not soft-deleted.
- `api_keys` stores only a hash and a display prefix. `source_credentials` stores only a secret reference (`scheme://...`), enforced by a CHECK. Principals reference the external AAS subject (`aas_sub`); no passwords.
- `jobs.status` enum: NEW, QUEUED, RUNNING, RETRYING, SUCCEEDED, FAILED, CANCELLED.
- No per-event status anywhere in PostgreSQL: only batch/job metadata, aggregate counters, bounded rejection samples.
- Rollback: forward-only; the header in each file documents the non-prod revert and the prod approach.

## ClickHouse tables
events_normalized_local, events_normalized_distributed, processing_errors_local, processing_quality_daily, source_metrics_hourly, pipeline_metrics_hourly. (`entities_index` deferred, "if needed" per §7.)

Template tokens substituted by `apply.sh`: `__ON_CLUSTER__` (empty unless `CH_CLUSTER` set), `__RMT__` (`MergeTree()` by default; `ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}','{replica}')` with `CH_REPLICATED=1`, needing macros and Keeper), `__CLUSTER__` (Distributed cluster name, default `default`).

Dedup / idempotent sink (details in `0001_events_normalized.up.sql`): deterministic insert blocks with `insert_deduplication_token`, a dedup window on the table, commit position in `sink_checkpoints`, new `processing_version` for replay, reconciliation, and revision-based aggregates. ReplacingMergeTree is deliberately not relied on. Aggregates are written by the aggregation job (not MVs on the insert path).

## Validation status
See the task report: validated against throwaway postgres:17 and clickhouse-server:24.8 containers on a single node (non-replicated mode). Replicated mode and multi-node Distributed were not exercised.
