# Live verification checklist (not yet run)

Status (2026-10-21): section 1 and the export-worker parts of sections 3 and 4 were run on throwaway docker containers (results
below); the replay e2e (section 2) and the clickhouse-sink quarantine path with a real broker are NOT run. Record results in this file with a date.

## Results 2026-10-21
- **0014 on PostgreSQL 16 (migrations 0001-0013 applied first):** the pre-check found a duplicate (a live row plus a soft-deleted FAILED row with the
  same version); applying 0014 failed atomically (`could not create unique index`, `schema_migrations` stayed at 13); after deleting the duplicate it
  applied. With a NOSUPERUSER NOBYPASSRLS role all `TestPGLive*` of replay-controller passed (two allocators, 50 concurrent allocations distinct,
  duplicate insert rejected and classified as a conflict, failed publish burns its version).
- **export-worker live e2e (`EXPORT_E2E=1`, ClickHouse 24.8 + PostgreSQL 16 + MinIO):** `TestE2ELive` passed. `TestE2EFailureModes` first FAILED:
  after PostgreSQL was stopped for 8 s (longer than the 3 s lease) with running+queued jobs, 2 of 8 jobs stayed RUNNING forever. Cause: a run cancelled
  because no heartbeat reached the queue left its row RUNNING and owned; after PostgreSQL returned, the heartbeat renewed that lease again.
  Fixed in `dispatcher.go` (abandon after a lease-lost cancel and after a failed `Start`; `pg.go` un-abandons on re-claim), with unit tests;
  `TestE2EFailureModes` then passed (114 s).
- **Still failing:** `TestE2ETimeoutsAndLeaseLoss/zombie_finish_rejected_object_deleted` fails when the whole group runs (3 of 3 runs, also
  without the fix above) and passes alone (2 of 2). The job takes about as long as the 3 s lease, and a worker whose heartbeat cannot reach
  PostgreSQL now cancels itself after a full lease, so the zombie may never finish its object. The test premise needs revisiting; not changed.
- `POST /v1/exports` while PostgreSQL is down returns HTTP 500 (the test only asserts "not 202"); a 503 with `Retry-After` was not implemented.
- `EXPORT_CH_HEADER_TIMEOUT` (5 m) is still not validated against real long queries (the e2e covers idle timeout and max duration only).

## 1. Migration 0014 (`replay_requests_version_uq`)
1. On a copy of the target database run the pre-check from the migration header; it must return zero rows.
2. If rows are returned, resolve them manually (two replays wrote the same `processing_version`) before applying.
3. Apply 0014; start two concurrent replay requests for the same (tenant, source); expect distinct versions, no unique violation.
4. Insert a duplicate directly via `psql`; expect `23505`, and the controller to map it to a conflict, not a 500.
5. Rollback note: `DROP INDEX replay_requests_version_uq;`.

## 2. Replay end-to-end with real services
Currently `services/replay-controller/internal/replay/e2e_test.go` and `services/replay-worker/internal/worker/e2e_golden_test.go` use fakes.
1. Seed events in ClickHouse, create a replay via replay-controller, run replay-worker against real Kafka.
2. Expect: events re-published with the new processing version, ClickHouse sink stores them, replay row reaches its terminal status.
3. Kill replay-worker mid-run and restart; expect no loss and no duplicate publication beyond at-least-once.
4. Cancel mid-run; expect the worker to stop and the version to stay burned.

## 3. Quarantine and export-worker fixes
1. clickhouse-sink quarantine path (`kafka_quarantine_test.go`): publish poison messages to a real broker, expect them in the quarantine topic and the partition to keep advancing.
2. export-worker against real ClickHouse and S3: run a large export, expect an object with the full row count.
3. PostgreSQL failover during an export: stop the primary while a job finishes; expect the terminal write to be retried for up to `EXPORT_FINISH_RETRY` (2m) and the job to end in a correct state, not stuck RUNNING.

## 4. `EXPORT_CH_HEADER_TIMEOUT` (default 5m)
1. Run the slowest realistic export query (largest tenant, widest time range) and note time to response headers.
2. The default is unvalidated: confirm it is above the worst observed value with margin, otherwise raise it.
3. Set a tiny value (e.g. `1s`) against a slow query; expect a clean timeout failure and no partial object left in S3.
