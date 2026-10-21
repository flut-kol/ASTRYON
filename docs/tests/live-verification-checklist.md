# Live verification checklist (not yet run)

Status: NOT RUN. Everything below is covered only by unit/fake-based tests today (`make test` passes). Each item needs a
throwaway compose project (Kafka, ClickHouse, PostgreSQL, S3-compatible store). Record results in this file with a date.

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
