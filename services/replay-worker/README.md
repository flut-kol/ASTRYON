# replay-worker (DPP-043)

Executes the jobs that `services/replay-controller` publishes to `replay.jobs.v1` (key `tenant_id 0x1f source_id`, JSON `ReplayJob`).

## What a replay is here: COPY-FORWARD (minimal model)
The worker does **not** re-run a changed pipeline and does **not** touch raw storage. For the job's tenant/source/time ranges it reads
the de-duplicated events of the source's **previous processing_version** (highest stored version below the job's, i.e. the one the
controller planned against) from ClickHouse and re-publishes each event to `processed.events.v1` with `processing_version` = the job's NEW
version. The existing `clickhouse-sink` writes them. Re-processing with a new `pipeline_version` needs a real pipeline runner and is TODO.
Payloads are JSON-compacted (whitespace only; the envelope checksum must match the wire bytes), `trace_id` = `replay:<job_id>`.

Why aggregates are not doubled: the copy lives under a different `processing_version`. Every aggregate row, dedup (`LIMIT 1 BY tenant,
source, event_id, processing_version`) and reader selects by version, so the old and new versions are separate series; a reader cuts over
by selecting the new version (not implemented here). Old rows are never modified.

## Execution
- Consumer group `replay-worker`, auto-commit off, one job at a time; the offset is committed only after the job is durably handled.
- ClickHouse: HTTP, POST `readonly=1`, only bound `param_*` parameters; table `events_normalized_distributed`. Pages of `REPLAYW_PAGE_SIZE`
  (default 200) rows ordered by `(event_time, event_id)` with a keyset cursor and `LIMIT 1 BY`; memory is O(page) (response also capped at 128 MiB per page).
- Envelope = `libs/event-contracts` Envelope + normalization fields (`batch_id`, `pipeline_id`, `pipeline_version`, `event_type`, `attributes`,
  `quality_flags`, `raw_ref`); each is validated with `Envelope.ValidateAt` before publishing; Kafka key = `PartitionKey()`; producer acks=all, idempotent.
- Idempotence: `BuildRecord` is a pure function of (job, row): a redelivered job yields byte-identical records. Before publishing a page the worker asks
  ClickHouse which of its event_ids already exist under the NEW version and skips them, so a redelivery after the sink drained adds no rows.
  Limit: records published but not yet written by the sink are invisible to that check; a redelivery inside that window can create physical duplicate
  rows (the sink's token is offset-based), which every reader/aggregate already collapses with `LIMIT 1 BY ... processing_version`.
- Failure: transient (ClickHouse, Kafka, PostgreSQL) -> request set FAILED, offset NOT committed, the process exits (supervisor restarts; redelivery re-runs, status
  goes RUNNING again). Permanent (undecodable/invalid job, invalid source row, cross-tenant row) -> FAILED, offset committed (no endless redelivery).
- A job whose `replay_requests` row is missing, CANCELLED or COMPLETED is skipped (not executed).
- Tenant: only `job.tenant_id` (set by the controller from the verified principal). Message key/headers are never used; a returned row of another tenant aborts the job.

## Status (`StatusUpdater`, pg.go)
`REQUESTED|APPROVED|FAILED -> RUNNING -> COMPLETED|FAILED` (RUNNING is an atomic claim: a second worker gets `ErrNotRunnable`; a RUNNING row untouched for `RunningLease` (default 1h, must exceed the longest job) is treated as a crashed worker's and may be re-claimed by exactly one), one transaction per update: `set_config('astryon.tenant_id', <job tenant>, true)` (RLS,
migration 0001) then `UPDATE replay_requests ... WHERE tenant_id AND source_id AND target_processing_version`. The role must NOT bypass RLS.

## Env
`ASTRYON_KAFKA_BROKERS` (+ `_TLS`, `_TLS_CA_FILE`, mTLS `_TLS_CERT_FILE`/`_TLS_KEY_FILE` (both or neither, needs TLS; re-read when mtime/size change, with the CA file, every `_TLS_RELOAD_INTERVAL`, default 1m, 0 = off; a broken new file keeps the last good one, logged at most once a minute; used for new broker connections), `_SASL_MECHANISM/_USERNAME/_PASSWORD`, `_CLIENT_ID`, `_DELIVERY_TIMEOUT`), `REPLAYW_CH_URL` (required),
`REPLAYW_CH_DATABASE` (astryon), `REPLAYW_CH_USER`, `REPLAYW_CH_PASSWORD`, `REPLAYW_CH_TABLE`, `REPLAYW_PG_URL` (unset = no status tracking, warning),
`REPLAYW_GROUP_ID`, `REPLAYW_JOBS_TOPIC`, `REPLAYW_OUT_TOPIC`, `REPLAYW_PAGE_SIZE`.

## Verified
- Unit (fakes for Kafka source/sink, ClickHouse, status): copy-forward + dedup + paging, redelivery -> identical records, resume skips present events,
  failure -> FAILED and no commit, permanent failure -> FAILED and commit, non-runnable skipped, tenant only from job, job validation, SQL bound params/readonly, PG tenant-first (fake tx).
- Live (Kafka 3.9 + ClickHouse 24.8, 2026-10-20, isolated compose project, real `clickhouse-sink` and `ch-aggregator` binaries): 520 physical / 500 unique v1 events
  -> job v2 -> v2 has 500 rows = 500 unique source events, v1 unchanged (520/500); same job delivered again -> 0 events published, still 500; aggregates v1=500, v2=500 (not 1000).
  Live tests: `REPLAYW_IT_URL` (+`_USER/_PASSWORD/_DB`) in this module, `REPLAY_KAFKA_IT_BROKERS` in replay-controller.
- Found live: json.Marshal compacts payloads, so a checksum over the original text made the sink quarantine every event; fixed (checksum over compacted bytes) with a unit test.
- Live PostgreSQL 16 (2026-10-20, migrations 0001-0007, non-superuser NOBYPASSRLS role; `REPLAYW_PG_URL`, tests `TestPGLive*`): REQUESTED/APPROVED -> RUNNING -> COMPLETED sets
  completed_at and updated_at; FAILED only from RUNNING (sets completed_at) and RUNNING again after FAILED; COMPLETED -> COMPLETED/RUNNING/FAILED, CANCELLED -> anything, missing,
  soft-deleted and other-tenant requests are refused (`ErrNotRunnable`, rows byte-identical afterwards); RLS blocks cross-tenant UPDATE independently of the WHERE clause;
  16 concurrent claimers over 2 pools -> exactly 1 winner; stale-lease takeover -> exactly 1 winner. Bug found: the old RUNNING rule accepted RUNNING -> RUNNING, so two workers could both claim a request; fixed with the lease rule.
  Re-completion of a COMPLETED request is a no-op reported as `ErrNotRunnable` (the worker already tolerates it).
- Kafka SASL_SSL (SCRAM-SHA-512/256, PLAIN) verified live (`REPLAYW_KAFKA_IT_*`, `docs/operations/kafka-tls-sasl-test.md`); `ASTRYON_KAFKA_TLS` accepts only 1/true/0/false, a CA file or PLAIN without TLS is a config error. mTLS client certs and cert/CA rotation without restart verified live (`REPLAYW_KAFKA_MTLS_IT_*`, `kafka_mtls_it_test.go`). Not verified live: multi-worker rebalancing, ranges beyond one day-split.

## Publish is bounded by ctx
`KafkaSink.Publish` runs `ProduceSync` aside and returns `ctx.Err()` at the deadline (like the gateway's `franzProducer.produce`), so an unreachable broker cannot hold the worker beyond its context (previously bounded only by `ASTRYON_KAFKA_DELIVERY_TIMEOUT`, default 30s). Records may still land later; the offset is not committed on error and the redelivered job skips already-present events. Tests: `kafka_publish_ctx_test.go` (hung producer, real client against `127.0.0.1:1`).

## End-to-end test
`TestE2EControllerJobToCompleted` feeds the controller's golden job (`../replay-controller/internal/replay/testdata/job.v1.json`) through `Worker.Step` with fakes: events copied v1 -> v2, request RUNNING -> COMPLETED, offset committed. Activation is covered controller-side.
