# File ingestion

Foundation for Sprint 07 (`DPP-031` through `DPP-035`). The package is a
library-only implementation at present; it does not yet expose an HTTP server
or run distributed workers.

## Implemented contracts

- tenant-scoped multipart upload planning with bounded size/part count,
  allow-listed content types, SHA-256 validation and short-lived part URLs;
- bounded-memory CSV and NDJSON parsing with per-record errors suitable for
  quarantine;
- restart-safe NDJSON positions (`Offset`, `NextOffset`, record number) and a
  checkpoint that can only advance over the next completed record;
- deterministic NDJSON deduplication keys derived from the immutable source
  fingerprint and byte offset;
- explicit batch states and transition validation;
- lease acquire/heartbeat rules;
- bulk-result lineage for Parquet S3 or ClickHouse paths (not per-row Kafka).

For NDJSON, persist `Checkpoint.NextOffset` and `NextRecordNumber` only after
the normalized or quarantine result has been durably accepted. On restart,
verify `Checkpoint.SourceFingerprint` against the object checksum, range-read
from `NextOffset`, and call `NDJSONFrom`. A crash after the sink write but
before checkpoint persistence can replay one record; sinks must therefore use
`Record.DedupKey` as their idempotency key.

## Durable job store (`internal/jobs/store.go`)

PostgreSQL repository over `jobs`/`job_attempts` (migration 0004), kind `process_file`: `Claim` (`FOR UPDATE SKIP LOCKED`, only QUEUED and due),
`Heartbeat`, `SaveCheckpoint` (checkpoint in `payload.checkpoint` plus progress counters), `Succeed`, `Fail` (RETRYING with backoff while
`attempt_count < max_attempts`, else FAILED) and `Sweep` (expired RUNNING leases -> RETRYING/FAILED, due RETRYING -> QUEUED). Every claim increments
`attempt_count`, which is the **fence**: all writes compare owner, fence, RUNNING and an unexpired lease, so a stale worker gets `ErrFenced`
and cannot move a checkpoint or finish the job. A re-claim returns the last checkpoint for resume. Live test (`FILEJOBS_PG_URL`, non-superuser
role, migrations applied) proved claim exclusion, expiry fencing, resume and backoff on PostgreSQL 16.

## Worker (`internal/worker`)

`Worker.Run/RunOnce`: claim -> range-read from `Checkpoint.NextOffset` via `Source` -> `NDJSONFrom` -> batches (default 500) to an idempotent
`Sink.Accept(valid, invalid)` -> `SaveCheckpoint` only after the sink accepted. A heartbeat goroutine cancels the work on `ErrFenced`, and a fenced
worker writes nothing more. Failures: `Permanent(code)` -> FAILED; source fingerprint differing from the checkpoint -> `source_changed` (FAILED);
anything else -> retryable with backoff. Shutdown leaves the lease to expire (Sweep re-queues from the checkpoint). Tests with fakes (race-clean);
the concrete S3 `Source`, ClickHouse/Kafka `Sink` and a `cmd/` executable are not written yet.

## S3 and verified objects

`internal/s3`: stdlib SigV4 client (copy of export-worker's signer; extract to a shared lib when a third user appears): `CreateMultipart` with
pre-signed part URLs, `CompleteMultipart`, `Abort`, `Head`, `Get` (ranged; a non-206 answer to a Range request is an error), `Delete`.
`internal/objects`: `PGStore` over `file_objects` (tenant RLS), `Verify` (streams the object once, compares SHA-256 and size with the declared
values, then sets `committed_at`; on mismatch deletes the object and soft-deletes the row) and `Source` (worker.Source: serves only committed
objects of the job's tenant from the configured bucket; the verified SHA-256 is the checkpoint fingerprint). Live-proven on MinIO + PostgreSQL 16:
multipart round trip with a 5 MiB part, ranged read, abort, tampered upload rejected and deleted, unverified object refused.

## HTTP API (`cmd/file-api`, `internal/api`)

Scope `astryon:uploads:write`; tenant only from the Principal (bodies with `tenant_id` or any unknown field are rejected, 256 KiB cap).
- `POST /v1/files/uploads` `{source_id, filename, content_type, sha256, size_bytes, parts}` -> 201 `{file_object_id, upload_id, part_urls[], expires_at}`
  (pre-signed part URLs, clients upload straight to S3). 400 invalid, 409 `already_registered` (same tenant+filename+sha256), 422 `unknown_source`.
- `POST /v1/files/uploads/{id}/complete` `{upload_id, parts:[{number, etag}]}` -> completes the multipart upload, verifies SHA-256/size, commits the
  row and enqueues the `process_file` job (idempotent: repeating returns the same job). 422 `checksum_mismatch` (object deleted) / `upload_incomplete`.
Env: see `cmd/file-api/main.go` (`FILE_API_PG_URL`, `FILE_S3_*`, AAS + optional DPoP like query-api, dev token for local use only).
Proven end to end with the built binary against MinIO + PostgreSQL 16: create, part PUT, complete, job QUEUED, repeat idempotent, 401 without token.
Contract: `docs/api/file-ingestion.openapi.yaml`; errors in `docs/errors/api-error-catalog.md` (`{code, message}`). Not yet: rate limits/quotas, malware scan.

## Direct ClickHouse path (`internal/chsink`, `cmd/file-worker`)

Decision 2026-10-21: file rows go straight to ClickHouse, not through Kafka (DPP-035). `chsink.Sink` maps each valid NDJSON record to an
`events_normalized` row (`tenant_id` from the job, `source_id`/`batch_id` = the file object's source and id, `event_type` from config,
optional event-time / entity-id fields, `payload` = the raw record, `raw_ref` = `s3://bucket/key#byteOffset`, `processing_version` default 1;
a missing or unparseable event time falls back to ingest time with quality flag `event_time_defaulted`) and each unparseable or oversized record
to `processing_errors` (stage `file_parse`, code `ERR_JSON_MALFORMED` / `ERR_RECORD_TOO_LARGE`, no record content). `event_id` is a stable UUID
derived from fingerprint + byte offset; inserts use `insert_deduplication_token` = hash(tenant, file object, first/last offset, count), separate for events and errors.
A batch replayed after a crash between sink and checkpoint is therefore dropped by ClickHouse. Caveat: dedup relies on the batch being re-cut identically
(same checkpoint and `FILE_BATCH_SIZE`); changing the batch size mid-job can duplicate rows, which the reconciliation count vs `uniqExact(event_id)` would show.
`cmd/file-worker` wires store, S3 source, sink and a 10 s sweep (env in its header comment); mapping is per worker process for now (not per source or job).
Proven end to end with the built binaries on MinIO + PostgreSQL 16 + ClickHouse 24.8: upload via file-api, job claimed by the worker, 3 events and 1
error row in ClickHouse with correct offsets, job SUCCEEDED with `events_processed=3`, `events_failed=1` and the final checkpoint; three identical inserts of one batch
left 3 event rows and 1 error row.

## Operations

Both binaries serve Prometheus text on `FILE_METRICS_ADDR` (off when unset; keep it loopback or cluster-internal): `astryon_file_worker_*`
(jobs claimed/succeeded/failed/retried/fenced, batches, records, invalid records, sink errors) and `astryon_file_api_requests_total{route,code}`.
Alert rules are in `infra/observability/prometheus-rules.yaml` (group `astryon-file-ingestion`). One `Dockerfile` builds either binary
(`--build-arg CMD=file-api|file-worker`, context = repository root; both images built OK, 12 MB).

## Remaining work

- malware/content scanning, object lifecycle rules, rate limits/quotas for uploads;
- Parquet streaming and resumable CSV/Parquet checkpoints;
- per-source mapping/config for the sink (event type, time and entity fields), Parquet output;
- actual Parquet normalization, ClickHouse bulk ingest and durable lineage.
