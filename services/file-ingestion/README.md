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

## Remaining work

- malware/content scanning, object lifecycle rules, the HTTP endpoint tying `upload.Service`, `Register` and `Verify` together;
- Parquet streaming and resumable CSV/Parquet checkpoints;
- `Sink` (quarantine + normalized path, idempotent on `DedupKey`) and `cmd/file-worker`;
- actual Parquet normalization, ClickHouse bulk ingest and durable lineage.
