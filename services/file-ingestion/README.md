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
the concrete S3 `Source`, ClickHouse `Sink` and `cmd/file-worker` are below.

### CSV in the worker

The parser is chosen per job from the upload record's `content_type` (`text/csv` -> CSV, anything else NDJSON; `jobs.Job.ContentType`).
`parse.CSVFrom` reports absolute byte offsets (quoted newlines stay inside their record, so a checkpoint never splits a field), and the
checkpoint is the same `Offset/NextOffset/RecordNumber` plus `Header` (column names only, never values) so a resume range-reads from
`NextOffset` and still knows the columns. Record 1 is the header: it advances the checkpoint but is not a data row and does not count towards
`MaxRecords`; an unparseable or >1024-column header fails the file with `csv_header_invalid`, a checkpoint past the header without names with
`bad_checkpoint`. Each data row becomes a JSON object keyed by header (BOM stripped, blank -> `colN`, repeats -> `name_2`) so profile checks
(`RequiredFields`, `MaxColumns`), the ClickHouse sink and `DedupKey` work exactly as for NDJSON; a row with the wrong cell count or invalid quoting
goes to `processing_errors` as `ERR_CSV_MALFORMED` (no content). Classification uses `classify.ScanCSV(header, cells)`, so labels name header
columns (never values); pre-scan, quarantine/block/geo/PII policies, `fail_file` profiles and in-flight tagging behave as for NDJSON. Memory: one record
plus the header. Blank lines are malformed rows. Live e2e: `FILEINGEST_E2E=1 go test ./internal/e2e` (throwaway postgres:16 + clickhouse 24.8 + MinIO on free ports, all migrations, non-superuser role, HTTP API -> worker -> ClickHouse; covers CSV, credentials quarantine without values, geo profile, malformed row -> processing_errors; containers removed). The upload's `content_type` is persisted on `file_objects` (it was previously dropped, so every file ran as NDJSON).

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
Contract: `docs/api/file-ingestion.openapi.yaml`; errors in `docs/errors/api-error-catalog.md` (`{code, message}`). Upload quotas: see below.
The Go `internal/scanner` package contains bounded HTTP adapters for Windows Defender and ClamAV, normalized verdicts
(`clean`, `infected`, `encrypted`, `error`), staging limits and a retry-aware circuit breaker. The adapters never return
raw engine output and never log file contents. Wiring a scanner into the upload/worker pipeline remains an explicit
deployment choice; an unavailable scanner must fail closed or remain retryable according to the deployment policy,
never silently mark an unscanned object clean.

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

**Retry mode** (`FILE_RETRY_MODE`, default `local`). `local` keeps the built-in `jobs.Store.Fail` (RETRYING with exponential backoff, FAILED when spent) plus the 10 s Sweep, which also recovers vanished workers; it already covers transient vs permanent. `controller` (`internal/retrybridge`) replaces only `Fail`: retryable failures (S3/ClickHouse/PostgreSQL outages) go to `retryclient.Client.Fail` as `Transientf` (jittered backoff in `scheduled_retries`, `FILE_RETRY_BASE_SECONDS` 5, `FILE_RETRY_MAX_SECONDS` 900, `FILE_RETRY_MAX_ATTEMPTS` 10, capped by the job's `max_attempts`), permanent ones (malformed/unsupported, policy block, source changed, row limit) as `PermanentErr` -> dead-letter (FAILED). Claim, heartbeat, fenced checkpoint writes, QUARANTINED and Sweep are unchanged, so a retried job resumes from `payload.checkpoint`. The client's attempt count is aligned with the claim-time count (rewound by one under the fence, `job_attempts` row closed by the bridge); a stale fence maps to `ErrFenced`; if a client cannot be built the failure falls back to the local path. No Kafka dead-letter sink is wired yet. Tests: `internal/retrybridge` (fake client); no live PG test for the bridge yet.
Proven end to end with the built binaries on MinIO + PostgreSQL 16 + ClickHouse 24.8: upload via file-api, job claimed by the worker, 3 events and 1
error row in ClickHouse with correct offsets, job SUCCEEDED with `events_processed=3`, `events_failed=1` and the final checkpoint; three identical inserts of one batch
left 3 event rows and 1 error row.

The env-gated `FILEINGEST_E2E=1 go test ./internal/e2e` also uploads a real multi-row-group Parquet object through the HTTP multipart API and verifies the
checksum-committed object in MinIO/PostgreSQL, the SUCCEEDED job counters and row-group checkpoint in PostgreSQL, normalized rows in ClickHouse, and the
worker's emitted batch-lineage contract. Batch lineage is still memory/log only: there is no PostgreSQL lineage table or recorder, so it is not durable across
worker restarts. The E2E keeps this gap explicit instead of claiming PostgreSQL lineage persistence.

## Operations

Both binaries serve Prometheus text on `FILE_METRICS_ADDR` (off when unset; keep it loopback or cluster-internal): `astryon_file_worker_*`
(jobs claimed/succeeded/failed/retried/fenced, batches, records, invalid records, sink errors) and `astryon_file_api_requests_total{route,code}`.
The file API queues `upload.create`, `upload.complete`, and validated-principal `auth.denied` records through the bounded `libs/audit` PostgreSQL
writer using `FILE_API_PG_URL`; invalid-token 401s have no trusted tenant and are deliberately not persisted. The record contains only the validated
tenant/actor/ACR, upload UUID, trace/request IDs, action and outcome—never filename, tags, object payload, PII or secrets. Tune bounds with
`FILE_AUDIT_QUEUE_SIZE` (max 65536), `FILE_AUDIT_BATCH_SIZE` (max 1000), `FILE_AUDIT_FLUSH_MS`, `FILE_AUDIT_WRITE_TIMEOUT_MS`,
`FILE_AUDIT_RETRIES` (0..10) and `FILE_AUDIT_DRAIN_SECONDS`; queue/write drops are logged without identity data and exposed as
`astryon_file_api_audit_{queued,written,dropped,rejected}_total`. SIGINT/SIGTERM stops HTTP intake, drains the writer for the configured bound,
then closes PostgreSQL. There is no cancel endpoint yet, hence no synthetic `upload.cancel` event.
Alert rules are in `infra/observability/prometheus-rules.yaml` (group `astryon-file-ingestion`). One `Dockerfile` builds either binary
(`--build-arg CMD=file-api|file-worker`, context = repository root; both images built OK, 12 MB).

## Malware scanner and archive password policy

Scanner providers are separate sources, not a replacement for the ingestion classifier. A deployment may run both
Windows Defender and ClamAV and combine their normalized verdicts: `infected` or `encrypted` holds the object for
quarantine, `error` with `retryable=true` retries, and only a positive `clean` result can satisfy a clean-scan gate.
Provider URLs and credentials belong to service configuration; they are never accepted from an upload request. Store
only provider name, normalized status/reason code, file checksum and timestamps in operational state/audit.

Worker scanner wiring is opt-in and disabled by default. Set `FILE_SCANNER_MODE=optional` or `required` together
with `FILE_SCANNER_DEFENDER_URL` and/or `FILE_SCANNER_CLAMAV_URL` to enable the bounded pre-sink gate. Both modes
currently fail closed; the distinction is reserved for deployment policy. Optional settings are
`FILE_SCANNER_TIMEOUT` (duration, default `30s`), `FILE_SCANNER_STAGE_DIR` (private local staging directory),
`FILE_SCANNER_MAX_STAGE_BYTES` (default `1073741824`), `FILE_SCANNER_CIRCUIT_FAILURES` (default `3`), and
`FILE_SCANNER_CIRCUIT_COOLDOWN` (default `30s`). URLs are never logged: the worker logs only provider count, mode,
timeout, and staging limit. With `off` (the default), no scanner HTTP calls or staging occur.

Password-protected archives have a separate, deliberately narrow path. Candidate lookup is disabled unless the upload
contains both `archive_passwords=enabled|true|1` and a non-empty `archive_password_campaign=<campaign-id>` tag. The
campaign resolves a tenant-scoped list previously curated by an administrator. This is not dictionary generation or
unbounded brute force: candidates are capped and time-bounded, and plaintext candidates never enter tags, logs,
metrics, audit events or API responses. Missing/invalid tags cause zero attempts. A successful candidate does not by
itself bypass malware scanning or archive safety limits.

## Remaining work

- object lifecycle rules and request rate limits for the configured Defender/ClamAV worker gate;
- Parquet streaming and resumable CSV/Parquet checkpoints;
- per-source mapping/config for the sink (event type, time and entity fields), Parquet output;
- durable PostgreSQL lineage (Parquet normalization and direct ClickHouse ingest are implemented and covered by the env-gated E2E).

## Upload quotas (file-api)

Per tenant (tenant from the Principal only): max file size (413 `FILE_TOO_LARGE`), max concurrently open uploads (429 `TOO_MANY_UPLOADS`),
max declared bytes in a rolling 24 h (429 `UPLOAD_QUOTA`). Usage is derived from `file_objects` (open = uncommitted, not deleted, < 1 h old), so no
new migration. Defaults: `FILE_MAX_BYTES`, `FILE_QUOTA_OPEN` (20), `FILE_QUOTA_BYTES_PER_DAY` (100 GiB); 0-valued overrides mean unlimited. Per-tenant
overrides in `tenant_quotas`: `upload_file_max_bytes`/`upload_open_max` (period `total`), `upload_bytes` (period `day`). Quota store errors fail
closed (503). The check precedes registration, so concurrent requests can overshoot by at most the in-flight count. Metric:
`astryon_file_api_quota_denied_total{reason}`. Live test: `FILEQUOTA_PG_URL`.

## Tags, source types and content classification

Migration `0012_file_tags_classification` (forward-only, rollback note inside): `file_objects.source_type/tags/sensitivity`, `jobs.content_labels`, job status `QUARANTINED`.
Same tenant RLS as the sibling tables; the tenant comes only from the Principal.

**Upload creation** accepts `source_type`, `tags` (max 16, key `^[a-z][a-z0-9_.-]{0,31}$`, value <= 64 chars of `[A-Za-z0-9_.:/@+ -]`) and `sensitivity`
(public|internal|restricted). `GET /v1/files/uploads/{id}` returns them with the latest job (`status`, `error` code, `labels`). 

**Preview**: `GET /v1/files/uploads/{id}/preview` requires `astryon:data:read` (or the existing
`astryon:uploads:write` scope), is tenant-scoped, and serves only committed `text/csv` or
`application/x-ndjson` objects. The response is `Content-Disposition: inline`, capped at 64 KiB and
200 lines (`X-Preview-Truncated: true` when capped). Uncommitted, `QUARANTINED`, credentials-labelled,
or PII-labelled objects return metadata only; matched values are never returned. ZIP/TAR/GZIP and all
other archive formats are rejected by the upload allowlist and preview endpoint. Archive extraction is
intentionally not implemented until a design provides nesting, path-traversal, decompression-ratio and
resource limits.

The opt-in `internal/archive` package provides that first inspection layer for a future quarantine/admin
workflow; it is not wired into normal ingestion. `Inspect`/`InspectContext` recognize ZIP, TAR and GZIP,
inspect metadata only, reject absolute/UNC/`..` paths, ZIP encryption, symlinks and TAR hardlinks, detect
nested archives, and enforce bounded compressed bytes (64 MiB), expanded bytes (1 GiB), file count (10,000),
compression ratio (100:1), and nesting depth (2). Results include deterministic reason codes such as
`unsafe_path`, `encrypted`, `symlink`, `compression_ratio`, `expanded_bytes`, `file_count`, and
`nesting_depth`. ZIP input is buffered only up to the compressed-byte limit because Go's ZIP reader requires
random access; all limits remain fail-closed and callers should keep archive reports metadata-only.
Password candidates are an explicit opt-in only: callers must supply both tags
`archive_passwords=enabled|true|1` and a non-empty `archive_password_campaign=<campaign-id>` before
consulting an external `PasswordProvider`. Candidate count/time remain bounded; no generated dictionaries,
password logging, or plaintext password persistence is allowed. Missing or invalid tags mean zero attempts.
Set `FILE_ARCHIVE_MODE=inspect` to enable the file API integration point: archive MIME/extension pairs are
accepted, checksum-verified, inspected with those limits at upload completion, and returned as
`mode=archive_inspect` without creating a worker job. The default empty mode rejects archive uploads exactly
as before. Inspection failures return `ARCHIVE_REJECTED` and are never sent to a sink.

**List** `GET /v1/files/uploads` (`internal/listing`): tenant from the Principal plus RLS, keyset cursor on `(created_at, id)` DESC (`limit` default 50, max 200,
`next_cursor` opaque), filters `source_type`, `sensitivity`, `status` (latest job, incl. `QUARANTINED`), `label` (`contains_geo|contains_credentials|contains_pii`),
`tag=key` or `tag=key:value` (repeatable, max 5, AND), `created_from`/`created_to`. Tag equality uses `tags @>` (served by the existing `tags` GIN, so no new
migration); results carry label names/counts only. Bad parameters are 400 `BODY_INVALID`. Live test: `FILELIST_LIVE=1 go test ./internal/listing` (docker, postgres:16).

**Profiles** (`internal/profile`): a static table in code, no user rules. Source types: telemetry, geo, report, export, config, log, credentials_bundle,
personal_data, financial, media_meta, reference, unknown (default for unknown/omitted: all detectors, strictest detection). Enforced now: content type + extension,
detectors, separate credentials/geo/PII policies (allow|tag|quarantine|block), sensitivity default and floor (a lower request is raised), required tags, limits
(file bytes, records, columns, line bytes), required fields + `drop_row`/`fail_file`, sink gating (`NoSink`), `geo` requires valid coordinates.
Declared metadata, exposed on GET but **pending enforcement**: archive nesting limit, encoding, retention class, required read scope, export flag, mask/tokenize PII,
dedup-key choice, audit level and alert on findings, macro/executable-attachment detection.

**Classifier** (`internal/classify`): streaming, per record (NDJSON in the worker via `ScanJSON`; `ScanCSV(header,row)` for `parse.CSV`), no file buffering. Detectors: geo
(lat/lon columns in range, WKT, GeoJSON), credentials (JWT, PEM private key, AWS key ids, `password=`/`secret=`/`api_key` text, secret-named fields, high-entropy token fields),
PII-lite (email, phone, IBAN mod-97, card Luhn, US SSN shape). Output is `{contains_geo|contains_credentials|contains_pii: {count, fields[]}}`: field names (capped, charset-limited,
replaced by `(redacted)` if the name itself looks like a secret), never values; nothing is logged. Tests prove values do not reach labels, metrics or logs.

**Policy**: credentials policy precedence is explicit tenant/source override (`FILE_CREDENTIALS_POLICY_OVERRIDES=<tenant>/<source>=block,<tenant>=allow`) > profile > env default
`FILE_CREDENTIALS_POLICY` (quarantine|block|allow, default quarantine). When any policy can hold the file back, the worker classifies the whole object in a bounded streaming pre-scan
before the first sink call, so quarantine/block mean nothing was written. quarantine: job `QUARANTINED`, sensitivity raised to restricted; block: job `FAILED` with the catalog code
(`CREDENTIALS_DETECTED`, `PII_DETECTED`, ...); allow/tag: labels only (merged atomically with each checkpoint). Metrics:
`astryon_file_worker_content_labels_total{label}` and `astryon_file_worker_policy_actions_total{action}`.
Live test: `FILETAGS_LIVE=1 go test ./internal/objects -run Live` (throwaway postgres:16 via docker, applies all migrations, non-superuser, removes the container).
