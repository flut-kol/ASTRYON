# export-worker (DPP-049, execution core)

Module `github.com/astryon/export-worker` (stdlib + `libs/go-common/auth` for AAS in `cmd/` only) that runs export jobs:
verified principal + `ExportSpec` -> read-only ClickHouse stream -> `ObjectStore` -> short-lived pre-signed URL.
It has an HTTP layer and either an in-memory or a durable PostgreSQL job store/quota (`EXPORT_PG_URL`); Kafka is NOT here.

## HTTP API (`internal/export/http.go`, `jobs.go`, `auth.go`; spec `docs/api/export-worker.openapi.yaml`)
- `POST /v1/exports` (scope `astryon:export:write`): body = `ExportSpec` (unknown fields incl. `tenant_id` -> 400);
  validates, reserves quota (429 `quota_exceeded`/`too_many_concurrent` + `Retry-After`), creates a QUEUED job and returns 202
  `{id,status}`. With `EXPORT_PG_URL` POST only enqueues (any replica's dispatcher runs it, see below); without it the job runs in a
  bounded in-process pool (`EXPORT_WORKERS`, queue cap -> 503 `queue_full`).
- `GET /v1/exports/{id}` (scope `astryon:export:read`): status, rows, bytes, `error_code`; for SUCCEEDED a pre-signed URL
  signed at request time (never stored). Other tenants' ids -> 404. Errors: `{error:{code,message,details,trace_id}}`, `X-Request-ID`.
- Statuses QUEUED -> RUNNING -> SUCCEEDED|FAILED. Graceful shutdown (SIGTERM) waits for jobs until the deadline, then cancels
  and marks every unfinished job FAILED (`shutdown`). Quota is reserved at POST and held until the job ends.
- Auth: AAS via `libs/go-common/auth` when `AAS_BASE_URL` is set (`cmd/export-worker/aas.go`), dev static token
  (`EXPORT_DEV_TOKEN` + `EXPORT_DEV_TENANT`), else deny-all.
- Env: `LISTEN_ADDR` (:8083) `EXPORT_WORKERS` (2) `CH_URL CH_DATABASE CH_USER CH_PASSWORD` `S3_ENDPOINT S3_BUCKET S3_REGION S3_ACCESS_KEY S3_SECRET_KEY`
  `EXPORT_MAX_PER_DAY` `EXPORT_MAX_CONCURRENT_PER_TENANT` `EXPORT_URL_TTL_SECONDS` `AAS_*`
  `EXPORT_PG_URL` (enables durable jobs + quota + the dispatcher; unset = in-memory with a warning log) `EXPORT_WORKER_ID` (default hostname-pid, unique per replica) `EXPORT_LEASE_SECONDS` (60)
  `EXPORT_SWEEP_SECONDS` (300, 0 = off) / `EXPORT_SWEEP_GRACE_SECONDS` (60): orphan multipart-upload sweeper (dispatcher mode, see 'Failure modes').
- Docker: `docker build -f services/export-worker/Dockerfile -t astryon/export-worker .` (repo root context).
- Verified: httptest suite with fakes under `-race` (401/403, tenant in body 400, cross-tenant 404, quota 429, lifecycle,
  URL only when SUCCEEDED, queue bound, shutdown); image built and a request without credentials returned 401.
  The HTTP flow against live ClickHouse + PostgreSQL + MinIO is verified by the E2E harness below. NOT verified: AAS signed-token flow,
  no job listing/cancel/expiry. With in-memory jobs/quota: lost on restart and per process (not replica-safe).

## Durable jobs and quota (`internal/export/pg.go`, migration `db/postgres/migrations/0009_export_jobs.up.sql`)
- Migration number 0009 follows 0008_tenant_scoped_fks (copied from branch flut_kol/ASTRYON); renumber on merge if that branch adds its own 0009+. Forward-only, rollback note in its header.
  Table `export_jobs` (tenant-scoped FK-ready `UNIQUE (tenant_id, id)`, RLS via `astryon_enable_rls`, lease columns `locked_by`, `lease_expires_at`).
- `PGJobStore`: Create/Get/Update/FailUnfinished. Every operation runs in a transaction that first does
  `set_config('astryon.tenant_id', <principal tenant>, true)`; another tenant's id is "not found". Update uses `FOR UPDATE`.
  A job owned by a replica (`locked_by`) keeps a lease refreshed by a heartbeat (every lease/3). `Recover` (at startup and
  periodically, any replica) fails non-terminal jobs with an expired lease as `worker_lost`. `FailUnfinished` (shutdown) only touches
  this worker's jobs. `Enqueue` + `Claim` (`FOR UPDATE SKIP LOCKED`, unowned or lease-expired QUEUED jobs) let several replicas share
  a queue; Claim sets the tenant for its write from the claimed row. The cross-tenant scans (claim pick, heartbeat, recovery,
  shutdown) use the platform-admin RLS switch, a constant in the code, never derived from a request.
- `PGQuota`: `CreateReserved` takes `pg_advisory_xact_lock(tenant)`, counts today's (UTC, DB clock) jobs and QUEUED/RUNNING jobs from
  `export_jobs` and inserts the job in the same transaction, so replicas cannot over-grant. The `Server` uses it automatically
  (`ReservingCreator`); `Acquire` is only an advisory check. A concurrency slot is freed when the job turns terminal.
- Runtime role: non-superuser without BYPASSRLS with DML on `export_jobs`. Pool via pgx v5.5.5.
- Verified (fakes, always): tenant set first and validated, rollback, claim statement order, quota decisions and lock order, Server 429 path.
- Verified LIVE (PostgreSQL 16, migrations 0001-0009, role `NOSUPERUSER NOBYPASSRLS`, `EXPORT_PG_URL` set, otherwise skipped):
  50 jobs / 2 store instances x 3 loops never claimed twice and none were lost; cross-tenant get/update is not found; 20 concurrent
  creates from two instances grant exactly the limit (daily and concurrent); RLS hides rows and rejects inserts for a session without
  tenant and for another tenant; lease recovery fails only the dead worker's job, heartbeat keeps the live one.
- NOT verified: the full binary against a live PG together with ClickHouse/S3, a PG failover, more than two dispatcher instances
  in one process. Limits: a job whose heartbeat cannot reach PG for longer than the lease is failed by another replica (the late
  finish is ignored, the object may remain until the S3 lifecycle rule removes it); no listing/cancel/expiry sweep yet.

## Dispatcher (`internal/export/dispatcher.go`; active when `EXPORT_PG_URL` is set)
Flow: `POST` -> `PGQuota.CreateReserved` inserts the job QUEUED and UNOWNED (quota reserved in the same transaction) -> 202 and
`Dispatcher.Wake()` (local wake-up). Every replica runs `EXPORT_WORKERS` runner goroutines: `Claim` (`FOR UPDATE SKIP LOCKED`, oldest
first; the job stays QUEUED, owned by the replica with a lease, `started_at` NULL) -> `Start` (QUEUED -> RUNNING, owner-guarded) ->
`Runner.Run` (tenant from the claimed row, which came from the verified principal at POST) -> `Finish` (SUCCEEDED/FAILED with rows,
bytes, object key, `error_code`; owner-guarded, so a replica that lost its lease cannot overwrite the recovered state).
Idle runners poll with jittered exponential backoff (200 ms .. 5 s, +-50%) and poll at once on a local Wake. One heartbeat loop per
replica extends the lease of all its jobs every lease/3; the same loop runs `RecoverJobs` every lease/3 (and once at startup).
- Re-queue rule (expired lease, any replica may run recovery): a job that had NOT started (QUEUED, `started_at` NULL, claimed by a
  dead replica) returns to QUEUED with `attempts + 1` and is claimed again, up to 3 attempts in total (`DefaultMaxAttempts`); when the
  third claim is lost it is FAILED `max_attempts`. A job that HAD started (RUNNING) may already have written output, so it is
  FAILED `worker_lost` and never re-run. A graceful `Release` (shutdown or failed `Start`) does not consume an attempt.
- Graceful shutdown (SIGTERM): stop claiming (a claim that races in is released), wait for running jobs until the 25 s deadline, cancel
  the rest, mark this replica's unfinished jobs FAILED `shutdown`, then stop the heartbeat. Cancelling the dispatcher context directly
  is a hard stop (simulated crash): nothing is written and leases expire.
- Schema: `export_jobs.attempts int NOT NULL DEFAULT 0` was added to migration 0009 IN PLACE (the file is untracked and unreleased, so
  no 0010 was created). A database that already applied the older 0009 needs `ALTER TABLE export_jobs ADD COLUMN attempts int NOT NULL
  DEFAULT 0 CHECK (attempts >= 0)` by hand. Claim no longer picks lease-expired QUEUED rows; recovery owns that decision.
- Verified (fakes, `-race`): FIFO claim with two dispatchers, each job runs once, failure codes, heartbeat extends the lease while
  running, graceful shutdown waits / stops claiming / times out to FAILED `shutdown`, hard stop writes nothing, lost Start is not run,
  Wake skips the idle backoff, POST in dispatcher mode does not run locally, recovery statements (requeue before fail, max attempts).
- Verified LIVE (PostgreSQL 16.x, role NOSUPERUSER NOBYPASSRLS, `EXPORT_PG_URL`, fake Runner data path = fake ClickHouse + in-memory
  object store): two dispatcher instances x 3 runners over 30 jobs of two tenants ran every job exactly once (30 distinct objects,
  both instances took work); a dispatcher killed by context cancel (no graceful path) left a started job that another instance
  failed as `worker_lost` and a claimed-not-started job that was re-queued (`attempts` 1) and completed once by the survivor;
  max attempts (2 re-queues, third lost claim -> `max_attempts`); owner-guarded Start/Finish; Release keeps attempts; graceful
  shutdown timeout -> FAILED `shutdown`.
- NOT verified: more than two instances, long-running load (dispatcher through the binary and PG interruption: see 'Failure modes'). An idempotency gap remains by design: a job whose output was written but whose `Finish` never
  landed is FAILED `worker_lost` (the object is left to the S3 lifecycle rule).

## What is in `internal/export`
- `spec.go`: `ExportSpec` (format csv|ndjson, from/to, processing_version required, source_id, event_type, entity_id,
  max_rows). No tenant field: tenant comes only from `Principal` (validated token claims). Same rules as
  `services/query-api` (bound ClickHouse params, `readonly=1`, `LIMIT 1 BY` dedup, range <= 31 days); copied, not imported.
  Object key: `exports/<tenant_id>/<export_id>.<ext>`; `KeyBelongsToTenant` guards downloads.
- `guard.go`: wraps the ClickHouse body; O(1) memory; enforces `max_rows` (query asks for max_rows+1 to detect
  overflow) and a byte cap; detects a mid-stream ClickHouse exception (`Code: N. DB::Exception` appended to a 200
  response) and truncated bodies. CSV row counting is quote-aware.
- `sigv4.go`, `s3.go`: hand-written SigV4, path-style `S3Store` (single PutObject below `PartSize`, otherwise multipart
  with one part buffered, abort on error), `PresignGet` (default TTL 15 min, max 1 h, min 1 s, else `ErrBadTTL`).
- `quota.go`: `QuotaChecker` + `MemoryQuota` (N exports per UTC day, max concurrent, per tenant, per process).
- `audit.go`: `AuditSink`, `MemoryAudit`, `JSONLinesAudit`; one record per attempt:
  tenant, subject, export_id, spec_hash, rows, bytes, outcome (+error).
- `worker.go`: `Runner.Run` (validate -> quota -> stream -> on any failure delete the partial object -> audit -> presign).

## Verification
Verified against fakes (`go test ./...`, always run):
- SigV4 against the AWS documented vectors (GET with Range, pre-signed GET, PUT object); a fake S3 re-verifies the
  signature of every request, covers single/multipart/short-last-part, abort on reader error and part failure.
- Tenant prefix, row cap, byte cap (partial object deleted, outcome audited), ClickHouse error mid-stream (ndjson, csv),
  truncated body, ClickHouse HTTP error, store error, TTL bounds, quota (daily + concurrent), audit for success/failure.

Verified live (`EXPORT_LIVE=1`, otherwise skipped; dev compose ClickHouse 24.x + adobe/s3mock, isolated project):
- 40k seeded rows (+ another tenant's rows) exported as ndjson and csv (~23 MB / ~18 MB, real multipart),
  exact row counts, no cross-tenant rows, download via the pre-signed URL without auth header,
  row-cap overflow aborts and leaves no object, unknown tenant yields an empty export.
Env: `EXPORT_CH_URL EXPORT_CH_DB EXPORT_CH_USER EXPORT_CH_PASSWORD EXPORT_S3_ENDPOINT EXPORT_S3_ACCESS_KEY EXPORT_S3_SECRET_KEY`.

SigV4 acceptance and URL expiry against a real MinIO are now covered by the E2E harness below (the dev `s3mock` does not
validate either). A genuine ClickHouse exception after streaming began is now provoked live (see 'Failure modes'). Audit is in-memory/JSON lines.

## Live end-to-end (`cmd/export-worker/e2e_test.go`, `EXPORT_E2E=1`)
Runbook: `cd services/export-worker && EXPORT_E2E=1 go test ./cmd/export-worker -run TestE2ELive -v -count=1 -timeout 15m`
(needs docker; images `clickhouse/clickhouse-server:24.8`, `postgres:16-alpine`, MinIO `bitnamilegacy/minio:latest` (override with `MINIO_IMAGE`;
`minio/minio` is no longer pullable from Docker Hub). Containers `chagg-expe2e-{ch,pg,minio}` in network `chagg-expe2e-net` on random
127.0.0.1 ports, passwords generated per run, nothing committed. The test applies `db/clickhouse` migrations (token substitution as
`db/apply.sh`) and `db/postgres` 0001-0010, creates a NOSUPERUSER NOBYPASSRLS role, builds the real binary, runs it with the dev
static token and `EXPORT_PG_URL`, and removes containers, network and temp dir at the end (`EXPORT_E2E_KEEP=1` keeps them). If a run is
killed, clean up with `docker rm -f chagg-expe2e-ch chagg-expe2e-pg chagg-expe2e-minio; docker network rm chagg-expe2e-net`.
Measured on a laptop (Docker Desktop, arm64), 100k seeded rows (tenant A 60k rows = 50k unique + 10k duplicates; tenant B 40k):
- tenant A export, `LIMIT 1 BY` works: ndjson 50 000 rows / 28.8 MB in ~1.0 s (POST to SUCCEEDED), csv 50 000 rows / 22.3 MB in ~0.8 s;
  downloaded without credentials via the pre-signed URL; byte count equals the job's `bytes`; 50 000 distinct event ids, no row of tenant B.
- `max_rows` overflow (1000 and a multipart-sized 20 000): FAILED `row_cap_exceeded`, no object, no leftover multipart upload in MinIO's data dir.
- tenant B GET of A's job -> 404; wrong token -> 401.
- MinIO rejects a tampered signature (403 `SignatureDoesNotMatch`), the signature reused for another object key (403), an unsigned GET (403),
  and an expired URL (TTL 4 s, waited 5 s: 403 `Request has expired`); a new GET re-signs and downloads again. Our SigV4 signer is accepted by real
  MinIO for CreateBucket, single/multipart Put, Get, Delete.
- quota: `EXPORT_MAX_PER_DAY=3`, 4th POST -> 429 `quota_exceeded` (Retry-After = seconds to UTC midnight, ~18 000 s).
- two workers (2 runners each, lease 3 s), burst of 40 queued jobs x 50 000 rows (1.1 GB ndjson): all SUCCEEDED in 32.7 s total, each job
  has exactly one audit record, split 22 / 18 between the workers. (This is far from linear scaling: ~0.8 s per job; the four runners
  share one laptop, MinIO and ClickHouse; not tuned.)
- `kill -9` of the worker while MinIO was paused (job RUNNING): a second worker marked it FAILED `worker_lost` 3.1-3.2 s after the kill
  (lease 3 s), not re-run, no object; the survivor then served a new job normally.
- Result of this first harness run: no defect found. The failure-mode run below did find and fix defects.
Did NOT / not covered here: real AAS tokens; the 100k-row scale is small; the burst test depends on the idle backoff (<= 5 s). The failure
modes are in the next section.

## Failure modes (`cmd/export-worker/e2e_failures_test.go`, `EXPORT_E2E=1`, test `TestE2EFailureModes`)
Runbook: `cd services/export-worker && EXPORT_E2E=1 go test ./cmd/export-worker -run TestE2EFailureModes -v -count=1 -timeout 25m` (~2.5 min after images are
pulled; seeds 1 000 000 rows for one tenant, ~575 MB as ndjson; leases 3 s). Measured on the same laptop (Docker Desktop, arm64), real binary, CH 24.8, PG 16, MinIO.
- ClickHouse `KILL QUERY` mid-stream (multipart upload already running; concurrency cap 1): **BUG FOUND AND FIXED.** CH 24.8 ends a killed `JSONEachRow`
  stream with a line `{"exception": "Code: 394. DB::Exception: Query was cancelled..."}`. That line is valid JSON, the guard counted it as a row and the
  truncated export (65 410 of 1 000 000 rows) was marked SUCCEEDED. The guard now recognises `{"exception":` (and the CSV `Code: ` line) and fails the
  job. Now: FAILED `clickhouse_error` 0.75-1.1 s after the kill, audit error keeps the CH text, no object, 0 incomplete multipart uploads, the concurrency
  slot is free (next job SUCCEEDED 160 ms later). The error code was `export_failed` before; ClickHouse failures (connect error, non-2xx, truncated or errored
  stream) are now `clickhouse_error` (a cancelled context stays `shutdown`/`timeout`).
- `docker kill` of ClickHouse mid-stream: FAILED `clickhouse_error` 0.6-1.5 s later, no object, 0 uploads; a job posted while CH is down fails at once
  (`clickhouse_error`, nothing written); after `docker start` CH answers `/ping` 0.7-3.9 s later and the next job SUCCEEDED (~170 ms job) 0.9-4.2 s after the start.
  NOT covered: a CH that hangs (no data, no reset): the read has no idle timeout, only `max_execution_time` (600 s) and the 1 h job timeout bound it.
- PostgreSQL `docker restart` (1.5-6.7 s) with 8 jobs x 259 200 rows (2 workers x 2 runners; running + queued): the workers did not crash, the log stays small
  (heartbeat failure logged once per outage, "queue reachable again after ..."), all 8 jobs SUCCEEDED (the running ones kept streaming and the terminal
  write was retried until PG came back), each audited exactly once, new work served 170-210 ms after PG was back. All terminal 32-53 s after the disruption (job duration dominates).
- PostgreSQL stopped 8-9 s (> lease 3 s) with 8 jobs in flight: POST -> HTTP 500 while down. Defined outcome per job, run 1 / run 2: SUCCEEDED 7 / 6, FAILED
  `worker_lost` 1 / 2, never both, no job lost, none run twice (<= 1 audit record each). Which one a job gets is a race between the owner's heartbeat and
  the other replica's recovery (both run every lease/3 once PG is back): a job whose lease expired while PG was down and that the OTHER replica recovers first is
  FAILED `worker_lost` (its late `Finish` is ignored). The owner now heartbeats immediately before each recovery pass, so a single replica always resumes its
  own jobs; with two replicas the other one may win. In run 1 the lost job had a finished object (an orphan object, see S3 lifecycle below); in run 2 none was
  visible at check time (the zombie run is not cancelled when the lease is lost, so it can still finish and leave an unreferenced object later). Recovery time: new job served 170 ms after PG up.
  Fixes: terminal write retried with jittered backoff for up to `FinishRetry` (2 min, then the job is abandoned: heartbeats for it stop so recovery fails it
  `worker_lost`; before: ONE attempt, then the job stayed RUNNING and was heartbeated forever); every claim/heartbeat/recover has a 15 s timeout (before: a hung PG blocked
  the runner and the shutdown); heartbeat failures are logged once per outage. NOT verified: goroutine count (no pprof endpoint; the unit tests cover the bounded retry), PG failover/replicas, outages longer than `FinishRetry`.
- SIGTERM mid-export: (a) ~5 s export: the listener closes at once, the worker waits, the job ends SUCCEEDED, exit 2.5-3.3 s after SIGTERM, 0 uploads left.
  (b) S3 stalled (MinIO `docker pause`) so the job cannot finish: the worker waits the 25 s deadline, cancels, aborts the upload (the abort gets through when S3 returns
  within its 30 s timeout), marks the job FAILED `shutdown`, exits 26.1 s after SIGTERM; no object, 0 uploads. NOT covered: S3 down for the whole abort window (then the
  upload is an orphan; the sweeper or the lifecycle rule below removes it), SIGTERM with CH slow instead of S3.
- `kill -9` mid-multipart: **an orphan incomplete multipart upload remains** (1 upload, the abort never ran). Cleanup implemented: `UploadSweeper` (every replica,
  every `EXPORT_SWEEP_SECONDS`, default 300) lists incomplete uploads and aborts those whose job is terminal (e.g. FAILED `worker_lost`) or unknown once older than
  `EXPORT_SWEEP_GRACE_SECONDS` (60), and any older than 70 min whatever the job says. Measured with a 2 s sweep and 2 s grace: job FAILED `worker_lost` 3.8 s and the
  orphan aborted 4.8 s after the kill (production defaults: ~lease 60 s + <= 300 s sweep). MinIO quirk found: its ListMultipartUploads only matches an EXACT key as
  `prefix`, so the sweeper lists everything and filters client-side (assumes a dedicated export bucket). Defence in depth, apply an S3 lifecycle rule:
```json
{"Rules":[{"ID":"abort-incomplete-exports-mpu","Status":"Enabled","Filter":{"Prefix":"exports/"},"AbortIncompleteMultipartUpload":{"DaysAfterInitiation":1},
"Expiration":{"Days":7}}]}
```
  `aws s3api put-bucket-lifecycle-configuration --bucket <bucket> --lifecycle-configuration file://lifecycle.json` (the 7-day expiry also removes
  objects left by FAILED `worker_lost` jobs; keep it >= the longest download window). The lifecycle rule was NOT exercised against MinIO here.
- Pre-signed URL scope (real MinIO, no AAS): the object's signature gives 403 for another key under the same prefix, another tenant's prefix, a sibling key,
  another bucket name, the tenant directory, `exports/`, the bucket root with and without `?list-type=2&prefix=...` (no listing), 400 for `../` traversal;
  PUT, DELETE, HEAD -> 403 and POST -> 400 with the GET URL; the object stays readable through its own URL.
- Regression tests without Docker: `guard`/`errCode` for the exception line (also split across chunks), finish retry and abandon, heartbeat-before-recovery, sweeper decisions.

## Remaining proposal (not implemented)
```sql
export_audit(id bigserial, tenant_id uuid, subject text, export_id uuid, spec_hash text, rows bigint,
  bytes bigint, outcome text, error text, at timestamptz)  -- append-only
```
Apply an S3 lifecycle rule on `exports/` so objects expire after the download window. `export_jobs` and quota are done (above).
