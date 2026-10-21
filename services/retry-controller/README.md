# retry-controller

Releases due tenant-scoped job retries from PostgreSQL. It uses `FOR UPDATE
SKIP LOCKED`, so replicas can run concurrently.

Required environment variable: `DATABASE_URL` (PostgreSQL URL).

Optional variables:

- `ASTRYON_RETRY_POLL_INTERVAL` — positive Go duration; default `2s`.
- `ASTRYON_RETRY_BATCH` — positive maximum due rows per poll; default `100`.
- `ASTRYON_RETRY_METRICS_ADDR` — metrics/health bind address; default `:8081`.

Endpoints: `GET /healthz`, `GET /metrics`. The controller never accepts a
tenant ID from HTTP input and exposes only aggregate, tenant-free metrics.
Backlog gauges (read from `scheduled_retries` at scrape time): `astryon_retry_backlog_pending`
(unfired, uncancelled retries), `astryon_retry_oldest_due_age_seconds` (0 when none due),
plus `astryon_retry_backlog_read_errors_total`.

Run after control-plane migrations are applied:

```bash
DATABASE_URL='<platform-provided-postgres-url>' go run ./cmd/retry-controller
```

## Worker integration (`pkg/retryclient`)

- `retryclient.New(db, Config{TenantID, WorkerID, Policy})`: TenantID comes only from validated claims/trusted config.
  `Client.Fail(ctx, jobID, err)` classifies (`Transientf(err)` = retryable; anything else is permanent), then in one
  transaction either schedules a bounded jittered retry (`RUNNING -> RETRYING`, attempt_count+1) or dead-letters the
  job (`FAILED`, `last_error` prefixed `dead-letter:`; optional `DeadLetterSink`) when permanent or attempts are
  exhausted. Idempotent per job id (`ActionAlready` / `ActionDeadAlready`); fenced by `jobs.locked_by == WorkerID`
  (`ErrLeaseLost` otherwise). Use a non-superuser DB role so tenant RLS applies.
- Dispatch: `Dispatcher` claims due retries under a per-claim fenced lease (`locked_by` token + `locked_until`),
  publishes a `Message{tenant_id, job_id, kind, attempt}` via `Publisher` (`KafkaPublisher` on franz-go, or
  `PublisherFunc` callback), then `Complete` (job -> `QUEUED`, retry `fired_at`). Delivery is at-least-once; consumers
  dedupe on (tenant_id, job_id, attempt) and still claim the job themselves.
- Set `ASTRYON_RETRY_DISPATCH_TOPIC` (+ `ASTRYON_KAFKA_*`) to run the controller in dispatch mode instead of the direct
  `QUEUED` flip. Never run both modes against one database.
- Hooks for file-ingestion/connector-manager workers (not yet wired there): call `Client.Fail` on job failure and consume the topic.
- Tests: `go test -race ./...`; live PG: `RETRYCLIENT_LIVE=1 go test ./pkg/retryclient` (needs docker).

See `docs/operations/runbooks/retry-controller.md` for diagnosis and recovery.
