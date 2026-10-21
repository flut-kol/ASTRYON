# Runbook: retry-controller

## Symptoms

- `astryon_retry_errors_total` grows;
- `astryon_retry_released_total` stops increasing while due rows exist;
- jobs remain in `RETRYING` beyond their `run_after` time;
- controller `/healthz` is unavailable.

## Safe diagnosis

Use a read-only, platform-approved PostgreSQL connection. Scope every query by
tenant when investigating a customer issue; do not copy `payload` or error text
into tickets.

```sql
SELECT retry_at, count(*)
FROM scheduled_retries
WHERE fired_at IS NULL AND cancelled_at IS NULL
GROUP BY retry_at
ORDER BY retry_at
LIMIT 20;
```

```sql
SELECT tenant_id, count(*)
FROM jobs
WHERE status = 'RETRYING' AND run_after < now() - interval '5 minutes'
GROUP BY tenant_id;
```

Check the controller's `/metrics` and `/healthz`, PostgreSQL availability and
the worker that consumes jobs after they return to `QUEUED`.

## Recovery

1. Restore PostgreSQL connectivity or credentials; never bypass TLS or replace
   the database with a local empty instance.
2. Restart the controller normally. `FOR UPDATE SKIP LOCKED` prevents duplicate
   firing between healthy replicas.
3. Confirm that due rows receive `fired_at` and associated jobs move to
   `QUEUED`.
4. If a job has exhausted `max_attempts`, investigate the underlying failure;
   do not reset attempts in bulk. A tenant-scoped, audited replay request is
   required for a deliberate retry after repair.

Do not delete `scheduled_retries` rows or directly change job status during an
incident. Those actions destroy audit evidence and can create unbounded repeat
work. Escalate suspected stuck leases, database corruption or cross-tenant data
to the platform on-call owner.

## Dispatch mode (`ASTRYON_RETRY_DISPATCH_TOPIC`)

Due retries are claimed with a fenced lease (`jobs.locked_by`/`locked_until`), published to Kafka, then marked fired
and the job set to `QUEUED`. Symptoms and handling:

- Job stays `RETRYING` past `run_after` with `locked_by` set: a dispatcher crashed between claim and publish. The
  lease (60s) expires and any replica re-claims it; no manual action. A late zombie `Complete` is rejected (fenced).
- Duplicate wake-ups on the topic are expected (crash after publish, before complete); consumers dedupe on
  (tenant_id, job_id, attempt).
- Kafka unavailable: publish errors appear as `retry dispatch:` log lines, claims are released and retried next poll.
- `FAILED` jobs with `last_error` starting `dead-letter:` exhausted attempts or hit a permanent error. Do not reset
  attempts in bulk; use the tenant-scoped, audited replay request after repair.
