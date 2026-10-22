# replay-controller (DPP-043)

`POST /v1/replays` planning core. A replay never overwrites data: it is assigned a NEW `processing_version`
(monotonic per tenant+source), so readers selecting the active version never see doubled aggregates
(DEVELOPMENT_PLAN §5.3; model in `db/clickhouse/migrations/0001` / `0003`).

## Behaviour
- Request `{source_id, from, to, pipeline_version, dry_run}`; unknown fields (including any `tenant_id`) are rejected with 400.
- Tenant comes ONLY from the verified principal (`Authenticator`, same shape as query-api). Scope `astryon:replay:write` (403 otherwise, 401 unauthenticated).
- Bounded scope: source required (UUID), `from < to`, range <= 31 days, `pipeline_version >= 1`.
- Dry run (200): reads only. Returns the would-be `processing_version` (informational, NOT reserved), estimated events
  (bounded `count()` over `LIMIT 1 BY (tenant_id, source_id, event_id, processing_version)` at the version being replaced, capped at 10M),
  and raw source ranges split per UTC day. Allocates and publishes nothing.
- Non-dry-run (202): `VersionAllocator.Allocate` then `JobPublisher.Publish(ReplayJob)`. If publishing fails the version stays burned (gaps OK, reuse never).
- ClickHouse access: HTTP interface, POST with `readonly=1`, bound `param_*` parameters, table `events_normalized_distributed`.

## Read API
`GET /v1/replays?limit=1..100&cursor=&source_id=&status=` (newest first, keyset on created_at,id) and `GET /v1/replays/{id}`; scope `astryon:replay:read` (the write scope does not read);
tenant only from the principal and RLS, another tenant's id is 404; needs `REPLAY_PG_URL` (else 501 `not_configured`). Fields: id, source_id, processing_version, status, time_from/to, job_id, created/updated/completed_at (no actor ids). Live-tested on PostgreSQL 16 with a NOSUPERUSER NOBYPASSRLS role (`list_it_test.go`).

## Env (cmd/replay-controller)
`CH_URL` (required), `REPLAY_PG_URL` (PostgreSQL; enables the multi-replica allocator, role must NOT bypass RLS; unset = single-replica in-process allocator), `CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `REPLAY_ADDR` (default `:8087`),
Auth: `AAS_BASE_URL` (+ `AAS_ISSUER`, `AAS_AUDIENCE`, `AAS_ENV`, `AAS_ALLOWED_AZP`) enables the shared AAS verifier (`libs/go-common/auth`, adapter `cmd/replay-controller/aas.go`);
tenant, `sub` (-> `Principal.Subject`, used as audit subject / `switched_by`) and scopes come only from the verified token. Neither AAS nor dev token set = deny-all (401).
DEV-ONLY: `REPLAY_DEV_TOKEN`, `REPLAY_DEV_TENANT_ID`, `REPLAY_DEV_SCOPES` (add `astryon:replay:read` for the read API) (explicit opt-in; overrides AAS and logs a WARNING).
Audit: `ASTRYON_KAFKA_BROKERS` (+ TLS/SASL as the job publisher) -> `audit.events.v1`; `REPLAY_AUDIT_MODE=required|best_effort` (default `required` with Kafka, `best_effort` without; `required` without Kafka refuses to start); `REPLAY_AUDIT_TIMEOUT` (default 5s per event).

Kafka client (`internal/replay/kafka_tls.go`, shared by `KafkaJobPublisher` and `KafkaAuditSink`; same behaviour as the gateway): `ASTRYON_KAFKA_BROKERS`, `ASTRYON_KAFKA_CLIENT_ID`, `ASTRYON_KAFKA_DELIVERY_TIMEOUT`,
`ASTRYON_KAFKA_TLS` (1/true/0/false, case-insensitive; anything else is a startup error, never plaintext), `ASTRYON_KAFKA_TLS_CA_FILE` (PEM; needs TLS, else error; certificate chain and hostname always verified),
`ASTRYON_KAFKA_TLS_CERT_FILE` + `_TLS_KEY_FILE` (mTLS client certificate; both or neither, needs TLS) and `ASTRYON_KAFKA_TLS_RELOAD_INTERVAL` (default 1m, 0 = off: renewed client cert/key and CA files are picked up by new broker connections without restart; a broken new file keeps the last good one and is logged at most once a minute),
`ASTRYON_KAFKA_SASL_MECHANISM` (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512; other value = error; PLAIN without TLS = error) + `_USERNAME`/`_PASSWORD`.
Errors: a failed publish/record caused by a TLS or SASL failure reads `... context deadline exceeded (last broker connect error: sasl: ... SASL_AUTHENTICATION_FAILED ...)` or `(... x509: certificate signed by unknown authority)`;
the password is redacted and the remembered cause expires after 30 s. Without a recorded cause the error is the bare timeout.

## Verified
- Unit tests (fakes): auth 401/403, validation bounds, tenant from principal only (body tenant rejected, other tenant's versions ignored),
  dry-run has no side effects, never reuses an existing version, 200 concurrent allocations are distinct, publish failure burns the version.
- Live ClickHouse 24.8 (`REPLAY_IT_URL`, `REPLAY_IT_DB`, `REPLAY_IT_USER`, `REPLAY_IT_PASSWORD`; skipped when unset): max(processing_version) per tenant+source
  (cross-tenant isolation), de-duplicated range count, LIMIT cap, 20 concurrent allocations distinct and above the stored max, dry-run leaves no reservation, writes rejected by readonly.

## Not verified / TODO
- Cutover exists (below) but nothing in this repo yet moves a replay to COMPLETED except the worker flow; check before relying on it. `KafkaJobPublisher` (replay.jobs.v1, key tenant+0x1f+source, acks=all; enabled by `ASTRYON_KAFKA_BROKERS`, TLS/SASL as the gateway) is verified live against Kafka with `REPLAY_KAFKA_IT_BROKERS`; jobs are consumed by `services/replay-worker`.
- The `CH` allocator alone is race-free within ONE process only; run several replicas only with `REPLAY_PG_URL` set (see below).
- No OpenAPI file yet.

## Multi-replica allocation (`PGAllocator`, pg.go)
One transaction per allocation: `set_config('astryon.tenant_id', <principal tenant>, true)` (RLS, migration 0001) ->
`pg_advisory_xact_lock(hashtextextended(tenant_id::text || source_id::text, 0))` -> `next = GREATEST(max(target_processing_version) over replay_requests
INCLUDING soft-deleted rows, ClickHouse max read just before) + 1` -> `INSERT replay_requests (status REQUESTED, time_from/to)` -> commit.
A failed publish sets the row to `FAILED`; the version stays burned (never reused). Dry run uses `Peek` (no row, no lock).
Verified live (PostgreSQL 16, non-superuser role): 50 concurrent allocations from two separate pools -> 51..55 contiguous/distinct above CH max;
tenant B numbering independent and invisible under RLS (no-tenant session sees 0 rows, cross-tenant INSERT rejected); failed publish -> FAILED, next = v+1.
Run: `REPLAY_PG_URL=postgres://app_role:...@host/db go test ./internal/replay -run PGLive` (migrations 0001-0005 applied; skipped when unset).

Unique index (migration `db/postgres/migrations/0014_replay_version_unique.up.sql`): `CREATE UNIQUE INDEX replay_requests_version_uq ON replay_requests (tenant_id, source_id, target_processing_version)`.
Rollback: `DROP INDEX replay_requests_version_uq;`. Deliberately NOT partial: FAILED/CANCELLED/soft-deleted rows keep their version burned (the allocator takes MAX over all rows), so a version is never reused and the
index can be total. It fails to apply if duplicates already exist. The advisory lock already makes allocation correct; the index is a guard against any writer that bypasses the allocator. If it fires
(SQLSTATE 23505 on `replay_requests_version_uq`), `Reserve` returns `ErrConflict`, nothing is published, and `POST /v1/replays` answers `409 replay_conflict` (retry allocates the next version).
Verified on PostgreSQL 16 (throwaway `postgres:16-alpine`, whole chain 0001-0014 applied, NOBYPASSRLS role): a duplicate insert is rejected and classified as a conflict (`TestPGLiveUniqueVersionIndex`).
Caveats: the advisory-lock hash can collide across different (tenant, source) pairs (only causes extra serialization, not duplicates); the lock is
per-database, so a PgBouncer in transaction mode is fine (xact lock) but statement mode is not.

## Cutover: `POST /v1/replays/{id}/activate[?rollback=true]` (needs `REPLAY_PG_URL`; else 501)
Scope `astryon:replay:write`, tenant only from the principal (another tenant's replay id = 404). Sets `source_active_versions` (migration 0010) for the replay's source,
which `services/query-api` uses when `processing_version` is omitted. One PG transaction: RLS tenant -> replay row `FOR SHARE` -> per-(tenant, source) advisory lock (separate namespace from allocation)
-> current row `FOR UPDATE` -> pure rule `decide` -> upsert (`previous_processing_version` = the replaced one). Rules:
- replay must be `COMPLETED` (else 409 `replay_not_completed`); target lower than the active version -> 409 `version_regression`; equal -> 200 `noop` (nothing written or audited);
- `?rollback=true` undoes THIS replay's activation: target must be the active version and a previous version must exist (else 409 `rollback_unavailable`); previous becomes active and the rolled-back version becomes `previous`, so re-activating is an ordinary higher activation.
Audit: see "Audit trail" below.

Verified: unit tests with fakes (401/403, non-COMPLETED, regression, noop, rollback, cross-tenant 404, audit, concurrency in memory) and live PostgreSQL 16 (`REPLAY_PG_URL`, non-superuser NOBYPASSRLS role,
migrations 0001-0010): same rules, RLS isolation (cross-tenant insert rejected), 36 concurrent activations over two pools -> exactly one row with the highest version.
Not verified: who moves `replay_requests` to COMPLETED.

## End-to-end test
`e2e_test.go`: `POST /v1/replays` -> job on the jobs topic (real `KafkaJobPublisher` encoding, fake producer) -> table flips REQUESTED -> RUNNING -> COMPLETED as the worker does -> `POST .../activate` (409 before COMPLETED, 200 after, version 2 active).
The worker half is in `services/replay-worker` (`TestE2EControllerJobToCompleted`); the two modules cannot import each other (`internal`), so both read `internal/replay/testdata/job.v1.json`, which `TestJobWireFormMatchesGolden` pins to the publisher's output.

## Authentication (AAS)
`aasAuthenticator` wraps `auth.Verifier.Authenticate`; every failure is a 401 without detail. Scope `astryon:replay:write` is checked by the handler (403). A `tenant_id` in a body is never read
(planning rejects unknown fields with 400; activation ignores the body). Verified: end-to-end test against a fake AAS (discovery + JWKS, RS256 tokens signed in the test): no token 401, valid token + scope 200,
missing scope 403, tampered signature 401, tenant only from the claim, `sub` reaches the audit record. Not verified: a real Keycloak/AAS deployment (the shared verifier has its own real-AAS test in `libs/go-common`).

## Audit trail (`audit.go`, `denied.go`)
Topic `audit.events.v1` (JSON, acks=all, idempotent producer, key = `tenant_id`; `auth.denied` key = truncated remote_ip). Never contains request bodies, event data, tokens or headers.
`request_id` = caller's `X-Request-Id` (printable ASCII <= 128) or random. `event_id` is a hash of (tenant, replay, source, action, versions, request_id, outcome, phase): a retry reusing the
`X-Request-Id` can be deduped by consumers. No Kafka configured: JSON lines on stdout (not durable).

Event schema (state-changing actions): `{event_id, ts, tenant_id, subject, action, replay_id, source_id, from_version, to_version, outcome, phase, request_id}`.

| action | phase | outcome | meaning |
|---|---|---|---|
| `replay.activate` / `replay.rollback` | `prepared` | `pending` | published BEFORE COMMIT (required mode only), inside the activation transaction |
| same | `committed` | `success` | the switch is durable (best-effort follow-up in required mode; the ONLY event in best_effort mode, written after COMMIT) |
| same | `aborted` | `aborted` | COMMIT failed (best-effort follow-up). Caveat: a lost COMMIT acknowledgement is ambiguous, the reconciler rule below decides |
| same | - | `rejected` | 404/409 rule refusal, nothing changed (best effort); a noop activation is not audited |
| `replay.started` | `prepared` | `pending` | version allocated, published BEFORE the ReplayJob (required mode); `replay_id` = job id |
| same | `aborted` | `aborted` | follow-up: the job could not be published after the prepared event (row FAILED, version burned) |
| same | `committed` | `success` | best_effort mode only: written after the job was published |
| `replay.plan` | - | `success` | dry run, best effort |

`auth.denied`: `{event_id, ts, action, request_id, path, method, reason: unauthenticated|missing_scope, remote_ip, subject?}`. 401 -> `unauthenticated`; 403 -> `missing_scope` (token verified, `subject` = its `sub`;
omitted for 401). `remote_ip` = `RemoteAddr` truncated to /24 (IPv4) or /48 (IPv6); `X-Forwarded-For` is deliberately NOT trusted. Path without query, no headers, no token. Best effort, never delays the response (async, max 8 in flight),
rate limited to 1 event per 10 s per (remote_ip, reason) with a bounded map (4096 keys; when full and nothing expired, new keys are dropped). Denials are therefore a sample, not a complete count.

Modes (`REPLAY_AUDIT_MODE`):
- `required` (default with Kafka). Activation: `prepared` is published inside the transaction after the new state is written and before COMMIT; if that publish fails (broker down, `REPLAY_AUDIT_TIMEOUT`) the
  transaction rolls back and the API answers `503 audit_unavailable`, nothing changed. After the transaction a best-effort `committed` or `aborted` follow-up is published (a failed COMMIT answers 502). Start: `replay.started`
  `prepared` is published before the job; failure -> `503 audit_unavailable`, nothing published, the allocated version stays burned and its `replay_requests` row is FAILED. Trade-off: PG row locks are held for up to the audit timeout while Kafka is slow.
- `best_effort`: activation and start events are written after the fact (phase `committed`), failures are only logged (the change may be unaudited), but a `success` event is never published for something that did not happen.

Reading the journal: `committed` = switch happened. `aborted` = it did not (barring the lost-ACK caveat). `prepared` with neither follow-up = UNKNOWN (process died or Kafka failed after COMMIT/rollback). A reconciler decides by reading
`source_active_versions` for (tenant_id, source_id) as of/after the event `ts`: if `active_processing_version = to_version` and `switched_at >= prepared.ts` (and `switched_by` = `subject`), the switch happened (treat as committed);
otherwise (a different version, or no row) it did not (treat as aborted). For rollback, `to_version` is the restored version. A later activation by someone else can mask the first one: use `switched_at` ordering and the replay chain (`from_version`).
For `replay.started` unknown: check `replay_requests` row (REQUESTED = job probably published, FAILED = not) and `replay.jobs.v1`.

Verified (fakes): topic/key/body fields, prepared before commit, forced COMMIT failure -> prepared + aborted and never committed, required prepared failure -> 503 and no state, best_effort emits committed only, start audit before job publish
(order checked), start 503 publishes nothing, job publish failure -> aborted, auth.denied for 401/403 (no token/cookie/query leaked, subject only when verified, /24 truncation, rate limit + bounded map, async).
Verified LIVE (PostgreSQL 16 NOBYPASSRLS role + apache/kafka 3.9.0 KRaft, topic read back with `kafka-console-consumer.sh`; env `REPLAY_PG_URL`, `REPLAY_PG_ADMIN_URL` (superuser, only to create/drop the failing trigger),
`REPLAY_KAFKA_IT_BROKERS`, `REPLAY_KAFKA_IT_CONTAINER`): a deferred constraint trigger makes COMMIT fail (SQLSTATE 40001) -> HTTP 502, no `source_active_versions` row, topic holds `prepared` + `aborted` and no `committed`; control run -> `prepared` + `committed`;
start with failing audit -> 503, no job, row FAILED, next version = v+1; start with live Kafka -> `replay.started` prepared; live `auth.denied` keyed by /24 without the token.
Verified LIVE over SASL_SSL (apache/kafka 3.9.0, SCRAM-SHA-512, throwaway CA, ACLs for the user; gated by `REPLAY_KAFKA_TLS_IT_BROKER/_CA/_OTHER_CA/_USER/_PASS`, `go test ./internal/replay -run TestIT_`):
job publisher and audit sink round trip (records read back by an independent consumer); wrong password and untrusted CA both fail with the cause named, no password in the error, nothing lands on the topics. Always-on unit tests cover flag parsing,
the validation matrix, untrusted CA and hostname mismatch (in-process TLS server) and cause surfacing/redaction/expiry. Not verified on the secured broker: SCRAM-SHA-256 and PLAIN (unit only), broker restart/reconnect, user without ACLs. mTLS client certs + rotation without restart: unit tests (`kafka_mtls_test.go`) and live (`REPLAYC_KAFKA_MTLS_IT_*`, `kafka_mtls_it_test.go`, `docs/operations/kafka-tls-sasl-test.md`).
Not verified: stdout fallback is not durable; the reconciler (below) is verified on PG + Kafka but has no scheduler (run it from cron/a Job); behaviour under a real broker outage mid-COMMIT (only simulated).

## Audit reconciler: `replay-controller reconcile-audit` (`internal/replay/auditreconcile.go`)
Resolves `prepared` events that never got `committed`/`aborted` (crash, lost ack). Flags: `-since` (duration or RFC3339, default 24h), `-until`, `-from-offset`, `-dry-run`, `-grace`, `-tx-window`, `-group`, `-max-keys`, `-idle`.
Env: `REPLAY_PG_URL` (required, role must NOT bypass RLS), `ASTRYON_KAFKA_*` (same TLS/SASL as above), `REPLAY_RECONCILE_GRACE` (default 2m), `REPLAY_RECONCILE_TX_WINDOW` (default 1m; same as `-tx-window`, the flag wins; must be a positive duration, an invalid env value logs a WARN and falls back to the default, an invalid flag exits 2), `REPLAY_RECONCILE_GROUP` (default `replay-controller-audit-reconciler`).
- Reads `audit.events.v1` with its own consumer group; offsets are NEVER committed (a window is re-read each run), run one instance. Pairs by (tenant, replay_id, source, action, request_id); memory is bounded by `-max-keys` (200k distinct changes; more = error, narrow the window).
- `prepared` older than the grace and without outcome: PostgreSQL lookup, tenant set per transaction from the EVENT's tenant_id (RLS). Activate/rollback: `source_active_versions` row has `active = to_version`, `switched_by = subject`, `switched_at >= prepared.ts - tx-window` => `committed`, else `aborted`.
  NOTE `switched_at` is `now()` = the START of the activation transaction, which precedes the prepared stamp, so the literal `switched_at >= ts` rule of the audit section would never hold; the tolerance `tx-window` replaces it (a transaction held longer than that, e.g. by lock waits, would be judged aborted: raise `REPLAY_RECONCILE_TX_WINDOW`/`-tx-window`, e.g. `5m`; too large a window can attribute an older switch by the same subject to this event. Covered by `TestJudgeActivationHonoursTxWindow`).
  A different version active with `switched_at` after the event: if `previous_processing_version = to_version` a later activation masked ours => committed; otherwise unresolvable. `replay.started`: `replay_requests` (tenant, source, target version) FAILED => aborted, RUNNING/COMPLETED => committed, REQUESTED/APPROVED/CANCELLED/missing => unresolved (cannot prove whether the job is on replay.jobs.v1).
  A replay START that has NO audit record at all (best_effort mode, or audit publish failed after the job was sent) is invisible to the reconciler, which only starts from audit events: such starts stay unresolved by design; inspect `replay_requests` directly (REQUESTED for long = job maybe never published).
- Resolution event: copy of the prepared event with `phase=committed|aborted`, `outcome=success|aborted`, `resolved_by=reconciler`, `ts=now`, `event_id = sha256(reconciler, prepared.event_id, phase)[:16]`, key = tenant_id. A re-run sees the resolution in the topic and emits nothing.
- Other direction: `committed` whose switch is not in the database, `aborted` whose switch is (within the event's time), or both phases for one change => `ALERT audit contradiction` log line, `contradictions` count + event ids. Outcomes masked by a later change are only counted as `unverifiable`.
- Output: JSON report on stdout (logs on stderr). Exit 0 clean; 1 when unresolved or contradicting events remain (dry run: also when something would be resolved); 2 usage/infrastructure error. Prepared events inside the grace period are counted as `young` and do not fail the run.
Verified: unit tests with fakes (lost ack, crash, masked, already resolved, contradictions, grace, dry run, idempotent ids, started, bounded keys, tenant from event). LIVE (PG 16 NOBYPASSRLS role + apache/kafka 3.9.0, `REPLAY_PG_URL` + `REPLAY_KAFKA_IT_BROKERS`, topic must exist, `go test ./internal/replay -run TestLiveReconciler`): real `PGActivator` lost-ack => committed, crash before COMMIT => aborted, already resolved untouched, committed-without-switch flagged, dry run emits nothing, second run emits nothing (exactly one resolution each, derived ids), cross-tenant lookup sees nothing; the built binary's `-dry-run` exited 1 against the live topic.
Not verified: SASL_SSL consumer path (shares the producer's TLS/SASL helpers, not exercised for consuming), consumer-group rebalance with several instances, topics with many partitions under load, `-from-offset`/`-until` against a live topic, and wrong verdicts where a long-held transaction exceeds `tx-window`.

## Metrics

`REPLAY_METRICS_ADDR` (unset = off) serves `GET /metrics` (Prometheus text). A bare `:9107` binds 127.0.0.1 only; set an explicit host to expose it. Series `astryon_replay_controller_*`: replays_requested/dry_run/approved_total, replays_rejected_total{code}, activations_total{action}, publish_errors_total. No tenant/source/replay-id labels.
