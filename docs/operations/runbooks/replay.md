# Runbook: replay (replay-controller / replay-worker)

Status: written from the service READMEs and code; no steps here were executed as an incident drill.
Never put event payloads, tokens or secret values in tickets: use replay ids, versions, counters and error codes.

## Rules that hold in every step

- A replay never overwrites data: it gets a NEW `processing_version`; readers use the active version
  (`source_active_versions`), so a failed or abandoned replay is invisible until activated.
- Tenant comes only from the token. Operators act within one tenant; do not work around RLS or use a role with BYPASSRLS.
- Scope: `astryon:replay:write` (create, activate, rollback), `astryon:replay:read` (list/get). Quarantine replays
  additionally need `astryon:quarantine:replay` (dry-run) and `astryon:quarantine:replay-publish` (publish).
- Always dry-run first: `POST /v1/replays` with `dry_run: true` returns the estimated events and day ranges and has no
  side effects. Range <= 31 days, source required.

## Diagnose

1. `GET /v1/replays/{id}`: status (REQUESTED -> RUNNING -> COMPLETED, or FAILED/CANCELLED), `job_id`, timestamps.
2. REQUESTED for long: job not on `replay.jobs.v1` or no worker consuming (worker logs, consumer group lag on that topic).
3. FAILED: publish failed after allocation (version burned, retry allocates the next one) or the worker failed; read worker logs by replay id.
4. API `503 audit_unavailable`: audit Kafka publish failed in `required` mode, nothing was changed. Fix Kafka reachability/credentials; do not set `REPLAY_AUDIT_MODE=best_effort` to push a change through without owner approval.
5. `409 replay_conflict`: version guard fired (index `replay_requests_version_uq`); repeat the request.

## Act

- Retry a failed replay: create a new one (new version). Do not edit `replay_requests` by hand.
- Cutover only after COMPLETED and a check of the result: `POST /v1/replays/{id}/activate` (409 `replay_not_completed`,
  `version_regression` protect against mistakes; same version again is a no-op).
- Undo a bad cutover: `POST /v1/replays/{id}/activate?rollback=true` (409 `rollback_unavailable` if this replay's
  version is not the active one). Data of the rolled-back version stays and can be activated again.
- Stop condition: if counts differ unexpectedly from the dry-run estimate, do not activate; run `ch-aggregator reconcile`
  for the source and escalate to the component owner.

## Audit gaps

After a crash during a change, `prepared` events in `audit.events.v1` may lack `committed`/`aborted`.
Run `replay-controller reconcile-audit -dry-run -since 24h` first (one instance only, offsets are never committed),
review the findings, then run it without `-dry-run`. Results the tool marks unresolved need the owner; do not guess.

## Not covered / unverified

Replay of quarantined records (service executes only planning/publication, see `services/quarantine`), execution-rate
limits during replay under production load, and who moves REQUESTED rows to COMPLETED outside the worker flow are not verified.
