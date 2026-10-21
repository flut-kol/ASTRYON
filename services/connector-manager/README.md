# Connector manager

Sprint 08 (`DPP-036` .. `DPP-040`). Library packages only so far; there is no executable yet.

## REST pull (DPP-038)

- `internal/restpull`: cursor-based GET client with a per-connector rate limiter, bounded response size, `Retry-After` handling
  and an optional `Authorization` header; `Progress`/`NextDelay` give bounded exponential backoff with jitter.
- `internal/pullstore` + migration `0011_connector_pull_state`: one row per enabled `REST_PULL` connector with the durable cursor,
  retry schedule and a fenced lease. `Sync` creates rows, `Claim` leases the next due connector (`FOR UPDATE SKIP LOCKED`, new fence,
  tenant RLS), `Succeed` / `Fail` / `Renew` write only while owner, fence and an unexpired lease match (`ErrFenced` otherwise).
  Live-proven on PostgreSQL 16: claim exclusion, fenced writes, cursor kept on failure, retry time respected, lease takeover, disabled connectors skipped.
- `internal/runner`: claim -> resolve credentials (`Resolver`, from the connector's `secret_ref`) -> pull one page with the persisted cursor ->
  `MapPage` (JSON array, or the array under `records_path`) -> `Emitter.Emit` -> advance the cursor **only after** the emit succeeded.
  `X-Next-Cursor` present = more pages (due immediately), absent = wait `interval_seconds`. Failures keep the cursor and schedule a retry
  (HTTP code, `Retry-After`, `emit_failed`, `bad_response`, `bad_config`, `secret_unavailable`, ...) with 30 s base backoff capped at 1 h.
  Envelopes follow `docs/events/schemas/envelope.v1.json`; `event_id` is a stable UUID from the connector and `id_field` (or the record's SHA-256),
  so a re-pulled page is idempotent downstream; records without a parseable time get `event_time_missing`.
  `connectors.config` keys: `endpoint`, `cursor_parameter`, `schema_id` (required), `schema_version`, `requests_per_second`, `max_response_bytes`,
  `interval_seconds`, `records_path`, `id_field`, `event_time_field`, `entity_field`, `processing_version`.

## Not yet

- Kafka `Emitter` for `raw.events.v1` (gateway has its own TLS/SASL/mTLS producer; extract a shared client first), a `SecretRef` resolver (Vault),
  `cmd/connector-runner`, metrics endpoint wiring (`runner.Metrics` exists), connector API/UI and connection test, CDC (Debezium) wiring, schema evolution.
