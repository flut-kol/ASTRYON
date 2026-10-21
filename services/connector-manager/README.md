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

## CDC consumer core (DPP-037, DPP-039)

- `internal/cdc`: consumes Debezium JSON change events (`before/after/source/op/ts_ms`, ops c/u/d/r, optional `{schema,payload}` wrapper)
  through a `Feed` interface (franz-go `KafkaSource`, group consumer with auto-commit off) and maps them to envelopes.
  `event_id` = v5-style UUID of connector + source position (LSN/tx or binlog file/pos/gtid; snapshot rows use the snapshot ts) + table + key + op,
  so redelivery dedups. `entity_key` = PK values (`primary_key` columns, else the Kafka record key). Deletes become tombstone-style events
  (`deleted:true`, key only, no row image). Tenant and source come only from the connector mapping config, never the payload.
- `Consumer.RunOnce`: bounded batch -> map -> emit -> save position (optional `PositionStore`) -> commit offsets (commit-after-emit; a crash in between
  re-emits identical ids). Malformed events go to `events.quarantine.v1` (`CDC_MALFORMED_EVENT`, deterministic deviation id from topic/partition/offset) and
  do not stop the stream; a bad mapping config fails the batch loudly instead. Debezium Kafka tombstones (nil value) are skipped.
- `cmd/connector-manager`: off unless `ASTRYON_CDC_TOPIC` is set; then `ASTRYON_CDC_CONNECTOR_ID`, `_TENANT_ID`, `_SOURCE_ID`, `_SCHEMA_ID` (required),
  `_PRIMARY_KEY` (comma list), `_GROUP`, `_SCHEMA_VERSION`, `_PROCESSING_VERSION`. Not yet: reading these from the `connectors` row, a PostgreSQL-backed
  `PositionStore`, live Debezium/Kafka test.

## Admin API audit

When `ASTRYON_AUDIT_PG_URL` is set, the admin API uses the bounded asynchronous PostgreSQL writer from
`libs/audit`. It records `connector.create`, `connector.pause`, `connector.resume`, and `auth.denied` for a
validated principal that lacks a scope. Invalid or missing tokens are not assigned a synthetic tenant and are
not written. Identity, tenant and `acr` come from the validated principal; `amr`, connector config, secret
references, payloads and PII are never stored. Create events contain only connector id, source id and connector
kind; failures add a bounded error code.

Writer bounds are configurable with `ASTRYON_AUDIT_QUEUE_SIZE`, `_BATCH_SIZE`, `_FLUSH_INTERVAL`,
`_WRITE_TIMEOUT`, `_RETRIES`, `_DRAIN_TIMEOUT`, and `_PG_MAX_CONNS`. The metrics listener exposes
`astryon_audit_{queued,written,dropped,rejected}_total`; drops also produce identifier-free log messages. SIGTERM
drains the audit queue before closing its pool.

There are currently no source create/update/pause/resume, connector update, or credential-reference mutation
endpoints in this service. Audit events for those actions must be added with the endpoints rather than inventing
write paths solely for audit.

## Not yet

- Kafka `Emitter` for `raw.events.v1` (gateway has its own TLS/SASL/mTLS producer; extract a shared client first), a `SecretRef` resolver (Vault),
  `cmd/connector-runner`, metrics endpoint wiring (`runner.Metrics` exists), connector API/UI and connection test, Debezium deployment/connector provisioning, schema evolution beyond `cdc.Compatible`.

## CDC tests and position store
- `internal/cdc` live Kafka test (restart resumes from committed offsets, quarantine record): `ASTRYON_KAFKA_IT_BROKER=host:port go test ./internal/cdc`.
- `cdc.PGPositions` writes the last emitted source position to `connector_cdc_position` (migration 0013, tenant RLS; diagnostic only, Kafka group commit is authoritative). Live test: `CDC_POS_PG_URL` (non-superuser role). Wired automatically in `cmd/connector-manager` when `ASTRYON_CDC_TOPIC` is set (ASTRYON_CDC_CONNECTOR_ID must be a connectors.id UUID).
