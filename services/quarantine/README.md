# Quarantine browser and replay policy

This service is the deployable read side of DPP-042. It serves
`GET /v1/quarantine` from ClickHouse's bounded deviation journal and keeps the
tenant boundary at both authentication and storage layers.

## HTTP contract

Query parameters:

- `limit`: 1..100, default 50;
- `cursor`: opaque keyset cursor returned as `next_cursor`;
- `include_original=true`: returns the bounded/masked `sample_payload` from
  `processing_errors`; it does **not** fetch the raw object referenced by
  `raw_ref`. Such records carry `payload_kind: "masked_sample"` so a client
  cannot mistake the value for the original bytes.

`astryon:quarantine:read` is required for metadata.
`astryon:quarantine-payload:read` is additionally required before the storage
query can select `sample_payload`. The tenant always comes from the validated
AAS claim; a query parameter or header cannot override it. Cursors are bound to
a hash of that tenant and cannot be reused across tenants.

Responses carry `X-Request-ID`. Errors use the common envelope:

```json
{"error":{"code":"FORBIDDEN","message":"quarantine read scope required","details":null,"trace_id":"..."}}
```

`GET /healthz` is an unauthenticated process-liveness endpoint. It does not
claim ClickHouse readiness; browser backend failures remain fail-closed as 503.

### Replay dry-run

`POST /v1/quarantine/replays/dry-run` builds a read-only replay plan. It does
not enqueue, publish, or execute a replay. The caller needs
`astryon:quarantine-replay:read`; the tenant is taken only from the validated AAS
claim and is not part of the request body. The endpoint requires
`Content-Type: application/json`, rejects unknown fields, and accepts:

```json
{
  "source_id": "00000000-0000-4000-8000-000000000001",
  "occurred_from": "2026-10-01T00:00:00Z",
  "occurred_to": "2026-10-02T00:00:00Z",
  "pipeline_version": 2
}
```

`source_id` must be a UUID, timestamps must be RFC3339 and form a non-empty,
half-open range `[occurred_from, occurred_to)` of at most 31 days, and
`pipeline_version` must be a positive `UInt32`. The ClickHouse estimate is
tenant/source/version/range-bound and stops at 10,001 rows. Responses report at
most `candidate_count: 10000`; if more records exist,
`candidate_count_is_lower_bound` is true and `executable` is false so an
operator must narrow the range. Every response carries `X-Request-ID` and uses
the common error envelope on failure. Dry-run performs only the bounded
ClickHouse read: it does not publish a job or write an audit event.

### Replay publication

`POST /v1/quarantine/replays` accepts the same selection body but requires the
separate `astryon:quarantine-replay:write` scope. The handler repeats the
tenant-bound plan and publishes only when the exact count is at most 10,000;
an over-limit plan returns `422 REPLAY_SCOPE_TOO_LARGE` with no audit or job
side effect. A `tenant_id` in the body is an unknown field and is rejected.

Publishing uses the dedicated `quarantine.replay.jobs.v1` contract (override
with `QUARANTINE_REPLAY_TOPIC`) rather than `replay.jobs.v1`. The latter is a
DPP-043 processing-version contract consumed by `replay-worker`; mixing a
quarantine selection into it would produce a permanently invalid job. The job
contains the claim-derived `tenant_id`, source, half-open time range, pipeline
version, exact bounded candidate count, generated job ID, and creation time.

Audit is mandatory for publication. Before the job is sent, a `prepared`
`replay.started` event (using the shared replay-controller audit JSON contract)
must be durably acknowledged on
`audit.events.v1`; an audit failure returns `503 AUDIT_UNAVAILABLE` and no job
is published. After the broker acknowledges or rejects the job, a best-effort
`committed` or `aborted` outcome is written. The prepared event is the
fail-closed authorization record, so an outcome-sink outage after a job ACK
cannot falsely turn an already queued job into an HTTP failure.

## Durable read model

`ClickHouseStore` reads `processing_errors_distributed` by default (migration
`db/clickhouse/migrations/0005_processing_errors_distributed.up.sql`). It:

- binds the claim-derived tenant as a ClickHouse `UUID` parameter;
- uses a deterministic descending keyset over the complete error identity;
- enables `readonly`, a 10-second execution limit, 101-row result cap, and a
  64 MiB memory cap;
- does not select `raw_ref` and selects `sample_payload` only after the separate
  payload scope succeeds;
- accepts an alternate table through `QUARANTINE_CH_TABLE`, restricted to one
  plain ClickHouse identifier so configuration cannot inject SQL.

The source table has a 90-day TTL. The browser is therefore durable across
service restarts but is not a permanent archive.

## Run

Production auth uses the shared `libs/go-common/auth` AAS verifier:

```sh
AAS_BASE_URL=https://aas.example/realms/aas \
AAS_ISSUER=https://aas.example/realms/aas \
AAS_AUDIENCE=astryon-quarantine \
AAS_ENV=prod \
CH_URL=http://clickhouse:8123 \
CH_DATABASE=astryon \
go run ./cmd/quarantine
```

Relevant variables are `LISTEN_ADDR` (default `:8085`), `CH_URL`,
`CH_DATABASE`, `CH_USER`, `CH_PASSWORD`, `QUARANTINE_CH_TABLE`,
`AAS_BASE_URL`, `AAS_ISSUER`, `AAS_AUDIENCE`, `AAS_ENV`, and
`AAS_ALLOWED_AZP`. Replay publication additionally uses the repository-standard
`ASTRYON_KAFKA_BROKERS`, `ASTRYON_KAFKA_CLIENT_ID`, `ASTRYON_KAFKA_TLS`,
`ASTRYON_KAFKA_TLS_CA_FILE`, `ASTRYON_KAFKA_TLS_CERT_FILE`,
`ASTRYON_KAFKA_TLS_KEY_FILE`, `ASTRYON_KAFKA_SASL_MECHANISM`,
`ASTRYON_KAFKA_SASL_USERNAME`, `ASTRYON_KAFKA_SASL_PASSWORD`, and
`ASTRYON_KAFKA_DELIVERY_TIMEOUT`. Kafka records require `acks=all`; invalid
TLS/SASL configuration fails startup. If brokers are absent, browser and
dry-run remain available but publication fails closed with 503.

For local development only, `QUARANTINE_DEV_TOKEN` and
`QUARANTINE_DEV_TENANT` enable a static principal. With neither AAS nor the dev
token configured the service rejects every request. Build the container from
the repository root:

```sh
docker build -f services/quarantine/Dockerfile -t astryon/quarantine .
```

Run tests:

```sh
go test ./...
```

### Journal ingestion (opt-in)

Set `QUARANTINE_JOURNAL_GROUP` together with the Kafka and ClickHouse settings
to consume canonical `events.quarantine.v1` records. The consumer validates the
shared `QuarantineRecordV1` contract, inserts tenant-bearing metadata into
`processing_errors_local` (or `QUARANTINE_JOURNAL_TABLE`), and commits Kafka
offsets only after ClickHouse acknowledges the insert. The opaque original is
not copied into the browser read model. The feature is disabled when the group
variable is empty; tenantless `events.dlq.v1` ingestion remains a separate
follow-up because the current journal schema requires a validated tenant UUID.
If a record on `events.quarantine.v1` fails canonical decoding, the consumer
publishes a bounded, provenance-bearing rejection to `events.dlq.v1` and only
then commits the source offset, so one malformed record cannot block its Kafka
partition indefinitely.

The flow was smoke-tested on the dev Kafka/ClickHouse stack on 2026-10-22:
an invalid quarantine value produced `ERR_QUARANTINE_RECORD_INVALID` on the
DLQ with source topic/partition/offset. If the opt-in journal consumer then
encounters a terminal Kafka, ClickHouse, or commit error, the service performs
graceful shutdown and exits non-zero so its container/orchestrator can restart
it; it never continues serving a healthy-looking browser with ingestion stopped.

Journal outcomes are exposed on the optional metrics listener as
`astryon_quarantine_journal_records_total{outcome="inserted|dlq|error"}` and
`astryon_quarantine_journal_commit_errors_total`; no tenant or source labels
are emitted.

## Remaining DPP-042 gaps

The read model is only as complete as writers feeding `processing_errors`.
Ingestion from `events.dlq.v1` into the ClickHouse deviation journal is still
pending. Fetching a raw object through
`raw_ref` needs a separately audited, tenant-checked object-store path and is
not implemented here. Actual replay execution remains pending. Planning is
intentionally based on fields that exist in the deviation journal (`source_id`,
`occurred_at`, and `pipeline_version`); the journal does not currently contain Kafka
topic/partition/offset coordinates, so it does not claim offset-based replay.
Provisioning `quarantine.replay.jobs.v1` and implementing its executor remain
pending outside this service subtree; this slice safely plans and durably
publishes the bounded, tenant-scoped job but does not execute it.
