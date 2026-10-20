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
`astryon:quarantine:read-payload` is additionally required before the storage
query can select `sample_payload`. The tenant always comes from the validated
AAS claim; a query parameter or header cannot override it. Cursors are bound to
a hash of that tenant and cannot be reused across tenants.

Responses carry `X-Request-ID`. Errors use the common envelope:

```json
{"error":{"code":"FORBIDDEN","message":"quarantine read scope required","details":null,"trace_id":"..."}}
```

`GET /healthz` is an unauthenticated process-liveness endpoint. It does not
claim ClickHouse readiness; browser backend failures remain fail-closed as 503.

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
`AAS_ALLOWED_AZP`.

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

## Remaining DPP-042 gaps

The read model is only as complete as writers feeding `processing_errors`.
Ingestion from both `events.quarantine.v1` and `events.dlq.v1` into the
ClickHouse deviation journal is still pending. Fetching a raw object through
`raw_ref` needs a separately audited, tenant-checked object-store path and is
not implemented here. Audit events, dry-run replay planning, publishing a
bounded replay job, and actual replay execution also remain pending; the
existing replay package currently enforces only request identity/policy.
