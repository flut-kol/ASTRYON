# Feature-flag and switch inventory (DPP-068)

Status: inventory of what exists in code on 2026-10-21. **There are no runtime feature flags**: no flag service, no
hot reload, no per-tenant toggle store (`docs/operations/release-rollback.md`, "Що є насправді"). Every switch below is a
process environment variable read at start-up, so changing it means a restart (Helm `env`/`secretEnv` in
`infra/kubernetes/charts/astryon-services/values.yaml`, or compose env). Defaults are quoted from the code; "unset" means the
feature is off. Do not add flags here that are not in code.

## 1. Behaviour switches (safe to use as canary levers)

| Switch | Service | Default | Effect | Rollback by | Source |
|---|---|---|---|---|---|
| `services.<name>.enabled` | Helm chart (all services) | `true` | deploy or remove a workload | set false / revert values | `charts/astryon-services/values.yaml` |
| `services.<name>.replicas` | Helm chart | 1-2 per service | scale a workload | revert values | same |
| `CHSINK_INTENT_TABLE` | clickhouse-sink | unset (off) | durable chunk-boundary intents so a crash between insert and commit does not store rows twice; needs CH migration 0004. Cost measured as ~2.7-3.9 ms p50 per chunk and no throughput difference at batch 5000 | unset + restart (rows already stored stay) | `internal/sink/config.go`, `docs/tests/sink-crash-restart.md` |
| `QUERY_AGG_CACHE_SECONDS` | query-api | unset (no cache) | cache aggregate responses | unset + restart | `cmd/query-api/main.go` |
| `QUERY_PG_URL` | query-api | unset | when set, `processing_version` becomes optional and resolves to the source's active version (cutover from replay); unset = version required on every request | unset + restart (clients must then send the version) | `cmd/query-api/main.go`, README |
| `QUERY_VERSION_CACHE_SECONDS` | query-api | 5 | how fast a version cutover becomes visible | edit + restart | same |
| `QUERY_REDIS_URL` (+ `_FAILURE=local|closed`, `_REPLICAS`) | query-api | unset | shared per-tenant rate/concurrency limits instead of per instance | unset + restart | query-api README |
| `QUERY_RATE_PER_SEC`, `QUERY_BURST`, `QUERY_MAX_CONCURRENT`, `QUERY_MAX_EXECUTION_SECONDS`, `QUERY_MAX_RESULT_ROWS`, `QUERY_MAX_MEMORY_BYTES` | query-api | 10, 20, 8, 30, 1001, 2e9 | per-tenant and per-query guards (load shedding lever) | edit + restart | query-api README |
| `AAS_REQUIRE_DPOP` | gateway, query-api, quarantine, file-api | off (`!= "true"`) | require DPoP-bound tokens; needs `AAS_REDIS_URL` for a shared replay store | unset + restart | `cmd/*/aas.go` |
| `AAS_INTROSPECTION_KEY_FILE` | query-api | unset | L3 live token status check, fails closed if AAS is down | unset + restart | `query-api/cmd/query-api/aas.go` |
| `ASTRYON_KAFKA_TLS`, `_SASL_*` | gateway, replay, quarantine, connector-manager, retry | off | Kafka TLS/SASL on the client side | edit + restart | `kafka_publisher.go` etc. |
| `FILE_RETRY_MODE` | file-worker | `local` | `controller` routes retryable failures through retry-controller | `local` + restart | `file-worker/main.go`, README |
| `FILE_CREDENTIALS_POLICY`, `_OVERRIDES` | file-worker | `quarantine` | credential-in-file policy `quarantine|block|allow`, per tenant/source overrides | edit + restart | file-ingestion README |
| `FILE_CONTENT_TYPES`, `FILE_MAX_BYTES`, `FILE_MAX_PARTS`, `FILE_QUOTA_OPEN`, `FILE_QUOTA_BYTES_PER_DAY` | file-api | ndjson+csv, 5 GiB, 1000, 20, 100 GiB | upload admission limits (per-tenant overrides in `tenant_quotas`) | edit + restart | `file-api/main.go` |
| `ASTRYON_GATEWAY_MAX_BODY_BYTES`, `_MAX_BATCH_BYTES`, `_MAX_BATCH_EVENTS` | gateway | 2 MiB, 8 MiB, 100 | ingestion request limits | edit + restart | `gateway/config.go` |
| `ASTRYON_CDC_TOPIC` (+ `_CONNECTOR_ID`, `_TENANT_ID`, `_SOURCE_ID`, `_SCHEMA_ID`, `_PRIMARY_KEY`, ...) | connector-manager | unset (CDC off) | start the Debezium consumer | unset + restart (Kafka group offsets keep position) | `cmd/connector-manager/cdc.go` |
| `ASTRYON_RETRY_DISPATCH_TOPIC` | retry-controller | unset | enable the Kafka dispatcher | unset + restart | `retry-controller/main.go` |
| `REPLAY_AUDIT_MODE` | replay-controller | unset = `required` if Kafka configured else `best_effort` | whether activation needs a durable audit event | edit + restart | `replay/audit.go` |
| `EXPORT_WORKERS`, `EXPORT_MAX_PER_DAY` (0 = no cap), `EXPORT_MAX_CONCURRENT_PER_TENANT` (2), `EXPORT_URL_TTL_SECONDS` | export-worker | 2, 0, 2, 900 s | export throughput and quotas | edit + restart | `export-worker/main.go` |
| `AGG_FAIL_ON_GAPS` | ch-aggregator | off | non-zero exit on empty-hour gaps | unset | `ch-aggregator/main.go` |
| `BFF_ALLOWED_ORIGINS`, `BFF_UPSTREAMS`, `BFF_SCOPES` | admin-bff | unset | cross-origin UI, proxied upstreams, requested scopes | edit + restart | `admin-bff/main.go` |

Data-level levers that are not env switches: per-source active processing version (`POST /v1/replays/{id}/activate?rollback=true`,
table `source_active_versions`), connector pause/resume (Admin UI and connector API), ClickHouse TTL (migration 0006; forward-only).

## 2. Development-only switches (must be absent in canary and production)

| Switch | Service | Why it is dangerous |
|---|---|---|
| `ASTRYON_DEV_AUTH=1`, `ASTRYON_DEV_TOKEN`, `ASTRYON_DEV_TENANT_ID`, `ASTRYON_DEV_SCOPES` | gateway | static bearer token, tenant fixed by env |
| `ASTRYON_DEV_FAKE_PUBLISHER=1` | gateway | accepts events without Kafka: acknowledged events are lost |
| `QUERY_DEV_*`, `FILE_API_DEV_*`, `EXPORT_DEV_*`, `REPLAY_DEV_*`, `QUARANTINE_DEV_*`, `QUARANTINE_ENABLE_DEV_AUTH=true`, `ASTRYON_CONNECTOR_DEV_*` | per service | dev-token auth mode |
| `BFF_COOKIE_INSECURE=1` | admin-bff | session cookie without `Secure` |

Release gate: the Helm `requiredEnv` list covers AAS and storage endpoints; **TODO-owner**: add a check (values schema or CI grep)
that none of the section 2 variables appears in stage/prod values.

## 3. Gaps

- No tenant-percentage or traffic-weight flag exists anywhere in the services. Canary cohorts therefore rely on routing outside
  the repo (see `canary-plan.md`, TODO-owner).
- No audit trail for changing a switch other than the GitOps history of the values file.
- OpenSearch, Iceberg, OCR: no switch because no code (see `post-rollout.md`).
