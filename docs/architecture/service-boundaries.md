# Service boundaries and data contract ownership (DPP-004)

Plan refs: §3, §4, §5, §6. Ownership rule (AGENTS.md): one agent/team per subtree; do not edit other subtrees. "Status" reflects the repo at 2026-10-20 (see `PROJECT_INDEX.md`).

## Services

| Service / lib | Responsibility | Consumes | Produces | Owns data | Status |
|---|---|---|---|---|---|
| `services/gateway` | Authenticate (AAS JWT), derive `tenant_id` from claims, validate envelope, publish; quarantine invalid | HTTP `/v1/events[/batch]` | `raw.events.v1`, `events.quarantine.v1` | none (stateless) | implemented |
| `services/stream-processor` | Validate/normalize/dedup (Go instead of Flink, ADR-015) | `raw.events.v1` | `processed.events.v1`, quarantine/DLQ | bounded dedup state only | in progress |
| `services/clickhouse-sink` | Idempotent batched insert to ClickHouse | `processed.events.v1` | ClickHouse `events_normalized_*` | sink offsets/dedup token | implemented |
| `services/query-api` | Tenant-scoped reads from ClickHouse | HTTP (API v1, §8) | responses | none | in progress |
| `services/control-plane` | Tenants, sources, schemas, jobs, quotas, replay requests, audit | HTTP | PostgreSQL rows, outbox events | PostgreSQL control plane | planned |
| `services/connector-manager`, `file-ingestion`, `batch-orchestrator`, `retry-controller`, `search-sink` | Connectors/CDC, file jobs, batch, scheduled retries, optional search | per plan §4 | filejobs/jobstatus topics, S3 | PG job tables, S3 | planned |
| `libs/event-contracts` | Envelope, validation, topic names, dedup key | - | - | contract code | done |
| `libs/go-common` | Logging, AAS JWT verification | - | - | shared lib, no business logic | partly done |

## Data stores

| Store | Written by | Read by | Notes |
|---|---|---|---|
| Kafka topics (§5.2) | producer named in table above only | downstream consumers | one producer service per topic |
| PostgreSQL (control plane) | control-plane (and migrations in `db/postgres`) | services via control-plane API or read-only config | no per-event status; RLS tenant scope |
| ClickHouse | clickhouse-sink; aggregation job (planned) writes aggregate tables | query-api | migrations in `db/clickhouse` |
| S3 | file-ingestion / workers / archiver | workers, replay | controlled buckets only |

## Data contract ownership

| Contract | Location | Owner | Change rule |
|---|---|---|---|
| Event envelope v1 | `docs/events/schemas/envelope.v1.json`, `libs/event-contracts` | event-contracts owner (Architect/Tech Lead approves) | backward-compatible additions only within v1; breaking change = new version and new topic suffix; contract tests required |
| Kafka topic names/config | `infra/kafka/topics.yaml`, `libs/event-contracts` | platform/infra owner | partition counts are hypotheses until POC |
| PostgreSQL schema | `db/postgres/migrations` | control-plane/db owner | forward-only + rollback note |
| ClickHouse schema | `db/clickhouse/migrations` | analytics/db owner | forward-only; coordinated with clickhouse-sink and query-api |
| HTTP API | `docs/api/` (OpenAPI) | API owner per service | version in path (`/v1`) |
| Auth/claims | `docs/architecture/authentication.md` | security owner | AAS is external; no copied auth code |

## Boundary rules
1. `tenant_id` comes only from validated token claims at the edge; internal services trust the envelope field set by the gateway, never client-supplied headers/body.
2. A service never reads another service's private store; it uses a topic, an API, or the shared contract library.
3. Each Kafka topic has exactly one producing service.
4. Sinks are idempotent; consumers commit offsets only after durable write (ADR-011).
5. Dependencies on shared libs go through `go.work` modules; no cross-service package imports from `internal/`.
6. Large payloads go to S3 and travel as `object_ref` (ADR-012).
