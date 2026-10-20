# ASTRYON — project index

Read this first; then open only the section of `DEVELOPMENT_PLAN.md` you need (`grep -n '^## ' DEVELOPMENT_PLAN.md`).

## Plan section map (DEVELOPMENT_PLAN.md)
| § | Topic | Lines |
|---|---|---|
| 1 | ADR-001..014 | 12-30 |
| 2 | SLO | 31-42 |
| 3 | Logical pipeline | 43-59 |
| 4 | Monorepo layout | 60-139 |
| 5 | Data contracts: envelope 5.1, Kafka topics 5.2, semantics 5.3 | 140-186 |
| 6 | PostgreSQL control plane | 187-192 |
| 7 | ClickHouse design | 193-203 |
| 8 | API v1 | 204-235 |
| 9 | Sprints 01-15 (DPP-001..) | 236-371 |
| 10-11 | Dependencies, tests/acceptance | 372-421 |
| 12-13 | Security, observability | 422-444 |
| 15-17 | DoD, agent task template, non-goals | 459-507 |

Auth: external AAS (`/var/apps/astranex/aas-auth`), contract in `docs/architecture/authentication.md` (env `AAS_BASE_URL`). Never copy auth code here.

## Code map (status)
| Path | Purpose | Task | Status |
|---|---|---|---|
| `libs/event-contracts/` | Envelope, Validate, topics, dedup key; schema in `docs/events/schemas/` | DPP-022 | done (tests pass) |
| `db/postgres/migrations/` | control-plane schema: all §6 entities across 8 forward-only migrations + rollback-plan comments (0008 fixes 3 FKs that weren't tenant-scoped composite keys, a real cross-tenant data leak path) | §6 | done; RLS tenant isolation, FK enforcement, audit_log immutability, soft-delete, idempotent re-apply all verified live (`db/postgres/schematest/`, env-gated on `DATABASE_URL`) |
| `db/clickhouse/migrations/` | analytics schema: events_normalized_local/distributed, processing_errors_local, processing_quality_daily, source_metrics_hourly, pipeline_metrics_hourly | DPP-028 | schema done (events_normalized + dedup verified live via `services/clickhouse-sink/internal/sink/realinfra_test.go`); aggregate tables have no producer yet by design (written by the DPP-044/045 aggregation/reconciliation job, not an MV — see 0003_aggregates.up.sql header), so nothing to test until that job exists |
| `infra/docker/`, `infra/kafka/`, `Makefile`, `scripts/dev/` | dev stack: Kafka (PLAINTEXT `EXTERNAL:19092` + additive `DEVTLS` SASL_SSL/SCRAM-SHA-512 PEM listener on `19093`, certs via `scripts/dev/gen-kafka-tls.sh`), PostgreSQL, ClickHouse, MinIO (S3 emulator) via `docker-compose.dev.yml` | DPP-006 | done |
| `services/gateway/` | `POST /v1/events[/batch]`, quarantine, Authenticator iface (`internal/gateway/`, `cmd/gateway/`) | DPP-007/021/023/024/014 | done; KafkaPublisher (franz-go v1.18.1) smoke-tested on dev Kafka AND over TLS+SCRAM-SHA-512 against the `DEVTLS` listener (`kafka_tls_sasl_test.go`, env-gated on `ASTRYON_KAFKA_IT_TLS_BROKER`); ingestion integration tests (retry, broker failover, duplicates, malformed, cross-tenant) in `ingestion_integration_test.go` (DPP-025); AAS JWT verifier now real (`AASJWTAuthenticator` via `libs/go-common/pkg/authn`, env `AAS_BASE_URL`/`ASTRYON_AAS_AUDIENCE`) |
| `services/clickhouse-sink/` | Kafka processed.events.v1 → CH HTTP insert, token=sha256(topic,partition,first,last offsets), commit after insert (`internal/sink/`) | DPP-008 | done; KafkaSource + Distributed-table dedup verified against real Kafka/ClickHouse dev stack (`internal/sink/realinfra_test.go`, env-gated like `integration_test.go`), no bugs found |
| `tests/load/` | load generator `loadgen` (module github.com/astryon/loadgen, stdlib + event-contracts): rate/duration/batch/concurrency/size/tenant-skew/dup/invalid; text+JSON report | DPP-009 | done; e2e smoke in `docs/tests/e2e-smoke.md` |
| `tests/e2e/` | MVP-gate end-to-end tests (module github.com/astryon/e2e): real gateway -> stream-processor -> sink -> ClickHouse -> query-api as subprocesses, replay/rollback, tenant isolation, perf measurement; env-gated `ASTRYON_E2E=1` | DPP-030 | done; `docs/tests/mvp-gate.md`, `docs/tests/performance-report-v0.md` |
| `libs/go-common/` | `pkg/logging` (slog JSON + context helpers), `pkg/authn` (OIDC discovery + JWKS AAS JWT verification, ES256/RS256, bounded cache) | DPP-014 | logging, authn (+ DPoP RFC 9449 proof validation `dpop.go`, in-memory replay cache; not wired into gateway; valid-RS256 path untested), `pkg/http` (httpx: request-ID, recover, access-log, body limit, error shape, tenant-from-claims authn middleware; gateway not migrated yet), `pkg/secrets` (env/file/Vault KV v2 refs, redacting `Secret`), `pkg/tlsconf` (mTLS with hot reload) all done (tests pass); no service consumes secrets/tlsconf yet; see `docs/architecture/secrets-and-mtls.md`, `infra/vault/` |
| `.github/`, `.golangci.yml`, `docs/security/supply-chain.md` | CI (`ci.yml`: lint, test+race, CodeQL/gosec SAST, govulncheck/Trivy SCA, gitleaks, CycloneDX SBOM), `release.yml` (cosign keyless signing + SLSA provenance on `v*` tags), Dependabot | DPP-013 | done; workflows YAML-validated locally but not yet run on GitHub; image signing waits for Dockerfiles (DPP-012); action SHA pinning + required checks need org settings |
| `services/stream-processor/` | raw.events.v1 → validate → normalize → bounded TTL+max-entries dedup → processed / quarantine / DLQ; Go consumer instead of Flink (ADR-015); warm-start from processed topic | DPP-026/027 | done; real-Kafka test env-gated on `STREAMPROC_IT_KAFKA`; TLS/SASL not wired |
| `services/query-api/` | `POST /v1/query/events` (cursor), `POST /v1/query/aggregate`, `GET /v1/metrics/summary`, `/v1/quality/summary`, `/v1/sources`, `/v1/jobs`, `/healthz`; tenant from token only; contract `docs/api/openapi.yaml` | DPP-029 | done; ClickHouse path verified live (`QUERYAPI_IT_CH_URL`); PostgreSQL path (sources/jobs) not integration-tested |
| `web/admin-ui/` | Admin UI (Vite + vanilla TS): counters, jobs, sources, health; mock server `npm run mock`, vitest | DPP-030 | done (tests+build); being aligned to openapi.yaml; no cursor paging UI |
| `infra/ansible/` | roles common/disks/network/hardening/node_exporter, dev inventory | DPP-011 | written, NOT validated (ansible/ansible-lint/yamllint not installed); disk format + bonding off by default |
| `infra/helm/`, `infra/kubernetes/`, `infra/argocd/`, `*/Dockerfile` | generic `astryon-service` chart, kustomize base+dev/stage/prod, Argo CD AppProject/ApplicationSets, distroless Dockerfiles for gateway + clickhouse-sink | DPP-012 | done; kustomize renders, Dockerfiles build; Helm/Argo manifests NOT validated (helm/kubeconform missing); repo URL/domains are placeholders |
| `infra/observability/`, `docs/operations/observability.md` | Prometheus, Alertmanager, Grafana (4 dashboards), OTel Collector, Loki, Tempo, Alloy, exporters; §13 alert rules | DPP-015 | written, `docker compose config` OK, rules not promtool-validated; gateway (`ASTRYON_GATEWAY_METRICS_ADDR`, :9101) and sink (:9102) now expose Prometheus `/metrics`; alerts/panels untested against live scrape |
| `docs/tests/performance-report-dpp010.md`, `scripts/dev/bench.sh` | 10k/25k/50k/120k events/s run on a single laptop | DPP-010 | done: 120k NOT met; max ~46k ev/s, p95 ack 1.7 s (target 250 ms); bottleneck = gateway sequential per-event `ProduceSync`; fixed by batched `PublishBatch` (linger `ASTRYON_KAFKA_LINGER`=2ms) but NOT yet re-measured; rerun needed on a quiet host |
| `docs/` | source catalog, ADR-001..015, service boundaries, acceptance targets (hypotheses), perf profile; `CONTRIBUTING.md`, `.github/` templates | DPP-001..005 | done (docs only) |

## Conventions
Go 1.22 workspace (`go.work`), one module per service/lib; module prefix `github.com/astryon/<name>`. Ownership: one agent per subtree.
