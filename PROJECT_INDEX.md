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
| `db/postgres/migrations/` | control-plane schema | §6 | in progress |
| `db/clickhouse/migrations/` | analytics schema | DPP-028 | in progress |
| `infra/docker/`, `infra/kafka/`, `Makefile`, `scripts/dev/` | dev stack | DPP-006 | in progress |
| `services/gateway/` | `POST /v1/events[/batch]`, quarantine, Authenticator iface (`internal/gateway/`, `cmd/gateway/`) | DPP-007/021/023/024 | done; KafkaPublisher (franz-go v1.18.1, smoke-tested on dev Kafka); AAS JWT verifier still stub (fail-closed); TLS/SASL untested |
| `services/clickhouse-sink/` | Kafka processed.events.v1 → CH HTTP insert, token=sha256(topic,partition,first,last offsets), commit after insert (`internal/sink/`) | DPP-008 | done; KafkaSource untested on real broker; Distributed-table dedup unverified |
| `tests/load/` | load generator `loadgen` (module github.com/astryon/loadgen, stdlib + event-contracts): rate/duration/batch/concurrency/size/tenant-skew/dup/invalid; text+JSON report | DPP-009 | done; e2e smoke in `docs/tests/e2e-smoke.md` |
| `libs/go-common/` | logging, authn (AAS JWT verify), http | DPP-014 | planned |

## Conventions
Go 1.22 workspace (`go.work`), one module per service/lib; module prefix `github.com/astryon/<name>`. Ownership: one agent per subtree.
