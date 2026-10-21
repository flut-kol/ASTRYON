# Post-rollout: capacity review, cost model skeleton, v1.1 backlog (DPP-071, DPP-072, DPP-074)

Status: templates and skeleton. No production traffic exists, so **no value in this file is a production measurement**. Cells marked
`TODO-owner` or `TBD(measure)` must be filled from real telemetry. Reference numbers quoted from the repo are single-node dev runs and
are labelled as such.

## 1. Capacity-model review template (DPP-071)

Run after each canary stage and after the first full week at 100%. One copy of the table per review; keep reviews in this directory
as `capacity-review-YYYY-MM-DD.md` (not created yet).

### 1.1 Inputs observed (fill from Prometheus/Grafana; dashboard names in `infra/observability/dashboards/`)

| Item | Metric / source | Observed | Model assumed | Delta |
|---|---|---|---|---|
| Events/day, avg and p99 ev/s | `astryon_gateway_*` accepted counters | TBD(measure) | 100M/day = 1,157 ev/s avg (plan §2) | |
| Peak ev/s and duration | gateway rate, 1 min buckets | TBD | 120k ev/s target | |
| Avg and p99 event bytes | gateway request bytes / events | TBD | ~1 KiB (benchmarks) | |
| Tenant skew | top-1 / top-10 tenant share | TBD | 80% hot tenant scenario | |
| Duplicate share, invalid share | quarantine and dup counters | TBD | loadgen profiles only | |
| ACK p50/p95/p99 | `astryon:gateway_ack_p95_seconds:5m` | TBD | p95 <= 250 ms; dev: 420 ms | |
| Kafka lag and drain time after peak | `kafka_consumergroup_lag` | TBD | backlog drains (PERF-04) | |
| Sink rows/s and insert latency | `astryon_clickhouse_sink_*` | TBD | dev drain 30k ev/s per sink | |
| ClickHouse parts, merges, disk growth/day | needs CH exporter (not wired: observability README gaps) | TBD | 619 MiB / 10M rows w/ projection, synthetic | |
| Query mix, p95 by route, throttling | `astryon:query_api_p95_seconds:5m`, `AstryonQueryThrottling` | TBD | PERF-08 20/100/500 | |
| Hot-tenant concurrency vs memory | CH memory errors, `QUERY_MAX_CONCURRENT` x `QUERY_MAX_MEMORY_BYTES` vs node RAM | TBD | rule from dpp-047: concurrency x cap < node memory | |
| File ingest volume, job duration, failures | `astryon_file_*` | TBD | 5 GiB/file limit, 100 GiB/tenant/day | |
| PG control plane size, connections | PG exporter (opt-in profile) | TBD | not modelled | |
| S3 growth/day | storage metrics (Ceph) | TBD | not modelled | |
| CPU/RAM/IO per service vs Helm requests/limits | k8s metrics vs `values.yaml` (gateway 250m/256Mi req, 1 CPU/512Mi limit) | TBD | initial values, never load-tested | |

### 1.2 Questions the review must answer

1. Which resource saturated first at the observed peak (gateway CPU, Kafka disk/network, sink, ClickHouse merges, query memory)?
2. What headroom factor remains at today's peak, and at 2x (path to 1B/day)? State the evidence, not a guess.
3. Do replication (RF=3, acks=all, min ISR=2) and TLS/SASL change the dev numbers? Dev runs had RF=1 and no TLS (dpp-010 limits).
4. Are partition counts (plan hypothesis: 96 for raw/validated/processed) still right given consumer parallelism and skew?
5. Are retention defaults (CH 400 d, aggregates 800 d, Kafka raw 7 d) consistent with disk and cost?
6. Do alert thresholds (all "initial guardrails") need retuning from real distributions? List each alert that fired and was it right.
7. Any hot-tenant isolation failure (queries, exports, replays)? Link incidents.

### 1.3 Outputs

Updated node/replica counts per service, updated Helm resources, updated query caps, list of benchmarks to rerun (PERF-xx with new
parameters), decision log entries with owner and date. Review owner: TODO-owner.

## 2. Cost-per-million-events model, skeleton (DPP-072)

Goal: compute `cost per 1M events = monthly_total_cost / (events_per_month / 1e6)`, split by component. All prices and server counts are
`TODO-owner` (the plan warns against fixing a server count before a real mix test, §17).

### 2.1 Workload inputs

| Symbol | Meaning | Value | Source |
|---|---|---|---|
| E | events per day | 100e6 early; 1e9 stretch | plan §2 |
| B | average stored event size (bytes, on wire) | ~1,024 for benchmarks; real: TBD(measure) | dpp-010 |
| F | share of volume arriving as files | TBD(measure) | |
| D | duplicate/invalid share | TBD(measure) | |

### 2.2 Derived quantities (formulas; example arithmetic only)

| Quantity | Formula | Example at E=100e6, B=1 KiB (arithmetic, not a measurement) |
|---|---|---|
| Average rate | E / 86,400 | 1,157 ev/s |
| Raw ingest bytes/day | E x B | ~95 GiB/day |
| Kafka storage | E x B x kafka_stages x retention_days x RF | stages = raw, validated, processed with retention 7/3/3 d (plan §5.2); RF = 3; compression factor TBD(measure) |
| ClickHouse storage | E x retention_days x bytes_per_row_compressed x replicas | bytes/row: dpp-047 stored 10M synthetic rows (payload 200-500 B) in 619 MiB **with** the rejected projection, i.e. at most ~65 B/row compressed there; the default schema was not recorded separately. Real value TBD(measure). Retention 400 d (migration 0006); replicas 2 (plan §7: 2 shards x 2 replicas) |
| S3 storage | raw files + archive + checkpoints | TBD(measure) |
| Sink capacity needed | peak ev/s / sink ev/s per instance | dev drain ~30k ev/s per sink (laptop host, not a capacity figure) |
| Gateway instances | peak ev/s / ev/s per instance | dev single node: 45.3k ev/s accepted, p95 420 ms; per-instance sustainable rate TBD(measure) |
| Query nodes | peak concurrent heavy queries x `QUERY_MAX_MEMORY_BYTES` (2 GB default) <= node RAM | rule from dpp-047 |

### 2.3 Cost lines (fill with prices)

| Line | Quantity driver | Unit price | Monthly | Owner |
|---|---|---|---|---|
| Compute: gateway, processing (Flink), sink, query, workers | vCPU-h, from capacity review | TODO-owner | | |
| Kafka brokers (NVMe, 3+) | storage + network | TODO-owner | | |
| ClickHouse (2 shards x 2 replicas + Keeper) | storage + RAM | TODO-owner | | |
| PostgreSQL HA | fixed | TODO-owner | | |
| S3 / Ceph | GiB-month + requests | TODO-owner | | |
| Network egress (exports, signed downloads) | GiB out | TODO-owner | | |
| Observability stack | retention x volume | TODO-owner | | |
| Backup / DR target | GiB + restore drills | TODO-owner (RPO/RTO undecided) | | |
| AAS usage | external, not charged here | TODO-owner | | |
| On-call / support | people | TODO-owner | | |

Result rows: `total / E_per_month x 1e6` overall, and per component; sensitivity to B (x2), retention (x0.5, x2), duplicates, skew.
Do not publish a figure until section 2.1 and 2.3 have measured/priced inputs.

### 2.4 Storage lifecycle levers to evaluate (DPP-072)

Evidence-backed levers already in the repo: ClickHouse TTL (`ttl_only_drop_parts`, migration 0006) and S3 for raw originals; count aggregates from
the pre-aggregated 0003 tables avoid per-row dedup (~50 ms vs ~1-2 s on an 8M-row tenant, dpp-047); events projection rejected (about 2x storage for -10%
latency). Not yet decided: tiered storage (cold volume or S3-backed ClickHouse disk), per-tenant retention, Kafka retention reduction.

## 3. v1.1 backlog (DPP-074)

Items from the plan plus gaps found in this review. Priority and owners are proposals (TODO-owner). Nothing here is committed scope.

| ID | Item | Why / evidence | Dependency or trigger |
|---|---|---|---|
| B1 | Advanced full-text search (OpenSearch or ClickHouse text indexes) | no requirement yet; ADR says defer | triggers in `docs/architecture/adr-opensearch-decision.md` |
| B2 | Lakehouse / Iceberg on S3 | plan ADR-014: after a PoC; no code | confirmed analytics use case, cost model |
| B3 | OCR / AI document workers | no code; S8 in `docs/discovery/source-catalog.md` | source demand, throughput target (AP-19) |
| B4 | Self-service pipeline builder | plan DPP-074; pipeline versions exist only as replay processing_version | admin roles in AAS (not created yet, `roles.md`) |
| B5 | Parquet/Avro/Protobuf file formats | not accepted by file-api | demand (S5) |
| B6 | Malware scan for uploads | README: "Not yet: malware scan" | security review |
| B7 | End-to-end freshness metric (5 s SLO) | no metric (observability README) | needed for AP-06 |
| B8 | Availability SLI recording rules | incident-management: SLI undefined | needed for 99.9% budget |
| B9 | Runtime feature flags with audit and per-tenant scope | none today (`feature-flags.md`) | canary cohort mechanism decision |
| B10 | Exporters and rules: Kafka URP, CH merges/parts, PG replication, Ceph, Flink backpressure | observability README "Remaining gaps" | exporters deployed |
| B11 | Serve count aggregates from 0003 tables; document approximate vs exact counts | dpp-047 finding 4 | API contract decision |
| B12 | Per-tenant query concurrency tied to node memory, lower limit for hot tenants | dpp-047 finding 3 | capacity review |
| B13 | DPoP Redis replay store live test | dpop-plan: core only | decision to require DPoP |
| B14 | Vault `SecretRef` resolver; SSRF controls for REST_PULL endpoints | connector-manager README, catalog S6 | before user-defined connectors |
| B15 | Align scope names between `authentication.md` and code; create roles in AAS | PROJECT_INDEX roles row | AAS owner |
| B16 | Rolling upgrade and reshard runbooks (DPP-064) | not written | |
| B17 | Rehearse restore, DR, chaos (Sprint 13 items) | `operational-acceptance.md` unchecked | |
| B18 | Migrate three legacy quarantine producers to strict `QuarantineRecordV1` | PROJECT_INDEX event-contracts row | |
| B19 | MySQL CDC verification, CDC schema evolution, snapshot + tail sync | connector README "Not yet" | real source |
| B20 | Rerun benchmarks with RF=3, acks=all, TLS/SASL, AAS verifier, processor and sink; meet ACK p95 250 ms and 120k ev/s or re-agree targets | dpp-010 limits | PERF-01..08 |

Final report for the Sprint 15 gate (throughput, failures, data quality, SLO, cost, backlog) is assembled from the capacity review (section 1),
cost model (section 2) and this backlog; owner TODO-owner.
