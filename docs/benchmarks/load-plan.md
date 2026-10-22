# Load and saturation plan (Sprint 12: DPP-002, DPP-056..060)

Status: tooling written and unit-tested; scripts smoke-tested only against a fake HTTP gateway. **No scenario has been
measured against the real stack.** The dev gateway was not running when this was written, so every number a report
contains must come from a run you do, on the environment recorded in its `environment.json`. Dev-scale numbers are not
production capacity.

## Tooling

| piece | where |
|---|---|
| generator, profiles, burst, query, backlog, checks | `tests/load` (`go build` -> `loadgen`; `loadgen -h`) |
| scenario runners | `scripts/load/*.sh` (fail closed with exit 2 when the gateway is unreachable; they never emit a report without a run) |
| reports | `$OUT` (default `reports/load/<UTC stamp>/`): one JSON per scenario plus `environment.json` |

Credentials come from `LOADGEN_TOKEN` + `LOADGEN_TENANT_ID` or `LOADGEN_TENANTS="<uuid>=<token>;..."`, never flags.

## Workload profiles (`loadgen -profile NAME`)

| profile | shape | expected gateway outcome |
|---|---|---|
| `small-json` | ~512 B flat JSON, batch 100; the 120k ev/s reference profile of plan section 2 | all accepted |
| `wide-json` | ~4 KiB, 120 fields plus nested object, batch 50 | all accepted |
| `skewed-tenants` | Zipf(1.2) over tenants, 20% of events on one `entity_key` (hot key); needs several tenants | all accepted; look for per-tenant or per-partition hotspots downstream |
| `duplicates` | 10% re-sent events (same `event_id`, same bytes) | all accepted; downstream result must have no unaccounted duplicates (plan section 2) |
| `late-events` | 20% of events with `event_time` up to 7 days old | all accepted; check partition/late handling |
| `invalid-schemas` | 5% schema-level invalid (empty `schema_id`, `schema_version <= 0`) | quarantined, no 5xx, no effect on valid events |
| `mixed` | 1 KiB, 5% dups, 5% late (24 h), 1% invalid | used by the daily-equivalent runs |

Explicit `-batch` / `-event-size` override the profile. Ratios the profile sets are fixed by it.
Generation is deterministic per `-seed`.

## Scenarios

| id | task | command | what it does |
|---|---|---|---|
| S1 | DPP-056 | `scripts/load/profiles.sh [profile...]` | each profile at `RATE` (default 500 ev/s) for `DURATION`; writes accepted ids (`-ids-out`) for downstream reconciliation |
| S2 | DPP-056 | `scripts/load/daily-equivalent.sh 100m` | 100M/day = 1157 ev/s average; target = average x `PEAK_FACTOR` (3) x `DEV_SCALE` (0.05) |
| S3 | DPP-056 | `scripts/load/daily-equivalent.sh 1b` | 1B/day = 11574 ev/s average, same scaling. Full-scale needs a multi-node stack and load-generator hosts; set `DEV_SCALE=1` only there |
| S4 | DPP-057 | `scripts/load/burst.sh` | baseline (1k ev/s) -> burst (120k ev/s, 30 s) -> baseline; samples the consumer backlog from Prometheus (`PROM_URL`, `BACKLOG_QUERY`) and reports peak and seconds from burst end until backlog <= threshold |
| S5 | DPP-058 | `scripts/load/saturation.sh ladder` | rate ladder (`RATES`), stops after two consecutive failing steps; writes `capacity-curve.json` and `max_stable_target` |
| S6 | DPP-058 | `scripts/load/saturation.sh hotkey` | `skewed-tenants` profile; compare against S1 `small-json` at the same rate |
| S7 | DPP-059 | `scripts/load/query-while-ingest.sh` | ingest alone, then ingest plus concurrent query-api GETs; `isolation.json` gives the ACK p95 ratio |

Host saturation (CPU, RAM, disk, network on gateway, Kafka, Flink, ClickHouse) is not sampled by the tools: capture the
dashboards in `infra/observability` for the same window and attach screenshots or exports to the report. DPP-060 tuning
(Kafka partitions/brokers/producer batching, Flink parallelism/checkpoints, ClickHouse inserts/parts) is done by repeating S5/S4
after each single change; record one change per run in the report's "Tuning" table.

A 120k ev/s burst needs a generator host that can sustain it (HTTP + JSON building is CPU bound): check
`achieved_rate_fraction` first; if the generator is the bottleneck, run several `loadgen` instances and add up the reports.

## Pass criteria (from plan section 2)

| check (JSON name) | limit | source / caveat |
|---|---|---|
| `ack_p95_ms` | <= 250 ms in LAN | plan s2 "p95 ACK ... a target, not an SLA until tested"; healthy Kafka, nominal load |
| `failed_fraction` | 0 | a batch item not acknowledged; every ACKed event must reach Kafka (`acks=all`, RF=3, min ISR=2) |
| `achieved_rate_fraction` | >= 0.95 of target | otherwise the run measured the generator, not the stack |
| `backlog_drain_sec` | <= `MAX_DRAIN_SEC` (300 s default) | plan s2 gives no number: "sustained peak only with enough sink capacity and backpressure"; the limit is a working assumption to be agreed. Backlog must reach threshold and no events be lost |
| `query_p95_ms` | not enforced by default | plan has separate analytical-query SLOs without numbers; set `-slo-query-p95-ms` once agreed |
| `isolation_pass` (S7) | ACK p95 with queries <= 1.5x ingest-only, zero query errors | 1.5x is a working assumption (`ACK_DEGRADE_MAX`) |

Criteria that the generator cannot check and the reporter must verify by hand, with queries on the stack:
- **End-to-end p95 <= 5 s** (realtime small message, plan s2): compare `event_time`/ingest time with availability in ClickHouse for a sample of the accepted ids (`*.ids`). loadgen measures ACK only.
- **No unaccounted duplicates**: `SELECT count(), uniqExact(event_id)` over the accepted ids per tenant; `duplicates` profile re-sent events must collapse via contract dedup.
- **No loss of acknowledged events**: accepted ids (unique, from `-ids-out`) versus rows in the processed/normalized tables.
- **Quarantine accounting** (`invalid-schemas`): quarantined count in the report equals the quarantine topic/table count.
- **Backpressure**: during S4 the gateway must answer 429/503 (counted as `failed`/`http_status`), not drop silently or exhaust memory.

## Report template

Copy to `docs/benchmarks/load-<yyyy-mm-dd>.md` and fill from the JSON. Leave a cell empty rather than guessing.

```
# Load report <date>

Verdict: PASS | FAIL | PARTIAL.  Measured on: <environment.json: commit, host, cpus, dev_scale>
Stack: <gateway replicas, Kafka brokers/partitions, Flink parallelism, CH nodes/shards>, versions, limits.
Generator: <host, cpus>; achieved_rate_fraction <x>.

## Results
| scenario | profile | target ev/s | achieved ev/s | ack p50 | p95 | p99 | failed | quarantined | http 4xx/5xx | checks |
|---|---|---|---|---|---|---|---|---|---|---|

## Burst (S4)
peak backlog | seconds to drain after burst end | gateway 429/503 during burst | memory high-water

## Capacity curve (S5): max stable target, first failing target and failing check
## Isolation (S7): ack p95 ingest-only vs with queries, ratio, query p95, query errors
## Downstream verification: e2e p95, duplicate check, loss check (ids file vs tables), quarantine count
## Saturation: first saturated resource per component (CPU/RAM/disk/net), evidence link
## Tuning (DPP-060): change | before | after | keep?
## Degradation actions: what to do when <check> fails (Gate: "actions on degradation")
## Not measured / caveats
```

Gate for Sprint 12: performance report, maximum stable rate, capacity curve, degradation actions.
