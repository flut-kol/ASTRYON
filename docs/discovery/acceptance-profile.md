# Acceptance profile, draft (DPP-003)

Status: DRAFT, unsigned. The Sprint 01 gate needs a signed, measurable profile (`DEVELOPMENT_PLAN.md` §9). This draft
lists the targets that already exist in the plan (§2, §11), the latest measured value from this repo, and marks every number
nobody has decided as `TODO-owner`. Nothing here is a new measurement. "Measured" values come from single-node dev runs and
do not prove production capacity (each benchmark file says so).

## 1. Measurable targets

| ID | Dimension | Target (source) | Measured so far (source) | Gap / status | Owner |
|---|---|---|---|---|---|
| AP-01 | Average load | 100M events/day early = ~1,157 ev/s average (arithmetic from §2) | none at this sustained rate | PERF-05 not run | TODO-owner |
| AP-02 | Stretch load | 1B+/day = ~11,574 ev/s average (arithmetic; §2 "after confirmation") | none | not run | TODO-owner |
| AP-03 | Burst | 120k ev/s for small message, 10 min, backlog drains (§2, PERF-04) | 45,318 ev/s accepted at 120k requested, single broker, RF=1 (`dpp-010`) | not met | TODO-owner |
| AP-04 | Sustained tiers | 10k/60 min, 25k/60 min, 50k/30 min (PERF-01..03) | 10 s runs only: 9.9k, 24.5k, 37.5k accepted ev/s | not run for duration | TODO-owner |
| AP-05 | ACK latency | p95 <= 250 ms in LAN (§2, target not SLA) | p95 420.3 ms at max rate, 360.3 ms at 10k (`dpp-010`); 45.8 ms at 500 ev/s with the sink and no TLS/SASL (`docs/tests/e2e-smoke.md`) | not met at load | TODO-owner |
| AP-06 | End-to-end freshness | p95 <= 5 s to analytics availability (§2) | no metric exists (`infra/observability/README.md`) | not measurable yet | TODO-owner |
| AP-07 | Sink drain | no target number in plan | 29.8k-32.0k ev/s draining 200k x ~460 B backlog, one sink, laptop host (`docs/tests/sink-crash-restart.md`) | needs target | TODO-owner |
| AP-08 | Availability | 99.9% per month for ingestion/query after the production gate = ~43 min/30 d error budget (`docs/operations/incident-management.md`) | no SLI recording rule for availability | SLI undefined | TODO-owner |
| AP-09 | Durability | zero loss of confirmed events on single-node failure; Kafka `acks=all`, RF=3, min ISR=2 (§2) | dev broker RF=1; sink kill -9 restart: no loss observed; failover broker IT env-gated | HA/chaos not run | TODO-owner |
| AP-10 | Duplicates | no unaccounted duplicates in defined business results (§2) | e2e smoke: `count()` = accepted incl. client duplicates, `uniqExact(event_id)` = loadgen unique (`e2e-smoke.md`) | reconcile on production-like data not run | TODO-owner |
| AP-11 | Query concurrency | 20/100/500 parallel clients, p95 and budgets documented (PERF-08) | 1 and 16 clients on one node; tail tenant p95 <= 118 ms at 16 clients; 8M-row tenant aggregate p50 ~1.0 s after `uniqExact` change, 16 concurrent hot-tenant queries exceed the 2 GB cap or OOM a 6 GB node (`dpp-047`) | 20/100/500 not run | TODO-owner |
| AP-12 | Query p95 | initial alert guardrail 2.5 s (`prometheus-rules.yaml`, "not agreed SLO") | see AP-11 | SLO value undecided | TODO-owner |
| AP-13 | Tenant skew | tenant X = 80% traffic, no uncontrolled hot partition (PERF-06) | query side only (`dpp-047`); ingestion skew not run | partly | TODO-owner |
| AP-14 | Max batch object | file <= 5 GiB default (`FILE_MAX_BYTES`); backfill 1+ TB with bounded memory (PERF-07) | limits exist in code; no TB run | needs decision on max object size | TODO-owner |
| AP-15 | Max event / request | event body 2 MiB, batch 8 MiB / 100 events (gateway defaults) | benchmarked at ~1 KiB, batch 100 | wide-event size: TODO-owner | TODO-owner |
| AP-16 | Retention | defaults: CH events 400 d, aggregates 800 d, errors 90 d; Kafka raw 7 d (migration 0006, §5.2) | TTL drop verified on CH 24.8 | defaults pending this decision | TODO-owner |
| AP-17 | RPO / RTO | node failure: covered by AP-09. Site failure: "defined after backup/DR is chosen; do not promise RPO=0" (§2) | no restore drill | no numbers | TODO-owner |
| AP-18 | Cost envelope | no number in plan | none | see `docs/release/post-rollout.md` cost skeleton | TODO-owner |
| AP-19 | Batch SLA / OCR | "TB/h import" and "documents/h OCR" listed in §2 without numbers | OCR not implemented | needs numbers or removal | TODO-owner |

## 2. Representative-data requirement (Sprint 01 gate)

The gate also asks for representative test data (DPP-002). Evidence: `tests/load` generates rate/size/tenant skew/duplicates/invalid
events. Not evidenced: late events, wide JSON, file fixtures at TB scale. TODO-owner.

## 3. Sign-off

| Role | Name | Date | Decision |
|---|---|---|---|
| Product / data owner | TODO-owner | | |
| Platform lead | TODO-owner | | |
| Operations | TODO-owner | | |
| Security | TODO-owner | | |

Rule: an AP row becomes binding only when a number replaces every `TODO-owner` in it and a test ID or report is named for it.
