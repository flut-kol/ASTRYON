# Acceptance profile and targets (DPP-003)

Source: DEVELOPMENT_PLAN.md §2 (SLO), §5.2, §11. Values marked **[plan]** are stated in the plan as targets/design requirements (not guarantees). Values marked **[HYPOTHESIS]** are placeholders to be confirmed in POC and signed off; **[UNKNOWN]** needs a business/infra decision. No measured numbers appear here. This document becomes the "signed measurable acceptance profile" (Sprint 01 gate) only after review.

| Dimension | Target | Basis |
|---|---|---|
| Average load | 100 M events/day early prod; up to 1 B+/day later (~11,574 ev/s avg at 1 B/day, 1 KiB) | [plan], growth conditional on perf tests |
| Burst | 120,000 ev/s short peak, small message; sustained peak only with sufficient sink capacity and backpressure | [plan] |
| Burst duration | 10 min used in PERF-04; real burst length per source | [HYPOTHESIS] |
| Event size | ~1 KiB planning figure; wide JSON, files and documents need separate profile | [plan] / sizes [HYPOTHESIS] |
| Ingestion ack latency | p95 <= 250 ms in LAN, healthy Kafka, nominal load | [plan] target, not SLA |
| End-to-end freshness | p95 <= 5 s to analytics availability, small realtime message, nominal | [plan] target |
| Availability | 99.9 % monthly for ingestion/query API after production gate | [plan] |
| Durability | no loss of confirmed events on single-node failure: Kafka acks=all, RF=3, min ISR=2, idempotent producer | [plan] |
| Duplicates | none unaccounted in defined business results; dedup by `event_id`, `pipeline_version`, `tenant_id` | [plan] |
| Retention (Kafka) | raw 7 d, validated 3 d, processed 3 d, quarantine/DLQ 14 d, audit 7 d + S3 | [plan] §5.2 |
| Retention (ClickHouse hot) | N days hot, rest in S3 | [UNKNOWN] decide with data owners |
| Retention (S3 raw/archive) | per source/legal policy | [UNKNOWN] |
| Query concurrency | 20 / 100 / 500 clients (PERF-08); p95/p99 budgets to be documented | [plan] profile; latency budgets [UNKNOWN] |
| Max batch object | gateway default <= 100 events per request today; max file/object size | request limit [implemented]; file/object max [UNKNOWN] |
| Batch import throughput | TB/h with completion SLA depending on data; PERF-07 1+ TB backfill with bounded memory | [plan]; numeric target [UNKNOWN] |
| OCR | documents/hour SLO | [UNKNOWN] |
| RPO (node failure) | 0 for confirmed events | [plan] |
| RPO / RTO (site failure) | determined after remote backup/DR choice; RPO=0 must NOT be promised for whole-site loss | [plan]; values [UNKNOWN] |
| Cost envelope | no cluster purchase before POC/benchmark; hardware sizing from measured bottlenecks (the "22 servers" figure is not to be bought blindly) | [plan]; budget [UNKNOWN] |

## Acceptance tests
PERF-01..08, chaos/DR and security tests are defined in DEVELOPMENT_PLAN.md §11 and are the acceptance mechanism; see `performance-profile.md` for generator coverage and gaps. Metrics to collect during every run are listed in §11.

## Sign-off checklist
- [ ] Each [HYPOTHESIS]/[UNKNOWN] row has an owner and a confirmation date.
- [ ] Reviewed by Architect/Tech Lead and QA/Performance (independent review required for sizing, failover, performance and security per §14).
