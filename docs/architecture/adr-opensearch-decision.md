# ADR: OpenSearch (DPP-048)

Status: proposed, 2026-10-21. Decision owner: TODO-owner. Relates to ADR-008 (OpenSearch optional), ADR-011 (at-least-once, idempotent
sinks), plan §7 ("OpenSearch only for genuinely required full-text"), §17 ("do not introduce OpenSearch and Iceberg without confirmed
use cases").

## Decision

**Do not introduce OpenSearch in v1. Defer DPP-048 (sink, index rollover, retention) until one of the trigger conditions below is met.**
ClickHouse stays the only query store. This is a recommendation from repository evidence; it needs the owner's sign-off.

## Evidence

1. **No full-text requirement exists in the repo.** A search of code, docs and OpenAPI for OpenSearch / full-text finds only the plan.
   No user story, tenant request or acceptance row asks for free-text search over payloads.
2. **The implemented query surface is structured.** `services/query-api` serves `POST /v1/query/events` (tenant from token, `from`/`to`,
   `source_id`, `event_type`, `processing_version`, keyset cursor), `/v1/query/aggregate` (whitelisted metrics, at most 3 `group_by`),
   `/v1/metrics/summary`, `/v1/quality/summary`. All map to the `events_normalized` ORDER BY (tenant, source, day, event_id) and the
   0003 aggregates. Arbitrary SQL and free text are not exposed (plan §8 standard).
3. **Benchmarks show no search-shaped bottleneck** (`docs/benchmarks/dpp-047-2026-10-21.md`, single node, 10M rows, synthetic):
   - tail tenants: p95 <= 118 ms at 16 clients in every distribution;
   - a 2.7M-8M row tenant costs 0.7-2.1 s per aggregate, but the measurement attributes it to **exact dedup over millions of keys**
     (`LIMIT 1 BY` 2109 ms vs `uniqExact` 1019 ms vs plain `count()` 51 ms), not to scanning or filtering. A search engine does not
     remove that cost; it would have to repeat the same dedup problem (ADR-011);
   - the real limit is concurrency x memory cap on a hot tenant (16 queries x 2 GB exceeded a 6 GB node).
   Remedies already identified are inside ClickHouse: pre-aggregated 0003 tables (~50 ms class), lower per-tenant concurrency.
4. **Ingest benchmark is the bigger gap**: ACK p95 420 ms vs 250 ms target and 45k vs 120k ev/s (`dpp-010`). A second sink consuming
   `processed.events.v1` adds load and failure modes while the first sink path is still unproven under HA/chaos.
5. **Extra cost the repo would have to carry**: a second idempotent sink and its own crash/dedup tests (the ClickHouse sink needed
   token-based dedup, intent table and a fault-injection suite); tenant isolation re-implemented in the index (document-level or per-tenant
   index, plus tests: plan §11 security); reindex path for replay / `processing_version`; index rollover and retention aligned with
   ClickHouse TTL (400 d default); reconciliation counts per source (`ch-aggregator reconcile` covers only ClickHouse); Kubernetes
   operations, backups, dashboards, alerts, runbooks (none exist); capacity not modelled (see `docs/release/post-rollout.md`).

## Options considered

| Option | Verdict |
|---|---|
| A. No OpenSearch, ClickHouse only (recommended) | Meets every implemented query; zero new components. |
| B. ClickHouse skip indexes for substring or token search on selected `attributes` fields | Try first if a search need appears; **not measured here**, test on representative data (TODO-owner). |
| C. OpenSearch with sink from `processed.events.v1` | Only after a trigger; scope to the fields that need search, not full payloads. |
| D. S3 + external search tool | No evidence of need; skip. |

## Trigger conditions to revisit (any one is enough to open DPP-048 again)

1. **Demand:** a named user group (TODO-owner) with an accepted requirement for free-text, fuzzy or relevance-ranked search over event
   fields, with example queries and the expected result latency.
2. **Evidence that ClickHouse cannot serve it:** option B prototyped on representative data and the query p95 for the requested search
   exceeds the agreed target (target value TODO-owner; for reference the current alert guardrail for query p95 is 2.5 s) at the agreed
   concurrency (PERF-08: 20/100/500), or scans exceed `QUERY_MAX_MEMORY_BYTES` / `QUERY_MAX_EXECUTION_SECONDS`.
3. **Volume bound:** the searchable subset is defined (fields, retention, daily volume from the cost model) and its index size and cost
   fit the cost envelope (AP-18, TODO-owner).
4. **Preconditions satisfied:** ClickHouse sink path has passed HA/chaos (`operational-acceptance.md`), and an owner and on-call exist
   for the new component.

## Consequences if accepted

- `DEVELOPMENT_PLAN.md` ADR-008 stays "optional"; the DPP-048 gate item is recorded as "not justified, deferred" in the final report.
- Query API keeps the whitelisted filter model; any search endpoint added later needs its own OpenAPI, scopes (`scripts/check-scopes.sh`),
  tenant-claim enforcement and error codes.
- Revisit date: after the first rollout stage with real traffic (`canary-plan.md`) and the DPP-071 capacity review.
