# Canary rollout plan 1% -> 10% -> 50% -> 100% (DPP-067, DPP-068)

Status: DRAFT, not rehearsed. `readiness-review.md` verdict is NO-GO; this plan does not change it. It defines the stages and
the conditions up front so a promotion is a check, not a judgement call. Thresholds reuse alert rules in
`infra/observability/prometheus-rules.yaml` (initial guardrails, never fired against real series) and plan targets (§2).
Anything the repo cannot supply is `TODO-owner`.

## 1. What "x%" means

The code has no weighted routing and no per-tenant flag (`feature-flags.md`). A stage is defined as a **share of the real
production event stream moved to the new stack**, selected by cohort of tenants/sources:

- Cohort selection and routing (DNS/LB weight, producer config, or tenant allow-list at the source side): TODO-owner. Until a
  mechanism is chosen, "1%" cannot be executed.
- Cohorts are chosen by tenant, never by random request sampling, so each event keeps one `event_id` path and dedup holds
  (`event_id` must not change on retry, plan §5.3).
- First cohort: small, low-risk tenants with a named contact. Do not include the largest tenant before 50%
  (a single large tenant costs seconds per aggregate and can exhaust query memory: `docs/benchmarks/dpp-047-2026-10-21.md`).
- Mixed traffic of old and new stack must write distinct data or the same idempotent keys; the dual-run read comparison is
  done with `ch-aggregator reconcile`. Old-stack details: TODO-owner (the repo has no earlier production system).

## 2. Entry gate (before stage 1)

All must be true, otherwise stop:
1. `docs/release/operational-acceptance.md` items required for stage entry checked with evidence (currently all unchecked).
2. Production candidate verdict in `readiness-review.md` is not NO-GO. Owner: TODO-owner.
3. Roll-back path rehearsed once in stage environment: image rollback, switch rollback, version rollback (`docs/operations/release-rollback.md`). Not done.
4. Dev-only switches absent (`feature-flags.md` section 2); migrations pass `scripts/check-migrations.sh`; `scripts/check-scopes.sh` passes.
5. Alert routes and on-call exist (`incident-management.md`; owners TODO-owner), runbooks present for ingestion failures, stuck consumer, replay.
6. Baseline captured for the cohort before the switch: ACK p95, query p95, Kafka lag, quarantine ratio, per-day event counts per source.

## 3. Stages

| Stage | Traffic share | Minimum soak | Cohort |
|---|---|---|---|
| 1 | 1% | 24 h and at least one daily peak (hold longer if the cohort has weekly cycle: TODO-owner) | smallest tenants |
| 2 | 10% | 48 h | add mid-size tenants |
| 3 | 50% | 72 h, includes one full week boundary if possible | add large tenants one by one, hot tenant last |
| 4 | 100% | 7 d before the old path is retired | all |

Soak lengths are proposals (not derived from data): TODO-owner to confirm.

## 4. Promotion conditions (all must hold for the whole soak window)

| # | Condition | Measure (existing metric/alert) | Stage limit |
|---|---|---|---|
| P1 | No lost confirmed events | accepted count (gateway) = Kafka end offsets = ClickHouse unique rows per source/day; `ch-aggregator reconcile` findings = 0 (`AstryonReconcileDrift` not firing) | 0 tolerance, all stages |
| P2 | Ingest availability | `astryon:gateway_ack_error_ratio:5m` <= 1% (alert `AstryonGatewayAckErrors`), no `AstryonGatewayAckErrorsFast` | all stages |
| P3 | ACK latency | `AstryonGatewayAckLatencyFastBurn` / `SlowBurn` not firing; p95 ACK <= 250 ms is the target. Measured so far: 420 ms at benchmark peak, so P3 cannot pass until the target is met or re-agreed (AP-05 in `acceptance-profile.md`) | all stages |
| P4 | Pipeline keeps up | `AstryonKafkaConsumerLagGrowing` (> 100k for 15 m) and `AstryonSinkStalled` not firing; lag returns to baseline after peaks | all stages |
| P5 | Sink health | `AstryonSinkInsertFailures`, `AstryonSinkCommitFailures` not firing | all stages |
| P6 | Data quality | quarantine ratio (`astryon:sink_quarantine_ratio:15m`) below 1% (`AstryonSinkQuarantineRising`) and not above the cohort's pre-switch baseline + TODO-owner margin | all stages |
| P7 | Query health | `AstryonQueryBackendErrors` (> 2%) and `AstryonQueryLatencyP95High` (> 2.5 s) not firing; no `AstryonQueryThrottling` caused by a single tenant | from stage 2 |
| P8 | Heavy tenant isolation | with the hot tenant active, tail-tenant query p95 stays within the pre-switch baseline and no ClickHouse memory-limit errors or OOM | before stage 3 (PERF-06, dpp-047 limits) |
| P9 | Dedup and replay | one replay dry-run and one rollback of an active version executed successfully on the cohort | before stage 3 |
| P10 | Security | no cross-tenant read in audit/test results; no `AstryonQueryDPoPReplay`; certificate expiry alert clear | all stages |
| P11 | Ops | no SEV1/SEV2 open, error budget not spent above 50% (`incident-management.md`) | all stages |
| P12 | Storage | `AstryonClickHouseDiskHigh` not firing; projected disk at 100% share fits (use `post-rollout.md` cost model inputs) | before stage 3 |
| P13 | Sign-off | named release owner records stage, window, metric screenshots/queries, decision | all stages |

## 5. Automatic hold and rollback conditions

**Hold** (stop promotion, keep share, investigate): any warning alert of P3-P7 firing, lag not draining after a peak, quarantine ratio
above baseline margin, any unexplained reconcile finding. A hold longer than 24 h at one stage needs the release owner's written decision.

**Rollback** (return to previous share and cohort; do first, investigate after) when any of:
- R1 confirmed data loss or reconcile finding that cannot be explained in 1 h (SEV1);
- R2 `AstryonGatewayAckErrors` or `AstryonGatewayAckErrorsFast` firing (> 1% for 10 min, or > 5% for 3 min);
- R3 `AstryonSinkStalled` or lag growing with no recovery for 30 min;
- R4 any cross-tenant data exposure or auth bypass (SEV1; also revoke affected credentials, `runbooks/secret-rotation.md`);
- R5 `AstryonQueryBackendErrors` firing for 30 min or ClickHouse OOM/restart under query load;
- R6 a bad deploy: crash loops, readiness failing, migration failure.
Rollback timing target: TODO-owner (not measured; no rollback has been run).

## 6. Rollback actions (from `docs/operations/release-rollback.md`)

| Failure | Action |
|---|---|
| Bad service image | previous image digest via Helm/Argo; stateless, consumers idempotent |
| Misbehaving optional behaviour | revert the env switch from `feature-flags.md` and restart |
| Wrong processing version active | `POST /v1/replays/{id}/activate?rollback=true` (audited) |
| Cohort problems | return the cohort's traffic to the previous route (mechanism TODO-owner); events already accepted stay and are deduplicated by `event_id` |
| Schema | do not roll back the schema; ship a forward migration; code must work with N and N-1 |
| TTL / retention | change TTL forward; expired rows are not recoverable |

## 7. After each stage

Record: window, metric values against P1-P13, incidents, switch changes, decision, next owner. Keep this next to the release notes.
Open: where the record lives and who signs (TODO-owner).
