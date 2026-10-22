# Chaos game days (DPP-061, DPP-063)

Scope: controlled fault injection and the reconciliation that proves (or disproves) no silent loss or duplication.
Plan references: §9 Sprint 13, §13 alerts. Failure semantics: `docs/architecture/failure-model.md` (at-least-once,
identity dedup; not exactly-once).

## Tooling and safety

`scripts/chaos/chaos.sh <broker|clickhouse|postgres|s3|flink-taskmanager|all> [--execute]`

- Dry-run by default; `--execute` is required to touch anything.
- Local Docker only: refuses a non-local `DOCKER_HOST` or Docker context, and any container that is not `astryon_*`
  in compose project `astryon-dev` / `astryon-extras`. It cannot address staging or production.
- Each scenario: `ch-aggregator reconcile` baseline, SIGKILL (or TaskManager kill), hold down `CHAOS_DOWN_SECONDS`
  (default 10), restart, wait healthy, `reconcile` again. Passing = restarted and the reconcile findings are
  identical before and after. A dev stack can carry pre-existing findings; the script compares, it does not demand zero.
- Not covered by the dev stack: controller quorum loss, CH replica failover, PG primary failover (Patroni), Ceph OSD loss.
  These need the staging HA topology; the rows are TODO below.

## Production / staging game day procedure (manual, not scripted)

1. Announce window; confirm on-call and component owners (see `../../release/handover.md`, owners are TODO).
2. Take a `ch-aggregator reconcile --snapshot before.json` and note consumer lag, DLQ rate, error budget.
3. Inject one fault at a time; keep client traffic running with stable idempotency keys.
4. Verify alerts fired (§13) and the matching runbook was usable (`ingestion-failures.md`, `stuck-consumer.md`).
5. Recover, then `ch-aggregator reconcile --compare before.json`. A silent checksum change on a partition that had no
   replay is a failed drill.
6. Record results below; sign-off by the component owner (gate: all critical failover/restore runbooks executed and signed).

## Results

Dev-stack runs, 2026-10-21, single-node containers, **no client traffic during the fault** (so these show restart and
data survival, not loss-free ingestion under load). Kill = SIGKILL, down 8 s. Pre-existing reconcile findings on this
dev data (6 unregistered batch/version pairs, 3 aggregate drifts) were identical before and after every scenario.

| Scenario | Target | Result | Recovery time | CH rows before/after | Reconcile before -> after |
| --- | --- | --- | --- | --- | --- |
| Kill broker | `astryon_kafka` | recovered, healthy | 20 s | 1366 / 1366 | exit 2 -> 2, findings identical |
| Kill ClickHouse | `astryon_clickhouse` | recovered, healthy | 6 s | 1366 / 1366 | exit 2 -> 2, identical |
| Kill PostgreSQL | `astryon_postgres` | recovered, healthy | 6 s | 1366 / 1366 | exit 2 -> 2, identical |
| Kill S3 (s3mock) | `astryon_s3mock` | recovered, healthy | 6 s | 1366 / 1366 | exit 2 -> 2, identical |
| Kill Flink TaskManager | `astryon_processing` | new TaskManager id, job RUNNING | 6 s (restarted by script via `taskmanager.sh start`; no supervisor does this in the dev image) | 1366 / 1366 | exit 2 -> 2, identical (re-run 2026-10-21 after fixing recovery detection) |

Unrun / TODO (need staging HA):

| Scenario | Status |
| --- | --- |
| Kill Kafka controller (quorum loss) | unrun, TODO |
| Kill ClickHouse replica with replicated tables | unrun, TODO |
| Kill PostgreSQL primary, Patroni failover | unrun, TODO |
| Kill Ceph OSD | unrun, TODO |
| Fault under sustained load with idempotent producers (DPP-063 checkpoint+replay correctness with reconcile) | unrun, TODO. Needs a load generator plus a Flink checkpoint/restore cycle; `reconcile --snapshot/--compare` is the verification tool. |
| Sign-off | TODO (names, date) |

## Measured values

| Item | Value |
| --- | --- |
| Observed dev restart times | see table above (not an RTO) |
| RTO per component | TODO (measure on staging) |
| RPO per component | TODO (measure on staging) |
