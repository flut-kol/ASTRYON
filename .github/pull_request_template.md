## Summary
Task: DPP-___ . What and why:

## Scope
Subtree(s) touched (one owner):

## Definition of Done (DEVELOPMENT_PLAN.md §15)
- [ ] `make test` passes; build and lint clean
- [ ] Unit + integration tests added; contract tests if an interaction changed
- [ ] Migration has rollback/recovery note (if state/storage touched)
- [ ] OpenAPI / event schema updated and versioned
- [ ] Metrics, logs, traces, alerts, health endpoints
- [ ] Tenant isolation / authz tests; `tenant_id` from token claims only
- [ ] Timeout, cancellation, backpressure, idempotent retry
- [ ] Runbook added/updated; ADR added if architecture changed
- [ ] `PROJECT_INDEX.md` updated if a top-level module changed
- [ ] No secrets in the diff

## Test evidence
Commands run and observed results (do not state unmeasured numbers as results):

## Risks / rollback
