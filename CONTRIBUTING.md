# Contributing

Source of truth: `DEVELOPMENT_PLAN.md`; agent guidance in `AGENTS.md`; code map in `PROJECT_INDEX.md`.

## Branch strategy
- `main` is always releasable and protected: changes land via pull request only, with review and green CI.
- Branch from `main`: `feat/<DPP-id>-<slug>`, `fix/<slug>`, `docs/<slug>`, `chore/<slug>`. Keep branches short-lived.
- Squash-merge with a message starting with the task id, e.g. `DPP-008: idempotent sink`.
- Releases are tagged from `main` (`vMAJOR.MINOR.PATCH`); hotfixes branch from the tag and merge back to `main`.
- Ownership: scope a change to one subtree; do not edit other owners' subtrees without agreement.

## Local workflow
`make dev-up`, `make topics`, `make migrate`, `make test` (see README). `make test` must pass before a PR is opened. Go 1.22 workspace; one module per service/lib. No secrets in the repo; configuration by environment variables only.

## Rules that reviewers check
- `tenant_id` only from validated token claims, never headers/body.
- No per-event status in PostgreSQL; migrations are forward-only with a rollback note.
- Tests live next to the code.
- After adding/removing a top-level module, update `PROJECT_INDEX.md`.
- Architecture changes need an ADR in `docs/architecture/decisions/`.

## Definition of Done (DEVELOPMENT_PLAN.md §15)
- [ ] Code in the repository, reviewed; build and lint pass.
- [ ] Unit and integration tests in CI; contract tests where an interaction changes.
- [ ] Migrations include a rollback/recovery plan if state or storage is touched.
- [ ] OpenAPI / event schemas current; version changed and compatibility checked.
- [ ] Metrics, logs, traces, alerts and health endpoints added.
- [ ] Tenant isolation and authorization covered by security tests.
- [ ] Timeouts, cancellation, backpressure and idempotent retry supported.
- [ ] Runbook added or updated.
- [ ] Sprint acceptance test passed; the release can be rolled back.

## Issues and PRs
Use the templates in `.github/`. Link the DPP task id, state acceptance criteria, and mark any figure not measured as a hypothesis.
