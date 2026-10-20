# ASTRYON — agent guidance

Distributed data platform. Source of truth: `DEVELOPMENT_PLAN.md` (sections referenced as §N). Auth: `docs/architecture/authentication.md` (external AAS, no own IdP).

## Index-first
1. Read `PROJECT_INDEX.md` (this dir) before searching. Read `DEVELOPMENT_PLAN.md` only by section (`grep -n '^## ' `, then `sed -n`).
2. Scope work to one subtree (see ownership below). Do not edit other agents' subtrees.
3. After adding/removing top-level modules, update `PROJECT_INDEX.md`.

## Rules
- Go 1.22, Go workspace (`go.work`); one module per service/lib. No secrets in repo; env vars only.
- tenant_id comes only from validated token claims, never headers/body.
- No per-event status in PostgreSQL. Migrations forward-only + rollback note.
- Tests next to code; `make test` must pass before reporting done.
