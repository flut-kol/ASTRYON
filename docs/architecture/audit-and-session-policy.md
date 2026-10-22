# Audit, security events, session policy and 2FA (DPP-053, DPP-045)

Library: `libs/audit` (Go). Storage: existing `audit_log` (migration 0007, extended by `0015_audit_security_events`).
No new table: `audit_log` already has tenant RLS (FORCE) and an append-only trigger (UPDATE/DELETE raise).

## Record

| Field | Column | Source |
|---|---|---|
| tenant | `tenant_id` | validated token claims only (`audit.Claims`), never header/body/query |
| actor | `actor_aas_sub`, `actor_client_id` | `sub`, `client_id` claims |
| action | `action` | `noun.verb`, lower case, e.g. `source.create`, `export.create`, `auth.denied`, `session.logout` |
| resource | `entity_type`, `entity_id` (UUID) or `resource_ref` (any other short id) | handler |
| outcome | `outcome` | `success` / `denied` / `failure` |
| category | `category` | `audit` (data/state actions) / `security` (authn, authz, session, 2FA) |
| trace | `trace_id`, `request_id` | W3C trace id, request id |
| 2FA evidence | `auth_acr` | `acr` claim of the session |
| metadata | `details` jsonb | <= 16 keys, values <= 256 chars, <= 8 KiB |

Forbidden: tokens, secrets, cookies, passwords, request/response payloads, PII (emails, names, phones). `Event.Normalize`
rejects detail keys matching secret/token/payload/PII names; callers still must not put such values under innocuous keys.
Migrations are forward-only; prod `audit_log` is retained (compliance), no rollback by DROP.

## Writer / reader

- `audit.NewWriter(pool, cfg)`: bounded queue (default 1024), batch per tenant in one transaction with
  `set_config('astryon.tenant_id', ..., true)` so the RLS `WITH CHECK` applies; `ON CONFLICT (id) DO NOTHING` makes retries idempotent.
- `Submit` never blocks: returns `ErrQueueFull` (counted, `OnDrop` callback). Use it for ordinary audit.
- `WriteSync` writes in the caller's goroutine: use it where the plan requires an audit record before the change
  (required-audit mode, e.g. replay activate, export create), failing the request with 503 if it errors.
- `Close(ctx)` drains on shutdown. `Stats()` gives queued/written/dropped/rejected for metrics (`astryon_audit_*`).
- `audit.List(ctx, pool, tenantFromClaims, Filter)`: keyset pagination newest first (limit <= 500), RLS + explicit tenant predicate.
  Exposure through an API needs a scope (see roles.md: `auditor` read-only); none is defined yet.
- The DB role must be neither superuser nor BYPASSRLS; `astryon.platform_admin` must not be set by this library.

## Integration points (wired opt-in in query-api and export-worker; other services not yet)

| Service | Events |
|---|---|
| gateway, all resource servers | `auth.denied` (401/403, security), `auth.token_invalid`; tenant mismatch attempts |
| admin-bff | `session.login`, `session.logout`, `session.backchannel_logout`, `session.expired`, `session.step_up_required` (security) |
| connector-manager | `source.create/update/pause/resume`, `connector.*`, credential reference changes (reference only, never the secret) |
| export-worker | `export.create`, `export.download_link` |
| replay-controller, quarantine | replay/quarantine publish: today two-phase to `audit.events.v1`; may additionally mirror into `audit_log` via the writer |
| file-ingestion | `upload.create/complete/cancel` |

Map `auth.Principal` to `audit.Claims{TenantID, Subject, ClientID, ACR}`. `Principal` has no `acr` yet: add it to the token
verifier (go-common owner) before relying on `auth_acr`.
Kafka topic `audit.events.v1` (plan 5.2) remains the machine stream; `audit_log` is the queryable tenant-scoped store.

## Session policy and 2FA (enforced by the IdP/AAS, not by ASTRYON)

ASTRYON has no IdP and stores no passwords or TOTP seeds. Policy:

- 2FA/MFA is mandatory for human users and is enforced by AAS at login (Authorization Code + PKCE). ASTRYON verifies
  evidence, it does not perform the second factor.
- Evidence: the validated ID/access token must carry `acr` (authentication context class reference, an MFA class
  configured in AAS) and/or `amr` containing a second factor (e.g. `otp`, `hwk`, `mfa`). admin-bff rejects a login
  whose `acr`/`amr` does not satisfy the configured minimum (`BFF_REQUIRED_ACR`, proposed) and records `session.login`
  outcome `denied` as a security event. Exact `acr` values must be taken from the AAS contract (not confirmed here).
- Step-up: privileged operations (platform-admin actions, replay activate, export of large scope) may require a recent
  authentication: check `auth_time` / `max_age` and re-authorize at AAS with `acr_values` + `max_age`; do not implement a local challenge.
- Service accounts use `client_credentials` + `private_key_jwt` (DPoP for L2); no 2FA applies, their `acr` is empty.
- Session lifetime (idle and absolute), concurrent-session limits and revocation are AAS-configured. ASTRYON side: BFF
  session cookie is `Secure; HttpOnly; SameSite`, server-side session in Redis with TTL not exceeding the token/session
  lifetime, back-channel logout deletes the session, access tokens short-lived. Proposed defaults to agree with AAS:
  idle 30 min, absolute 12 h; they are not set in this repo.
- Every login, logout, back-channel logout, expiry, denial and step-up is a `security` event in `audit_log`.

## Verification status

`go test -race` passes in `libs/audit`; the live test (`AUDIT_IT_PG_URL`) was run against PostgreSQL 16 with
migrations 0001-0015 (non-superuser role): round trip, tenant isolation, keyset pagination, append-only. Not verified:
the service-to-PostgreSQL path (query-api and export-worker are wired, tested with fake writers only), other services, real AAS `acr`/`amr` values, load behaviour of the writer.
