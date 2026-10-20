# ADR-009: Existing AAS Auth API for OIDC/RBAC

Status: Accepted (plan v1.0, DEVELOPMENT_PLAN.md §1)

## Context
ASTRYON must not run its own IdP.

## Decision
Authenticate via the existing AAS (OAuth/OIDC, JWKS verification in `libs/go-common/pkg/authn`, env `AAS_BASE_URL`). Tenant enforcement and service authorization stay in ASTRYON; `tenant_id` only from validated claims. Contract: `../authentication.md`. Auth code from the reference implementation is never copied.

## Consequences
Dependency on AAS availability and claim contract; no second Keycloak; DPoP validation deferred.
