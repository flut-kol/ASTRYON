# Onboarding a machine data source

This guide is for a system that submits events to the ASTRYON ingestion gateway.
It does not create credentials or duplicate AAS identity data.

## Ownership and prerequisites

| Owner | Provides |
|---|---|
| ASTRYON platform | gateway HTTPS URL, resource-server audience, maximum request limits and supported event schema version |
| AAS owner | one machine client, `tenant_id` signed claim, allowed `azp`, audience and minimum scope `astryon:events:write` |
| Source owner | private key in its secret manager, event mapping, retry/idempotency implementation and operational contact |

The source authenticates with AAS `client_credentials` plus `private_key_jwt`.
Private keys, access tokens, DPoP proofs and client assertions must never be
committed, sent in support tickets or stored in application logs.

## Submit events

Obtain a short-lived AAS access token for the gateway audience, then send a
single event to `POST /v1/events` or a bounded microbatch to
`POST /v1/events/batch`:

```http
Authorization: Bearer <short-lived-AAS-access-token>
Content-Type: application/json
Idempotency-Key: <stable-key-for-this-submit>
```

`tenant_id` in the request body is checked against the signed token claim and
is overwritten server-side. Do not take it from user input or configuration
controlled by an untrusted caller.

Use the envelope JSON schema at `docs/events/schemas/envelope.v1.json`. Keep
the same idempotency key when retrying an unconfirmed request. On `503
PUBLISH_FAILED`, honor `Retry-After`; on `422 ENVELOPE_INVALID`, repair the
event instead of retrying it unchanged. See `docs/errors/api-error-catalog.md`
for all client-facing error handling.

## Production handoff checklist

1. AAS contract test verifies issuer, discovery/JWKS, audience, scope, `azp`,
   `tenant_id`, token type and environment claim.
2. Source validates TLS and uses the platform-provided CA chain where needed.
3. The source enforces bounded batch/body sizes and records only `trace_id` and
   non-sensitive response metadata.
4. A retry test proves no duplicate business result when a response is lost.
5. The source owner has an alert for sustained `503` or authorization failures.

For browser/Admin UI access, use the separate BFF Authorization Code + PKCE
flow in `docs/architecture/authentication.md`; browser code must not reuse a
machine credential or persist tokens in web storage.
