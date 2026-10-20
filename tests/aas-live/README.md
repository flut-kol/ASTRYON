# tests/aas-live

Runs `libs/go-common/auth.Verifier` (and the `services/query-api` adapter) against a REAL AAS
(Keycloak, realm `aas`). Skipped unless `AAS_LIVE_BASE_URL` is set, so `make test` is unaffected.

## Run

Start an AAS you own (the reference deployment is not part of this repo; never copy it here) and set:

| Variable | Meaning |
|---|---|
| `AAS_LIVE_BASE_URL` | realm URL, e.g. `https://localhost:18443/realms/aas` (required) |
| `AAS_LIVE_ADMIN_USER`, `AAS_LIVE_ADMIN_PASSWORD` | Keycloak admin; used only to create/delete throw-away clients, scopes and a signing key |
| `AAS_LIVE_CA_FILE` | CA bundle if the AAS cert is not system-trusted |
| `AAS_LIVE_AUDIENCE` | default `https://api.aggregator.example` (the `gateway` scope audience) |
| `AAS_LIVE_ENV` | default `dev` (`env` claim of the realm) |
| `AAS_LIVE_KEYCLOAK_CONTAINER` | docker container name; enables the stop/start outage test |

`cd tests/aas-live && go test -count=1 -v ./...`. Clients authenticate with `private_key_jwt` (ES256)
using keys generated in the test; no secret is stored in the repo. Everything created is removed in cleanup.

## Proven (Keycloak 26.8.1, dev realm config)

- Valid token accepted; `tenant_id`, `scope`, `client_id`, `sub`, `jti` mapped; unrequested scope absent.
- Rejected: wrong audience, wrong env, disallowed azp, pinned `at+jwt` typ, wrong issuer pin, expired
  (clock and a real 3 s token), tampered signature, tampered payload, `alg=none`, HS256 confusion,
  non-UUID tenant.
- JWKS rotation: new realm key, new `kid` verified with a 1 h cache TTL via one refetch; old key still valid
  during overlap; unknown/forged kid rejected and rate limited; removed key not trusted by a fresh verifier.
- Outage: Keycloak container stopped; cached/stale key keeps verifying unexpired tokens, unknown kid and
  expired tokens rejected, verifier recovers after restart without being recreated.
- query-api built and run: 401 without token, 403 without `astryon:query:read`, 200 with it; tenant reaching the
  ClickHouse stub equals the claim even when a conflicting `X-Tenant-ID` header is sent.

## Not proven

DPoP-bound tokens (`typ=DPoP`, unsupported by the verifier, rejected by design), introspection/revocation (L3),
token exchange and `X-AAS-Subject`, the AAS proxy (`/oauth2/*` addresses), production deployment
behind a reverse proxy path (`/sso`), browser/user tokens, other adapters (gateway, export-worker,
replay-controller) end to end, RS256 realm keys, ClickHouse itself (stubbed).
