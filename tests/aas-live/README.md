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
| `AAS_LIVE_USER`, `AAS_LIVE_USER_PASSWORD`, `AAS_LIVE_USER_TOTP_SECRET` | a user with password+TOTP (AAS dev realm: `e2e.user`, `E2E_*` in its `.env`); enables the user-token and token-exchange tests. Needs `AAS_ENV=dev` realm config (`demo-sso`, `e2e.user`) and `scripts/e2e_user.py --post` (clears CONFIGURE_TOTP). Keycloak accepts each TOTP counter once; the last used one is kept in `$TMPDIR/aas-live-totp-last` |
| `AAS_LIVE_PROXY_URL` | AAS standard-address proxy, e.g. `https://localhost:18444`; enables `TestLiveProxy` |

`cd tests/aas-live && go test -count=1 -v ./...`. Clients authenticate with `private_key_jwt` (ES256)
using keys generated in the test; no secret is stored in the repo. Everything created is removed in cleanup (a scope that the realm already defines, e.g. `astryon:events:write`, is reused and left alone).

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

## Tenant isolation through the generated client, `tenant_isolation_test.go`

Two throw-away clients with different `tenant_id` (A, B) call query-api through `libs/sdk-query` with real AAS tokens. Proven
(Keycloak 26.8.1, 2026-10-21): the tenant reaching the (stubbed) ClickHouse is always the token's own, also when token B sends
`X-Tenant-ID=A` and token A sends `X-Tenant-ID=B`. Stored-data isolation in a real ClickHouse is covered separately by `services/query-api` `TestIntegrationTenantIsolationRealClickHouse` (`QUERY_CH_IT_URL`), without AAS.

## Introspection and revocation (L3), `introspect_test.go`

`auth.Introspector` (`libs/go-common/auth/introspect.go`) against the real AAS, `private_key_jwt` ES256, endpoints from discovery
(`introspection_endpoint`, `revocation_endpoint`; on the Keycloak deployment these are `.../protocol/openid-connect/token/introspect` and `/revoke`).
Proven: active token -> `active=true` with the same `tenant_id`, `sub`, `client_id`, `jti`, `scope` as the verified principal;
after `/revoke` the token still passes local signature/exp verification but introspection gives `active=false` and `RequireActive` answers 401;
a 3 s token is `active=false` after expiry; garbage token -> `active=false`; a client that is not in the token audience gets `active=false`.
Finding vs docs: Keycloak 26.8.1 returns `active=false` (not an error) to a client that is not in the token `aud`, even for the
client that was issued the token, so an L3 resource server must be an audience of the tokens it introspects (the reference gateway's
client id is the audience URI; the test adds an audience mapper to its throw-away client). The docs do not state this requirement.
Revocation only works for tokens of the authenticating client. Caveat: positive results are cached (default 10 s), so revocation is seen
within that window; negative results 30 s.

Local procedure used (reference deployment copied to a temp dir, never into this repo): copy it to `/private/tmp/claude-501/aas-run2`
without `.venv`/`.git`; in the copy change the compose host ports to `127.0.0.1:18443`/`19443`, set `.env` `KC_HOSTNAME=https://localhost:18443`
and `KC_FRONTEND_URL=https://localhost:18443`; `docker compose -p chagg-aasi up -d postgres keycloak`, wait for `https://localhost:19443/health/ready`,
`docker compose -p chagg-aasi --profile config run --rm keycloak-config` (re-run after changing `KC_FRONTEND_URL`, the realm stores it);
then set the `AAS_LIVE_*` variables with `AAS_LIVE_CA_FILE=<copy>/certs/ca.crt`. Tear down with `docker compose -p chagg-aasi down -v` and remove the copy.

## Other adapters, user tokens, exchange, proxy (2026-10-21, Keycloak 26.8.1, throw-away copy of the astranex AAS)

- `adapters_test.go`: `services/gateway` (in-memory publisher), `services/export-worker` and `services/replay-controller` (ClickHouse stubbed) built and run with
  real tokens: 401 without token, 403 without the scope (export: write-only token on GET and read-only on POST are 403), 2xx with it;
  gateway answers `TENANT_MISMATCH` 403 for a conflicting body `tenant_id`; `X-Tenant-ID` ignored.
- `user_test.go` `TestLiveUserTokens`: browser flow without a browser (authorization code + PKCE S256, password then TOTP, public client `demo-sso`).
  ID token `amr=[pwd otp]`; the user access token has no `aud` and no `tenant_id`, so the resource-server Verifier refuses it (fail closed);
  ID token refused; code single use; wrong `code_verifier` refused.
- `TestLiveTokenExchange`: standard token exchange of a user token to `https://api.astryon.sv4.one` keeps `sub`, adds the audience, still has no tenant
  (Verifier refuses it); refused for a client without `standard.token.exchange.enabled`, for an unknown audience, for a token issued to another client,
  for a garbage token. The audience must come from the client's scopes (the resource scope's audience mapper), otherwise `Requested audience not available`.
  Р5 (exp of the exchanged token not capped by the source) is logged as `KNOWN GAP` when it shows.
- `TestLiveProxy`: `/.well-known/oauth-authorization-server` lists `/oauth2/*`; `/oauth2/jwks` equals the realm JWKS; `client_credentials` through `/oauth2/token`
  gives a token the Verifier accepts; replayed `jti` and `exp-iat>60` and missing `iat` are 401; `resource` other than the gateway is 400; `/oauth2/introspect` reachable.
- Finding: `build/realm-config` of the reference copy is generated for `AAS_ENV=prod` (no `demo-sso`/`e2e.user`); run `AAS_ENV=dev python3 scripts/build_config.py` in the copy and re-run `keycloak-config`.

## Not proven

`X-AAS-Subject` (signed subject header), exchange of machine tokens and `act` delegation (Р4), DPoP-bound tokens through the proxy, production deployment
behind the `/sso` reverse-proxy path (the copy runs at the root path), RS256 realm keys, ClickHouse/PostgreSQL/Kafka/S3 behind the adapters (stubbed or in-memory),
file-ingestion and quarantine adapters, mTLS clients.

## DPoP (L2), `dpop_test.go`

`TestLiveDPoP` (written 2026-10-21; passed 2026-10-21 against a throw-away copy of the astranex AAS, Keycloak 26.8.1): throw-away client with `dpop.bound.access.tokens=true`, token request with a
proof (`htu` = token endpoint), expects `typ=DPoP` + `cnf.jkt`; then `Verifier` with `PublicBaseURL`/`MemoryReplayStore` must accept the token with a matching proof
and reject replay, other key, wrong htu/htm/ath, Bearer downgrade, missing proof and `RequireDPoP` with Bearer. Same env as above.
