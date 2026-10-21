# DPoP (L2) in libs/go-common/auth: core implemented, Redis store and live test pending

Status 2026-10-21: `auth/dpop.go` implements items 1-5 (scheme handling, downgrade protection, proof validation, `ReplayStore` + `MemoryReplayStore`,
`Principal.Confirmation`, `Config.RequireDPoP/PublicBaseURL/ReplayStore`); unit tests in `auth/dpop_test.go`. Done too: `RedisReplayStore` (standalone redis[s]://, CA file, fail closed; no Cluster/Sentinel).
Also done: `Config.OnDPoPReject(reason)` hook (services attach counters) and `WWW-Authenticate: DPoP algs="ES256"` challenge; query-api wired (`AAS_PUBLIC_BASE_URL`, `AAS_REDIS_URL`, `AAS_REQUIRE_DPOP`).
Live-proven on real Redis 7 (2026-10-21, `AUTH_IT_REDIS_URL`): replay seen across two store instances, key expires after TTL.
query-api counts rejects (`QUERY_METRICS_ADDR`, `astryon_query_api_dpop_rejected_total{reason}`).
Still open: counters in other services, wiring in gateway/quarantine, live test with a DPoP-bound AAS client. The text below is the original plan.

Status 2026-10-20: investigation only. `Verifier` rejects `typ=DPoP` tokens and `Authorization: DPoP ...` by design (fail closed).
Sources: `docs/architecture/authentication.md`, real Keycloak 26.8.1 behaviour recorded by the AAS owners (read-only reference
`aas-auth/docs/discrepancies.md` R11-R13, `dpop_verify.py`; nothing copied).

## What AAS issues (observed, not assumed)

- A DPoP-bound access token has payload `typ: "DPoP"` (not `Bearer`) and `cnf: {"jkt": "<RFC 7638 thumbprint>", "kc-jkt-type": "DPoP"}`.
  Unbound tokens keep `typ: "Bearer"`. `typ: DPoP` without `cnf.jkt` must be rejected.
- Keycloak 26.8.1 has no server DPoP nonce: `/token` accepts proofs without `nonce` and returns no `DPoP-Nonce`.
  A resource server therefore must not require a nonce; support `nonce` only as an opt-in for future AAS versions.
- At the token endpoint Keycloak compares the proof `htu` with its own `.../protocol/openid-connect/token` URL (client side concern).
  For resource servers `htu` is the URL of the ASTRYON endpoint as the client sees it (public URL behind the reverse proxy).

## What libs/go-common must add

1. Scheme handling: accept `Authorization: DPoP <token>` plus exactly one `DPoP: <proof>` header; keep `Bearer` for unbound tokens.
   A `Bearer` scheme carrying a token with `cnf.jkt` (or `typ=DPoP`) must be rejected (downgrade protection); a `DPoP`
   scheme with an unbound token must be rejected.
2. `Config` additions: `PublicBaseURL` (to rebuild `htu`; never trust `Host`/`X-Forwarded-*` from the client),
   `ProofMaxAge` (60 s), `ProofFutureSkew` (5 s), `ReplayTTL` (120 s), `RequireDPoP` per resource (L2 default per the docs), `ReplayStore`.
3. Proof validation (RFC 9449 section 4.3): compact JWS `typ=dpop+jwt`; `alg` allowlist ES256 (add EdDSA only if the Go stdlib
   path is accepted: `crypto/ed25519` is stdlib); header `jwk` is public (no `d`/`k`) and is the verification key;
   `htm` equals the request method; `htu` equals the normalized request URL (scheme/host lowercase, default port dropped, no query/fragment);
   `iat` within [now-60 s, now+5 s]; `ath` = base64url(SHA-256(access token)); `jti` present; RFC 7638 thumbprint of the header `jwk`
   equals the token `cnf.jkt` (compare in constant time). Order: verify the access token first, then the proof.
4. Replay: `jti` stored with `SET key 1 NX EX 120` and rejected if it already exists. Key = prefix + SHA-256(jti)
   (bounded key size, no attacker-chosen keys), ideally scoped by `jkt`. Replay-store error = reject (fail closed, matching the docs).
5. `ReplayStore` interface with `MemoryReplayStore` (single process / tests, bounded) and `RedisReplayStore` fed by `AAS_REDIS_URL`.
6. Metrics (names from the docs): DPoP replay, htu/htm/ath/jkt mismatch counters; no tokens or proofs in logs.
7. `Principal` gains `Confirmation` (jkt) so handlers can see the token is sender-constrained; `WWW-Authenticate: DPoP error="invalid_token", algs="ES256"`
   on failure, and `use_dpop_nonce` plumbing only when nonce is enabled.

## What is missing today

- No Redis client in `libs/go-common` (stdlib only, no third-party module). `services/query-api/internal/query/redis*.go` already speaks RESP
  (TLS, Cluster, Sentinel) for limits; either lift that into a small shared package (separate task, other owner) or add a minimal
  `SET NX EX` client to go-common. `AAS_REDIS_URL` must support `rediss://` + a CA file like `QUERY_REDIS_URL`.
- `Verifier.principal` rejects `typ != Bearer|at+jwt` and ignores `cnf`; both need to become aware of the binding.
- A live DPoP test: harness clients with `dpop.bound.access.tokens=true`, a proof generator (test code), `typ=DPoP` token parse,
  replay across two verifier instances sharing Redis, wrong htu/htm/ath/key, and a throwaway Redis container (`chagg-*` naming).
- Decision needed: public URL source for `htu` per service (config vs. gateway-injected), and whether internal services re-validate
  proofs or trust a gateway-token + `X-AAS-Subject` chain (docs: gateway-to-service uses token exchange, so internal hops are Bearer).
- Token exchange / `X-AAS-Subject` is also unimplemented and independent of DPoP.
