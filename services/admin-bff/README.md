# admin-bff

Backend-for-frontend for the Admin UI (`docs/architecture/authentication.md`, flow "Admin UI"). AAS is the only IdP.

- `GET /auth/login` -> redirect to AAS (Authorization Code, PKCE S256, `state`, `nonce`; pending state is single use, bound to a short-lived login cookie).
- `GET /auth/callback` -> checks state cookie, exchanges the code (client auth: `private_key_jwt` ES256 via `BFF_CLIENT_KEY_FILE`, or `BFF_CLIENT_SECRET` for dev only), checks the ID token (issuer, audience, expiry, nonce; received straight from the token endpoint over TLS), creates a server-side session. Only an opaque id reaches the browser: `__Host-astryon_session`, `HttpOnly; Secure; SameSite=Lax`.
- `GET /auth/me` -> `{authenticated, subject, session_expires_at}`; never returns tokens.
- `POST /auth/logout` -> drops the session, returns `logout_url` (AAS end_session with `id_token_hint`) for the browser to follow.
- `/api/<service>/...` -> reverse proxy to `BFF_UPSTREAMS`; the browser's `Cookie` and `Authorization` are dropped and `Authorization: Bearer <access token>` is added; the token is refreshed (refresh_token grant, under a per-session lock) when it has under 30 s left, otherwise the session ends with 401 `session_expired`. The tenant is never set here: it stays a claim in the token.
- CSRF: unsafe methods need `X-Requested-With: astryon-admin` and, if sent, `Origin` equal to `BFF_PUBLIC_ORIGIN`.

Env: see `cmd/admin-bff/main.go`. Register `BFF_PUBLIC_ORIGIN + /auth/callback` as the exact redirect URI of client `astryon-portal`.

Not done: back-channel logout (needs logout_token signature verification via JWKS), DPoP-bound tokens (rejected with 502), shared session store (sessions live in process memory: run one replica or sticky routing; restart signs everyone out), proxying WebSockets. Unit tests use a fake AAS (`go test ./...`); the full login/refresh-free path (code+PKCE, password+TOTP, proxy to query-api, logout) is also verified against a real Keycloak AAS by `tests/aas-live` `TestLiveAdminBFF` (2026-10-21, throw-away registered client). Real client registration for the production `astryon-portal`, token refresh and back-channel logout against the real AAS are still unverified.
