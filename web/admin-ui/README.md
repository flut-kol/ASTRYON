# Admin UI (DPP-030)
Vanilla TypeScript + Vite, no runtime dependencies. Hash routes: Overview, Uploads, Events, Quality, Connectors, Jobs, Sources.

## Run (against the demo stack, `scripts/dev/demo-up.sh`)
`npm install && npm run dev` -> http://127.0.0.1:15173. The Vite dev server proxies same-origin paths, so Go services need no CORS:
`/api/gateway` :18080, `/api/files` :18085, `/api/query` :18082, `/api/connectors` :8089, `/api/s3` :19200 (pre-signed part PUTs).
Override with env `PROXY_GATEWAY|PROXY_FILES|PROXY_QUERY|PROXY_CONNECTORS|PROXY_EXPORTS|PROXY_QUARANTINE|PROXY_REPLAYS|PROXY_S3`. `npm run typecheck`, `npm test`, `npm run build`.

## Auth
Dev: `scripts/dev/demo-up.sh` generates a random token once (`openssl rand -hex 24`, file `$DEMO_DIR/dev-token`, mode 0600, reused on restart), configures the services with it and starts this Vite server with env `ADMIN_UI_DEV_TOKEN`; the dev proxy then adds `Authorization: Bearer <token>` server-side to `/api/*` (not `/api/s3`) when the request has none, so no pasting is needed. The token is not bundled into client JS; the server binds 127.0.0.1 only. Running `npm run dev` by hand without that env var behaves as before.
Override: paste a bearer token in the header box (takes precedence over the injected one; dev: the static demo token; later an AAS access token, optionally returned in `#token=` after `VITE_LOGIN_URL`).
Kept in memory + sessionStorage only; never in source. The UI never sends tenant_id (comes from token claims server-side).

## Pages
- Overview: gateway `/healthz` and public `/metrics` counters.
- Uploads: drag&drop CSV/NDJSON (SHA-256, 8 MiB parts, create/part PUT/complete) with source type, tags, sensitivity; list with filters
  source_type/sensitivity/status/label/tag + cursor paging; detail shows status (incl. QUARANTINED), error code, label counts (never values).
  Browser limit 256 MiB (file is hashed in memory).
- Events: `POST /v1/query/events` with fixed filters (time, source, processing version, type, entity) and cursor paging. No free-form SQL anywhere.
- Quality: `/v1/metrics/summary`, `/v1/quality/summary`. The query API needs a source id and (if the source has no active version) a processing version.
- Connectors: connector-manager `GET /v1/connectors`; shows "not available" when the service is not running.
- Jobs / Sources: `/v1/jobs`, `/v1/sources` do not exist in this build -> "API not available" state.

All server strings are rendered via `textContent` (`src/dom.ts`); no `innerHTML`.
Production: serve `dist/` behind a gateway/reverse proxy exposing the same `/api/*` prefixes (or set `VITE_*_BASE`); S3 needs CORS exposing `ETag` if parts are PUT directly (`VITE_S3_PROXY` unset).


BFF mode: build/run with `VITE_AUTH_MODE=bff` behind `services/admin-bff` (same origin; `/api/<service>` is proxied there). The browser then holds no token: requests use the session cookie, unsafe ones add `X-Requested-With: astryon-admin`, and the header shows Sign in/Sign out via `/auth/me`, `/auth/login`, `/auth/logout`.
