# Admin UI (DPP-030)
Vanilla TypeScript + Vite, no runtime dependencies. Hash routes: Overview, Uploads, Events, Quality, Connectors, Jobs, Sources.

## Run (against the demo stack, `scripts/dev/demo-up.sh`)
`npm install && npm run dev` -> http://127.0.0.1:15173. The Vite dev server proxies same-origin paths, so Go services need no CORS:
`/api/gateway` :18080, `/api/files` :18085, `/api/query` :18082, `/api/connectors` :18088, `/api/exports` :18083, `/api/quarantine` :18086, `/api/replays` :18087, `/api/s3` :19200 (pre-signed part PUTs).
Override with env `PROXY_GATEWAY|PROXY_FILES|PROXY_QUERY|PROXY_CONNECTORS|PROXY_EXPORTS|PROXY_QUARANTINE|PROXY_REPLAYS|PROXY_S3`. `npm run typecheck`, `npm test`, `npm run build`.

## Auth
Dev: `scripts/dev/demo-up.sh` generates a random token once (`openssl rand -hex 24`, file `$DEMO_DIR/dev-token`, mode 0600, reused on restart), configures the services with it and starts this Vite server with env `ADMIN_UI_DEV_TOKEN`; the dev proxy then adds `Authorization: Bearer <token>` server-side to `/api/*` (not `/api/s3`) when the request has none, so no pasting is needed. The token is not bundled into client JS; the server binds 127.0.0.1 only. Running `npm run dev` by hand without that env var behaves as before.
Override: paste a bearer token in the header box (takes precedence over the injected one; dev: the static demo token; later an AAS access token, optionally returned in `#token=` after `VITE_LOGIN_URL`).
Kept in memory + sessionStorage only; never in source. The UI never sends tenant_id (comes from token claims server-side).

## Pages
- Overview: operations dashboard with gateway health, ACK/error KPIs, service reachability states, privacy callout and quick actions; gateway `/healthz` and public `/metrics` counters.
- Uploads: drag&drop CSV/NDJSON (SHA-256, 8 MiB parts, create/part PUT/complete) with source type, tags, sensitivity; list with filters
  source_type/sensitivity/status/label/tag + cursor paging; detail shows status (incl. QUARANTINED), error code, label counts (never values).
  Browser limit 256 MiB (file is hashed in memory).
- Events: `POST /v1/query/events` with fixed filters (time, source, processing version, type, entity) and cursor paging. No free-form SQL anywhere.
- Quality: `/v1/metrics/summary`, `/v1/quality/summary`. The query API needs a source id and (if the source has no active version) a processing version.
- Connectors: connector-manager `GET /v1/connectors`; shows "not available" when the service is not running.
- Jobs / Sources: `/v1/jobs`, `/v1/sources` do not exist in this build -> "API not available" state.

All server strings are rendered via `textContent` (`src/dom.ts`); no `innerHTML`.
Production: serve `dist/` behind a gateway/reverse proxy exposing the same `/api/*` prefixes (or set `VITE_*_BASE`); S3 needs CORS exposing `ETag` if parts are PUT directly (`VITE_S3_PROXY` unset).


BFF mode: build/run with `VITE_AUTH_MODE=bff` behind `services/admin-bff` (dev: `npm run dev` proxies `/auth` and `/api` to `PROXY_BFF`, default http://127.0.0.1:18090; run the BFF with `BFF_AFTER_LOGIN_URL=http://127.0.0.1:15173/`; same origin; `/api/<service>` is proxied there). The browser then holds no token: requests use the session cookie, unsafe ones add `X-Requested-With: astryon-admin`, and the header shows Sign in/Sign out via `/auth/me`, `/auth/login`, `/auth/logout`.

## Accessibility and UI tests (DPP-055)
- Semantics: skip link, `banner`/`nav[aria-label=Main]`/`main#main` landmarks (focused on route change), `aria-current` on the active route, one `h1`, document title per page,
  `th scope=col` and a hidden `caption` on every table, all inputs labelled, upload detail opened from a real `button`, async areas are `role=status`/`aria-live=polite`, errors `role=alert`,
  the drop zone always has the file chooser as keyboard fallback, visible `:focus-visible`, `prefers-reduced-motion` honoured.
- Contrast (WCAG 2.x, computed from the CSS tokens; text needs 4.5:1, control borders 3:1): light text pairs 4.54-16.6, dark 6.5-14.7; fixed in this pass:
  dark `button.pri` was white on `#7aa2ff` (2.49:1) and now uses `--onac:#0b1220` (7.52:1); control borders `--bd` were 1.38/1.41:1 and controls now use `--ctl` (3.67/4.27:1).
  The lowest remaining text pair is `--warn` on the page background in light mode (4.54:1).
- `npm test`: vitest, including `src/a11y.test.ts` (axe-core 4.10.2 under jsdom over all 10 routes, token and BFF mode, error state; fails on serious/critical; jsdom cannot compute colour contrast).
- `npm run e2e`: Playwright 1.49.1 against its own Vite server (port 15174, BFF mode) with a mocked backend (`src/mockApi.ts`, `page.route`); no AAS, services or tokens. One-time: `npx playwright install chromium`.
  `e2e/smoke.spec.ts` (sign-in states, overview, events query, quarantine + dry-run, uploads, 403 and 503 states, focus on route change) and `e2e/contrast.spec.ts` (axe with colour contrast in the real browser, light and dark, all pages).

## Design, themes and languages
- Layout: sticky top bar (brand, session, language and theme switchers), grouped Monitor/Operate/Manage sidebar navigation (horizontal bar below 820 px), responsive overview cards and keyboard-friendly quick actions. Styles live in `src/style.css` as CSS tokens (no inline style, no UI dependencies); icons are static SVG paths built in `src/dom.ts` (`svg()`).
- Themes: System (follows `prefers-color-scheme`), Light, Dark; forced theme is `<html data-theme>`, kept in `localStorage` (`astryon_admin_theme`) and applied by a tiny inline script before first paint (`src/theme.ts`).
- Languages: English and Ukrainian (`src/i18n.ts`). Code passes the English text to `t()`; the UA dictionary is keyed by it and missing entries fall back to English. Default: stored choice, else browser language `uk*` -> UA, else EN; kept in `localStorage` (`astryon_admin_lang`); `<html lang>` is `en`/`uk`. Server strings (ids, error messages, payloads) are not translated. To add a string: use `t('English text')` and add the UA entry.
- Tests: `src/i18n.test.ts` (switching, persistence, placeholders), e2e switcher test, and contrast/axe runs in UA, in both OS schemes and with each forced theme against the opposite OS scheme.
