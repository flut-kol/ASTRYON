# Admin UI (DPP-030)
Vanilla TS + Vite. Pages (hash routes): counters, jobs, sources, health.

- `npm install && npm run mock` (port 8099) then `npm run dev` (proxies `/v1`, `/healthz` to `VITE_API_PROXY`, default mock).
- `npm test`, `npm run build`.
- Config (build-time env, no secrets): `VITE_API_BASE_URL` (default same-origin), `VITE_LOGIN_URL` (AAS login; must redirect back with `#token=<access_token>`). Without it a dev paste-token field is shown. Token lives in sessionStorage only. tenant_id is never sent by the UI; it comes from the token claims server-side.
- Errors render `code: message` plus `trace_id` (falls back to `X-Request-ID`).

## API contract
Types in `src/types.ts` mirror `docs/api/openapi.yaml` (metrics `{from,to,totals,by_source,series}`, quality `{events_*,flags,top_errors}`, sources `{kind,is_enabled}`, jobs with `JobStatus` enum, list envelope `{items,next_cursor}`, health `{status:ok|degraded,checks}`; `/healthz` 503 still carries the Health body). Errors: `{code,message,details,trace_id}`. Pages show the first page only (no cursor UI yet); metrics/quality use the server default 24h range.
