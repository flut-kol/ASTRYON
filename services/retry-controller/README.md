# retry-controller

Releases due tenant-scoped job retries from PostgreSQL. It uses `FOR UPDATE
SKIP LOCKED`, so replicas can run concurrently.

Required environment variable: `DATABASE_URL` (PostgreSQL URL).

Optional variables:

- `ASTRYON_RETRY_POLL_INTERVAL` — positive Go duration; default `2s`.
- `ASTRYON_RETRY_BATCH` — positive maximum due rows per poll; default `100`.
- `ASTRYON_RETRY_METRICS_ADDR` — metrics/health bind address; default `:8081`.

Endpoints: `GET /healthz`, `GET /metrics`. The controller never accepts a
tenant ID from HTTP input and exposes only aggregate, tenant-free metrics.

Run after control-plane migrations are applied:

```bash
DATABASE_URL='<platform-provided-postgres-url>' go run ./cmd/retry-controller
```

See `docs/operations/runbooks/retry-controller.md` for diagnosis and recovery.
