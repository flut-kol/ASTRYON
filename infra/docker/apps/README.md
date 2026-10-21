# Dev application stack (one container, 11 Go services)

DEV ONLY. `make apps-up` brings up the dev infra with `astryon_*` container names (Kafka, PostgreSQL 16, ClickHouse 24.8,
S3 mock), creates topics, applies PG/CH migrations, seeds a dev tenant/role/source and the S3 buckets `files`/`exports`,
builds `astryon_apps:dev` (multi-stage, `golang:1.27`, `go.work`, `CGO_ENABLED=0`) and starts it. `make apps-logs`, `make apps-down`
(`scripts/dev/apps-down.sh --volumes` also drops data). Files: `Dockerfile`, `entrypoint.sh` (here), `../docker-compose.apps.yml`
(override of the base compose file, which is unchanged), `scripts/dev/apps-up.sh`.

| Service | Port (127.0.0.1) |
|---|---|
| gateway | 18080 |
| query-api | 18082 |
| export-worker | 18083 |
| file-api | 18085 |
| quarantine | 18086 |
| replay-controller | 18087 |
| connector-manager (admin API) | 18088 |
| clickhouse-sink, file-worker, replay-worker, retry-controller | no public port |

Tenant `00000000-0000-0000-0000-0000000000a1`, source `00000000-0000-0000-0000-0000000000b1`. clickhouse-sink reads `raw.events.v1`
directly (no normalizer in this repo). Not included: admin-ui, admin-bff, ch-aggregator (run on demand), Flink, Redis, Debezium.

## Auth modes
- **Dev (default):** static bearer token, generated once into `.astryon-dev/dev-token` (0600, gitignored), mounted read-only at
  `/run/secrets/astryon-dev-token`; the container runs as the host uid so the mode stays 0600. It is never printed or logged.
- **AAS (opt-in):** `AAS_BASE_URL=... AAS_ISSUER=... AAS_AUDIENCE=... AAS_ALLOWED_AZP=... [AAS_ENV=prod] make apps-up`. All services then
  verify AAS tokens and ignore the dev token. Live AAS tokens carry `env=prod` (default here). The realm defines all ASTRYON scopes
  (`realm-config/clients/astryon.yaml` in the AAS repo, applied 2026-10-21); allow every client `azp` you use, e.g.
  `AAS_ALLOWED_AZP=astryon-dev-ingest,astryon-portal`. Scope names: `docs/architecture/roles.md`.

## Verified (2026-10-21)
From a clean container state, no scratchpad: 11 processes; loadgen 350 events through the gateway landed in ClickHouse (1000 -> 1350 rows);
file-api, quarantine, replay-controller, connector-manager 200 with the dev token, query-api 200 on `POST /v1/query/events`, gateway 401 without token.
Caveat: the dev token is passed via a bind-mounted file, but the infra credentials are placeholders in the compose files.
