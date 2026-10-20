#!/usr/bin/env bash
# Applies dev migrations using the database clients inside the Compose services.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && COMPOSE+=(--env-file "$ROOT/.env")

pg_user="$("${COMPOSE[@]}" exec -T postgres printenv POSTGRES_USER)"
pg_database="$("${COMPOSE[@]}" exec -T postgres printenv POSTGRES_DB)"
ch_user="$("${COMPOSE[@]}" exec -T clickhouse printenv CLICKHOUSE_USER)"
ch_password="$("${COMPOSE[@]}" exec -T clickhouse printenv CLICKHOUSE_PASSWORD)"
ASTRYON_COMPOSE_FILE="$ROOT/infra/docker/docker-compose.dev.yml" \
ASTRYON_PG_USER="$pg_user" \
ASTRYON_PG_DATABASE="$pg_database" \
PG_CLIENT_EXEC="$ROOT/scripts/dev/psql-in-compose.sh" \
ASTRYON_CH_USER="$ch_user" \
ASTRYON_CH_PASSWORD="$ch_password" \
CH_CLIENT_EXEC="$ROOT/scripts/dev/clickhouse-client-in-compose.sh" \
  "$ROOT/db/apply.sh" "${1:-all}"
