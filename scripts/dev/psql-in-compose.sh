#!/usr/bin/env bash
# Executes psql inside the local PostgreSQL Compose service without flattening
# paths or credentials into a shell-split command string.
set -euo pipefail

: "${ASTRYON_COMPOSE_FILE:?ASTRYON_COMPOSE_FILE is required}"
: "${ASTRYON_PG_USER:?ASTRYON_PG_USER is required}"
: "${ASTRYON_PG_DATABASE:?ASTRYON_PG_DATABASE is required}"

docker compose -f "$ASTRYON_COMPOSE_FILE" exec -T postgres \
  psql -U "$ASTRYON_PG_USER" -d "$ASTRYON_PG_DATABASE" "$@"
