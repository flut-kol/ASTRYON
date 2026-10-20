#!/usr/bin/env bash
# Executes clickhouse-client in the local Compose service without shell-split
# paths or credentials.
set -euo pipefail

: "${ASTRYON_COMPOSE_FILE:?ASTRYON_COMPOSE_FILE is required}"
: "${ASTRYON_CH_USER:?ASTRYON_CH_USER is required}"
: "${ASTRYON_CH_PASSWORD:?ASTRYON_CH_PASSWORD is required}"

docker compose -f "$ASTRYON_COMPOSE_FILE" exec -T clickhouse \
  clickhouse-client --user "$ASTRYON_CH_USER" --password "$ASTRYON_CH_PASSWORD" "$@"
