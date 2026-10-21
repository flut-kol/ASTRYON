#!/usr/bin/env bash
# Stops the astryon_* stack started by apps-up.sh. --volumes also removes data.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export APPS_UID="$(id -u)" APPS_GID="$(id -g)"
docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml" -f "$ROOT/infra/docker/docker-compose.apps.yml" down "$@"
