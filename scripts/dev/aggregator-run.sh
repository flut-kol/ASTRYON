#!/usr/bin/env bash
# One-shot ch-aggregator run against the dev stack (astryon_clickhouse, astryon_postgres on astryon-dev_default).
# Usage: scripts/dev/aggregator-run.sh [run|reconcile ...]   (default: recompute aggregates once). DEV ONLY creds.
# Builds services/ch-aggregator/Dockerfile (needs the golang:1.27 image); AGG_BUILD=host builds a linux binary with the
# host Go toolchain and runs it in a distroless-free alpine container instead.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
NET=astryon-dev_default; IMG=astryon/ch-aggregator:dev
CH_USER="${CLICKHOUSE_USER:-astryon}"; CH_PASS="${CLICKHOUSE_PASSWORD:-astryon_dev_only}"
if [ "${AGG_BUILD:-docker}" = host ]; then
  d="$(mktemp -d)"; trap 'rm -rf "$d"' EXIT
  (cd services/ch-aggregator && CGO_ENABLED=0 GOOS=linux go build -o "$d/ch-aggregator" ./cmd/ch-aggregator)
  printf 'FROM alpine:3.20\nCOPY ch-aggregator /ch-aggregator\nENTRYPOINT ["/ch-aggregator"]\n' >"$d/Dockerfile"
  docker build -q -t "$IMG" "$d" >/dev/null
else
  docker build -q -f services/ch-aggregator/Dockerfile -t "$IMG" . >/dev/null
fi
docker run --rm --network "$NET" \
  -e CH_URL=http://clickhouse:8123 -e CH_DATABASE="${CLICKHOUSE_DB:-astryon}" -e CH_USER="$CH_USER" -e CH_PASSWORD="$CH_PASS" \
  -e REGISTRY_PG_URL="postgres://${POSTGRES_USER:-astryon}:${POSTGRES_PASSWORD:-astryon_dev_only}@postgres:5432/${POSTGRES_DB:-astryon}" \
  "$IMG" "$@"
