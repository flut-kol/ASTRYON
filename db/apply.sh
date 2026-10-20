#!/usr/bin/env bash
# Apply ASTRYON migrations in order. No secrets in repo: connection is taken from env.
#   Postgres:   standard libpq vars (PGHOST PGPORT PGUSER PGPASSWORD PGDATABASE) or DATABASE_URL.
#   ClickHouse: CH_HOST (default localhost) CH_PORT (9000) CH_USER CH_PASSWORD CH_DATABASE (default astryon);
#               CH_CLIENT (optional: full client command, e.g. 'docker exec -i <container> clickhouse-client', overrides host/port);
#               CH_CLUSTER (optional; enables ON CLUSTER + Distributed over it), CH_REPLICATED=1 (ReplicatedMergeTree).
# Usage: db/apply.sh [postgres|clickhouse|seeds|all]   (default: all, excluding seeds unless named / APPLY_SEEDS=1)
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
target="${1:-all}"

pg_apply() {
  local conn=("${DATABASE_URL:-}"); [ -z "${DATABASE_URL:-}" ] && conn=()
  local psql=(psql "${conn[@]}" -v ON_ERROR_STOP=1 -X -q)
  "${psql[@]}" -c "CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())"
  for f in "$here"/postgres/migrations/*.up.sql; do
    n="$(basename "$f")"
    if [ "$("${psql[@]}" -tA -c "SELECT 1 FROM schema_migrations WHERE name='$n'")" = 1 ]; then continue; fi
    echo "pg: applying $n"
    # single transaction per migration, recorded atomically
    { echo "BEGIN;"; cat "$f"; echo "INSERT INTO schema_migrations(name) VALUES ('$n');"; echo "COMMIT;"; } | "${psql[@]}"
  done
}

ch_apply() {
  local db="${CH_DATABASE:-astryon}"
  local ch
  if [ -n "${CH_CLIENT:-}" ]; then
    # e.g. CH_CLIENT="docker exec -i astryon-e2e-clickhouse-1 clickhouse-client" when the host has no clickhouse-client
    read -r -a ch <<<"$CH_CLIENT"; ch+=(--multiquery)
  else
    ch=(clickhouse-client --host "${CH_HOST:-localhost}" --port "${CH_PORT:-9000}" --multiquery)
  fi
  [ -n "${CH_USER:-}" ] && ch+=(--user "$CH_USER")
  [ -n "${CH_PASSWORD:-}" ] && ch+=(--password "$CH_PASSWORD")
  local onc="" cluster="${CH_CLUSTER:-default}" rmt="MergeTree()"
  [ -n "${CH_CLUSTER:-}" ] && onc="ON CLUSTER ${CH_CLUSTER}"
  [ "${CH_REPLICATED:-0}" = 1 ] && rmt="ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')"
  "${ch[@]}" -q "CREATE DATABASE IF NOT EXISTS ${db} ${onc}" </dev/null
  "${ch[@]}" -d "$db" -q "CREATE TABLE IF NOT EXISTS schema_migrations (name String, applied_at DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY name" </dev/null
  for f in "$here"/clickhouse/migrations/*.up.sql; do
    n="$(basename "$f")"
    if [ "$("${ch[@]}" -d "$db" -q "SELECT count() FROM schema_migrations WHERE name='$n'" </dev/null)" != 0 ]; then continue; fi
    echo "ch: applying $n"
    sed -e "s|__ON_CLUSTER__|${onc}|g" -e "s|__RMT__|${rmt}|g" -e "s|__CLUSTER__|${cluster}|g" "$f" | "${ch[@]}" -d "$db"
    "${ch[@]}" -d "$db" -q "INSERT INTO schema_migrations (name) VALUES ('$n')" </dev/null   # </dev/null: 'docker exec -i' would otherwise wait on stdin
  done
}

seeds_apply() {
  for f in "$here"/seeds/*.sql; do echo "seed: $(basename "$f")"; psql ${DATABASE_URL:+"$DATABASE_URL"} -v ON_ERROR_STOP=1 -X -q -f "$f"; done
}

case "$target" in
  postgres) pg_apply ;;
  clickhouse) ch_apply ;;
  seeds) seeds_apply ;;
  all) pg_apply; ch_apply; [ "${APPLY_SEEDS:-0}" = 1 ] && seeds_apply || true ;;
  *) echo "usage: $0 [postgres|clickhouse|seeds|all]" >&2; exit 2 ;;
esac
