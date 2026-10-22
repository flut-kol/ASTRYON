#!/usr/bin/env bash
# Schema compatibility gate: migrations are forward-only, contiguously numbered, each starting with a ROLLBACK PLAN line,
# and contain no destructive statement outside that comment line (those need a reviewed exception).
set -euo pipefail
cd "$(dirname "$0")/.."
rc=0
for dir in db/postgres/migrations db/clickhouse/migrations; do
  n=0
  for f in $(ls "$dir"/*.up.sql | sort); do
    n=$((n+1)); b=$(basename "$f")
    [[ "$b" == $(printf '%04d' "$n")_* ]] || { echo "$f: expected number $(printf '%04d' "$n")" >&2; rc=1; }
    head -1 "$f" | grep -q '^-- ROLLBACK PLAN:' || { echo "$f: first line must be '-- ROLLBACK PLAN:'" >&2; rc=1; }
    if grep -v '^\s*--' "$f" | grep -qiE '\b(DROP +(TABLE|COLUMN|INDEX|TYPE)|TRUNCATE|DELETE +FROM)\b'; then
      echo "$f: destructive statement; needs reviewed exception (add '-- DESTRUCTIVE-OK: <reason>')" >&2
      grep -q 'DESTRUCTIVE-OK:' "$f" || rc=1
    fi
  done
done
exit $rc
