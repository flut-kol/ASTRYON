#!/usr/bin/env bash
# DPP-047: query-api SQL shapes under different tenant distributions and concurrency.
# Starts a throwaway ClickHouse (own container, own port), loads synthetic data into the real
# 0001 schema, runs clickhouse-benchmark, prints a markdown table, removes the container.
#   ROWS=10000000 TENANTS=200 ITER=300 scripts/bench/query-profiles.sh
set -euo pipefail
cd "$(dirname "$0")/../.."
ROWS=${ROWS:-10000000}; TENANTS=${TENANTS:-200}; ITER=${ITER:-300}
IMAGE=${CH_IMAGE:-clickhouse/clickhouse-server:24.8}
NAME=qbench-ch-$$
PORT=${QB_PORT:-18623}
trap 'docker rm -f "$NAME" >/dev/null 2>&1 || true' EXIT
docker run -d --memory "${QB_MEM:-6g}" --name "$NAME" -p "$PORT:8123" "$IMAGE" >/dev/null
ch() { docker exec -i "$NAME" clickhouse-client "$@"; }
for _ in $(seq 60); do ch -q 'SELECT 1' >/dev/null 2>&1 && break; sleep 1; done

DDL=$(sed -e 's/__ON_CLUSTER__//' -e 's/__RMT__/MergeTree/' db/clickhouse/migrations/0001_events_normalized.up.sql \
  | awk '/CREATE TABLE IF NOT EXISTS events_normalized_local/{p=1} /events_normalized_distributed/{p=0} p')
# tenant index expression per scenario (t in 0..TENANTS-1)
declare -A TEN=(
  [uniform]="number % $TENANTS"
  [skewed]="toUInt32(floor($TENANTS * pow(rand64() / 18446744073709551615., 4)))"
  [hot]="if(rand64() % 10 < 8, 0, 1 + number % ($TENANTS - 1))"
)
for sc in ${SCENARIOS:-uniform skewed hot}; do
  ch -q "${DDL//events_normalized_local/ev_$sc}"
  ch -q "INSERT INTO ev_$sc (tenant_id, source_id, event_id, batch_id, processing_version, pipeline_id, pipeline_version, event_type, event_time, entity_id, attributes, payload, raw_ref, quality_flags)
    SELECT t_id, toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(t % 5), 12, '0'))), generateUUIDv4(), generateUUIDv4(), 1, generateUUIDv4(), 1,
      ['click','view','buy','error'][1 + number % 4], toDateTime64('2026-10-01 00:00:00', 3, 'UTC') + (number % 2592000) + rand() % 1000 / 1000,
      toString(number % 50000), map('k', toString(number % 7)), repeat('x', 200 + number % 300), 'kafka://t/0/1', []
    FROM (SELECT number, ($(echo "${TEN[$sc]}")) AS t,
      toUUID(concat('00000000-0000-4000-8000-', leftPad(toString(t), 12, '0'))) AS t_id FROM numbers($ROWS))" 
  # tenant 0 is the hot/largest one in skewed+hot; for uniform it is just a typical one. Tail tenant = last.
done
tid() { printf '00000000-0000-4000-8000-%012d' "$1"; }
SRC=00000000-0000-4000-8000-000000000001
echo "| scenario | tenant | query | clients | QPS | p50 ms | p95 ms | p99 ms | rows of tenant |"
echo "|---|---|---|---|---|---|---|---|---|"
for sc in ${SCENARIOS:-uniform skewed hot}; do
  # tail first: a hot-tenant run at 16 clients can OOM-kill the container
  for who in "$((TENANTS-1)) tail" "0 hot/largest"; do
    set -- $who; t=$1; label=$2
    n=$(ch -q "SELECT count() FROM ev_$sc WHERE tenant_id='$(tid $t)'")
    Q_EVENTS="SELECT event_id, source_id, batch_id, processing_version, event_type, event_time, entity_id, attributes, payload, quality_flags FROM ev_$sc WHERE tenant_id='$(tid $t)' AND event_time >= '2026-10-10 00:00:00' AND event_time < '2026-10-17 00:00:00' ORDER BY event_time, event_id LIMIT 1 BY tenant_id, source_id, event_id, processing_version LIMIT 101 FORMAT Null"
    Q_AGG="SELECT event_type, count() AS events FROM (SELECT * FROM ev_$sc WHERE tenant_id='$(tid $t)' AND event_time >= '2026-10-01 00:00:00' AND event_time < '2026-10-31 00:00:00' LIMIT 1 BY tenant_id, source_id, event_id, processing_version) GROUP BY event_type ORDER BY event_type LIMIT 1000 FORMAT Null"
    for qn in events aggregate; do
      q=$Q_EVENTS; [ $qn = aggregate ] && q=$Q_AGG
      for c in 1 16; do
        out=$(echo "$q" | docker exec -i "$NAME" clickhouse-benchmark -c $c -i $ITER --max_threads=4 --max_memory_usage=2000000000 --max_execution_time=30 2>&1 | tr '\r' '\n' || true)
        qps=$(echo "$out" | grep -o 'QPS: [0-9.]*' | tail -1 | cut -d' ' -f2 || true)
        p() { echo "$out" | grep -E "^ *$1\.000%" | tail -1 | awk '{printf "%.0f", $2*1000}' || true; }
        echo "| $sc | $label | $qn | $c | $qps | $(p 50) | $(p 95) | $(p 99) | $n |"
      done
    done
  done
done
