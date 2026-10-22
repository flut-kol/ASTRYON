#!/usr/bin/env bash
# DPP-059: query + ingest concurrently, then ingest alone, to verify isolation.
#   QUERY_URL='http://127.0.0.1:8081/v1/events?limit=100' LOADGEN_QUERY_TOKEN=... RATE=1000 scripts/load/query-while-ingest.sh
# Runs (1) ingest only, (2) ingest + queries. Prints the ACK p95 ratio; the isolation verdict is in the report,
# the pass limit is ACK_DEGRADE_MAX (default 1.5x ingest-only p95, a working assumption, not a plan number).
. "$(dirname "$0")/lib.sh"
[ -n "${QUERY_URL:-}" ] || die "set QUERY_URL (a tenant-scoped query-api GET) and LOADGEN_QUERY_TOKEN"
RATE=${RATE:-1000}; QRATE=${QUERY_RATE:-20}
preflight; env_note
lg mixed-ingest-only -profile small-json -rate "$RATE" -duration "$DURATION" || true
lg mixed-ingest-query -profile small-json -rate "$RATE" -duration "$DURATION" -query-url "$QUERY_URL" -query-rate "$QRATE" -query-concurrency "${QUERY_CONCURRENCY:-4}" || true
jq -n --slurpfile a "$OUT/mixed-ingest-only.json" --slurpfile b "$OUT/mixed-ingest-query.json" --argjson max "${ACK_DEGRADE_MAX:-1.5}" '
  ($a[0].phases[0].report.ack_latency_ms.p95) as $p0 | ($b[0].phases[0].report.ack_latency_ms.p95) as $p1 |
  {ingest_only_ack_p95_ms:$p0, with_queries_ack_p95_ms:$p1, ratio:(if $p0>0 then $p1/$p0 else null end),
   query:$b[0].query, isolation_pass:($p0>0 and ($p1/$p0)<=$max and ($b[0].query.errors==0))}' > "$OUT/isolation.json"
cat "$OUT/isolation.json"
