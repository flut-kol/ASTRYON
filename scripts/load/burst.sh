#!/usr/bin/env bash
# DPP-057: short burst (default 120k ev/s of the small-JSON profile) with backlog-drain measurement.
#   BURST_RATE=120000 BURST_DURATION=30s PROM_URL=http://127.0.0.1:9090 \
#   BACKLOG_QUERY='sum(kafka_consumergroup_lag{group="clickhouse-sink"})' scripts/load/burst.sh
# Without PROM_URL/BACKLOG_QUERY only the ingest side is measured (the report has no "backlog" and no drain check).
# 120k ev/s needs a many-core load generator host; the report records achieved vs target rate.
. "$(dirname "$0")/lib.sh"
BURST_RATE=${BURST_RATE:-120000}; BURST_DURATION=${BURST_DURATION:-30s}; BASELINE_RATE=${BASELINE_RATE:-1000}
DRAIN_WAIT=${DRAIN_WAIT:-300s}; MAX_DRAIN_SEC=${MAX_DRAIN_SEC:-300}
preflight; env_note
args=(-profile small-json -burst-rate "$BURST_RATE" -burst-duration "$BURST_DURATION" -baseline-rate "$BASELINE_RATE" -drain-wait "$DRAIN_WAIT" -slo-min-rate-frac "${MIN_RATE_FRAC:-0.95}")
if [ -n "${PROM_URL:-}" ] && [ -n "${BACKLOG_QUERY:-}" ]; then
  args+=(-prom-url "$PROM_URL" -backlog-query "$BACKLOG_QUERY" -backlog-threshold "${BACKLOG_THRESHOLD:-1000}" -slo-max-drain-sec "$MAX_DRAIN_SEC")
fi
lg burst "${args[@]}" -enforce
