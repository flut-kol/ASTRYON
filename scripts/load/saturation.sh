#!/usr/bin/env bash
# DPP-058: rate ladder to find the maximum stable rate, optional hot-key / skew run.
#   RATES="500 1000 2000 4000 8000" STEP_DURATION=30s scripts/load/saturation.sh [ladder|hotkey]
# ladder: one run per rate (small-json); stops at the first rate whose checks fail twice in a row.
#   Output: $OUT/sat-<rate>.json and $OUT/capacity-curve.json (rate, achieved, ack p95/p99, failed, pass).
# hotkey: skewed-tenants profile (Zipf tenants + 20% one entity_key) at HOT_RATE; needs several tenants.
# Host saturation (CPU/RAM/disk/network of gateway, Kafka, Flink, ClickHouse) is NOT sampled here: read the
# node/container dashboards (infra/observability) for the same time window, recorded in environment.json.
. "$(dirname "$0")/lib.sh"
mode=${1:-ladder}; STEP_DURATION=${STEP_DURATION:-30s}
preflight; env_note
case $mode in
  ladder)
    fails=0
    for r in ${RATES:-500 1000 2000 4000 8000}; do
      lg "sat-$r" -profile small-json -rate "$r" -duration "$STEP_DURATION" -enforce || true
      jq -e '.pass' "$OUT/sat-$r.json" >/dev/null && fails=0 || fails=$((fails+1))
      [ "$fails" -ge 2 ] && { echo "two consecutive failing steps; stopping at $r" >&2; break; }
    done
    jq -s '[.[] | {target:.config.target_events_per_sec, achieved:.phases[0].report.events_per_sec, ack_p95_ms:.phases[0].report.ack_latency_ms.p95,
             ack_p99_ms:.phases[0].report.ack_latency_ms.p99, failed:.phases[0].report.failed, pass}]
            | {capacity_curve:., max_stable_target:([.[]|select(.pass)|.target]|max)}' "$OUT"/sat-*.json > "$OUT/capacity-curve.json"
    cat "$OUT/capacity-curve.json" ;;
  hotkey)
    lg hotkey -profile skewed-tenants -rate "${HOT_RATE:-1000}" -duration "$DURATION" -enforce ;;
  *) die "usage: $0 [ladder|hotkey]" ;;
esac
