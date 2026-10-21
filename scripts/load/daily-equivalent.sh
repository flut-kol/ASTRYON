#!/usr/bin/env bash
# DPP-056: 100M/day and 1B/day equivalent load, scaled to the dev stack.
#   100M/day = 1157 ev/s average, 1B/day = 11574 ev/s average; PEAK_FACTOR (default 3) models the daily peak.
#   DEV_SCALE=0.05 DURATION=120s scripts/load/daily-equivalent.sh [100m|1b|both]
# Output: $OUT/daily-100m.json, $OUT/daily-1b.json (ScenarioReport, with pass/fail checks).
. "$(dirname "$0")/lib.sh"
which=${1:-both}; PEAK_FACTOR=${PEAK_FACTOR:-3}
preflight; env_note
run() { # name per-day
  local avg=$(( $2 / 86400 )) rate; rate=$(scaled $(( avg * PEAK_FACTOR )))
  lg "$1" -profile mixed -tenant-skew 0 -rate "$rate" -duration "$DURATION" -enforce -slo-min-rate-frac 0.95 || echo "$1: criteria failed or run error" >&2
  jq --argjson perday "$2" --argjson scale "$DEV_SCALE" '.config += {equivalent_events_per_day:$perday, dev_scale:$scale}' "$OUT/$1.json" > "$OUT/$1.tmp" && mv "$OUT/$1.tmp" "$OUT/$1.json"
}
case $which in
  100m) run daily-100m 100000000 ;;
  1b) run daily-1b 1000000000 ;;
  both) run daily-100m 100000000; run daily-1b 1000000000 ;;
  *) die "usage: $0 [100m|1b|both]" ;;
esac
echo "reports in $OUT"
