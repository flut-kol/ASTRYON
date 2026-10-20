#!/usr/bin/env bash
# DPP-010 laptop benchmark helper. Usage:
#   scripts/dev/bench.sh setup                      # stack + topics + CH schema + builds + gateway + sink
#   scripts/dev/bench.sh run RATE DUR BATCH CONC    # one loadgen run + resource sampling + sink drain
#   scripts/dev/bench.sh teardown
# Env: BENCH_DIR (work dir, default ${TMPDIR:-/tmp}/astryon-bench), BENCH_PARTITIONS (raw.events.v1 partitions override, setup only),
#      BENCH_GW_ENV / BENCH_SINK_ENV (extra "K=V K=V" for gateway / sink).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"; cd "$ROOT"
export COMPOSE_PROJECT_NAME=astryon-bench GOTOOLCHAIN=local
D="${BENCH_DIR:-${TMPDIR:-/tmp}/astryon-bench}"; mkdir -p "$D"
TENANT=0a1b2c3d-1111-4222-8333-444455556666; TOKEN=bench
CHC=(docker exec -i astryon-bench-clickhouse-1 clickhouse-client --user astryon --password astryon_dev_only)
KT=(docker exec astryon-bench-kafka-1 /opt/kafka/bin)
stop_procs() { for n in gateway sink; do [ -f "$D/$n.pid" ] && kill -TERM "$(cat "$D/$n.pid")" 2>/dev/null || true; rm -f "$D/$n.pid"; done; }
case "${1:-}" in
setup)
  scripts/dev/up.sh kafka clickhouse
  make topics >/dev/null
  if [ -n "${BENCH_PARTITIONS:-}" ]; then
    "${KT[@]}"/kafka-topics.sh --bootstrap-server kafka:9092 --alter --topic raw.events.v1 --partitions "$BENCH_PARTITIONS"
  fi
  CH_CLIENT="docker exec -i astryon-bench-clickhouse-1 clickhouse-client" CH_USER=astryon CH_PASSWORD=astryon_dev_only db/apply.sh clickhouse >/dev/null
  (cd services/gateway && go build -o "$D/gateway" ./cmd/gateway)
  (cd services/clickhouse-sink && go build -o "$D/sink" ./cmd/clickhouse-sink)
  (cd tests/load && go build -o "$D/loadgen" .)
  env ASTRYON_DEV_AUTH=1 ASTRYON_DEV_TOKEN=$TOKEN ASTRYON_DEV_TENANT_ID=$TENANT ASTRYON_GATEWAY_ADDR=127.0.0.1:18080 \
    ASTRYON_KAFKA_BROKERS=127.0.0.1:19092 ASTRYON_GATEWAY_MAX_BATCH_EVENTS=1000 ${BENCH_GW_ENV:-} \
    "$D/gateway" >"$D/gateway.log" 2>&1 & echo $! >"$D/gateway.pid"
  env CHSINK_TOPIC=raw.events.v1 CHSINK_CH_PASSWORD=astryon_dev_only CHSINK_METRICS_ADDR=127.0.0.1:18081 ${BENCH_SINK_ENV:-} \
    "$D/sink" >"$D/sink.log" 2>&1 & echo $! >"$D/sink.pid"
  sleep 3; echo "gateway pid $(cat "$D/gateway.pid") sink pid $(cat "$D/sink.pid")" ;;
run)
  RATE=$2; DUR=$3; BATCH=$4; CONC=$5; TAG="r${RATE}_b${BATCH}_c${CONC}"
  sunk() { curl -s 127.0.0.1:18081/debug/vars | sed -n 's/.*"inserted_rows":\([0-9]*\).*/\1/p'; }
  S0=$(sunk)
  ( while true; do
      echo "== $(date +%s)"; docker stats --no-stream --format '{{.Name}} cpu={{.CPUPerc}} mem={{.MemUsage}}' | grep astryon-bench
      ps -axo %cpu=,rss=,args= | awk '/astryon-bench\/(gateway|sink|loadgen)|\/(gateway|sink|loadgen)( |$)/ && !/awk|bench.sh/ {n=$3; sub(".*/","",n); printf "host %s cpu=%s rss_mb=%d\n", n, $1, $2/1024}'
      top -l1 -n0 | grep -m1 'CPU usage'
    done >"$D/stats_$TAG.txt" 2>&1 ) & SP=$!
  LOADGEN_TOKEN=$TOKEN LOADGEN_TENANT_ID=$TENANT "$D/loadgen" -url http://127.0.0.1:18080/v1/events/batch -rate "$RATE" \
    -duration "${DUR}s" -batch "$BATCH" -concurrency "$CONC" -json-out "$D/report_$TAG.json" | tee "$D/loadgen_$TAG.txt"
  kill $SP 2>/dev/null || true; wait $SP 2>/dev/null || true
  ACC=$(sed -n 's/.*"accepted": *\([0-9]*\).*/\1/p' "$D/report_$TAG.json" | head -1)
  T0=$(date +%s); LAG=$(( ACC - ($(sunk) - S0) )); echo "sink backlog at end of load: $LAG events"
  while [ $(( ACC - ($(sunk) - S0) )) -gt 0 ] && [ $(( $(date +%s) - T0 )) -lt 300 ]; do sleep 1; done
  echo "sink drain time after load: $(( $(date +%s) - T0 )) s (accepted=$ACC inserted_delta=$(( $(sunk) - S0 )))" | tee "$D/drain_$TAG.txt"
  echo "$LAG" >"$D/lag_$TAG.txt" ;;
teardown)
  stop_procs; sleep 2; scripts/dev/down.sh --volumes ;;
*) sed -n 2,8p "$0"; exit 2 ;;
esac
