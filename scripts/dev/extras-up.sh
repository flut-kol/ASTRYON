#!/usr/bin/env bash
# DEV extras: Redis, separate logical-replication PostgreSQL, Kafka Connect + Debezium and one demo connector.
# Needs the dev stack (scripts/dev/up.sh) for astryon_kafka and the astryon-dev_default network. DEV ONLY.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
DC=(docker compose -f "$ROOT/infra/docker/docker-compose.extras.yml")
[ -f "$ROOT/.env" ] && DC+=(--env-file "$ROOT/.env")
KAFKA=astryon_kafka
docker inspect -f '{{.State.Health.Status}}' "$KAFKA" 2>/dev/null | grep -q healthy || { echo "start the dev stack first (scripts/dev/up.sh)" >&2; exit 1; }
# Kafka auto-create is off: Connect internal topics (compacted, RF=1) and the demo data topic.
mk() { docker exec "$KAFKA" /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 1 "${@:3}" >/dev/null; }
mk astryon.connect.configs 1 --config cleanup.policy=compact
mk astryon.connect.offsets 1 --config cleanup.policy=compact
mk astryon.connect.status 1 --config cleanup.policy=compact
mk astryon_cdc.public.demo_items 1
"${DC[@]}" up -d --wait
PORT="${CONNECT_HOST_PORT:-18484}"
code=$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$PORT/connectors/astryon-demo-cdc" || true)
if [ "$code" != 200 ]; then
  curl -fsS -X POST -H 'Content-Type: application/json' --data @"$ROOT/infra/docker/extras/debezium-demo.json" "http://127.0.0.1:$PORT/connectors" >/dev/null
fi
echo "redis 127.0.0.1:${REDIS_HOST_PORT:-16379}  connect http://127.0.0.1:$PORT  pg_cdc 127.0.0.1:${PG_CDC_HOST_PORT:-15433}"
echo "demo topic: astryon_cdc.public.demo_items (ASTRYON_CDC_TOPIC, ASTRYON_CDC_PRIMARY_KEY=id)"
