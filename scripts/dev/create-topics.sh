#!/usr/bin/env bash
# Створює Kafka topics з infra/kafka/topics.yaml у dev-кластері (ідемпотентно).
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
COMPOSE=(docker compose -f "$ROOT/infra/docker/docker-compose.dev.yml")
[ -f "$ROOT/.env" ] && COMPOSE+=(--env-file "$ROOT/.env")
YAML="$ROOT/infra/kafka/topics.yaml"
RF="$(awk '/dev_replication_factor:/ {print $2}' "$YAML")"

grep -E '^\s*- \{name:' "$YAML" | while IFS= read -r line; do
  name=$(sed -E 's/.*name: *([^,}]+).*/\1/' <<<"$line")
  parts=$(sed -E 's/.*[{ ,]partitions: *([0-9]+).*/\1/' <<<"$line")
  ret=$(sed -E 's/.*retention_ms: *([0-9]+).*/\1/' <<<"$line")
  echo "topic $name partitions=$parts rf=$RF retention.ms=$ret"
  "${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server kafka:9092 --create --if-not-exists \
    --topic "$name" --partitions "$parts" --replication-factor "$RF" \
    --config "retention.ms=$ret" --config min.insync.replicas=1 </dev/null   # </dev/null: exec must not swallow the loop's stdin
done
"${COMPOSE[@]}" exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:9092 --list
