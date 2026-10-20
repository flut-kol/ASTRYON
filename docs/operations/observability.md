# Observability (DPP-015)

Stack: `infra/observability/docker-compose.observability.yml` (Prometheus, Alertmanager, Grafana, OTel Collector, Loki, Tempo, Alloy for container logs, kafka/postgres/node exporters).

    GRAFANA_ADMIN_PASSWORD=... PG_EXPORTER_DSN=postgresql://... \
      docker compose -f infra/observability/docker-compose.observability.yml up -d

ClickHouse must enable its built-in endpoint (`<prometheus><endpoint>/metrics</endpoint><port>9363</port>...</prometheus>`). Apps send OTLP to collector :4317/:4318.
Alert rules: `prometheus/rules/astryon-alerts.yml` (SLO: ack p95 250 ms, section 2; CH disk 70/80%, section 13). Not yet validated with `promtool` (not installed); validate with `docker run --rm --entrypoint promtool -v $PWD/infra/observability/prometheus:/p prom/prometheus:v2.55.1 check rules /p/rules/astryon-alerts.yml`.

## Metrics gap (services do not expose Prometheus metrics yet)

- gateway: only `GET /healthz`; no `/metrics`. Expected (used by rules and dashboards):
  `astryon_gateway_requests_total{route,code}`, `astryon_gateway_events_accepted_total`,
  `astryon_gateway_ack_duration_seconds` (histogram).
- clickhouse-sink: expvar JSON at `/debug/vars` (var `clickhouse_sink`, when `CHSINK_METRICS_ADDR` set), not Prometheus format. Keys: consumed, inserted_rows, quarantined, batches, insert_retries, insert_failures, commits, commit_failures, buffered. Expected Prometheus names: `astryon_clickhouse_sink_{consumed,inserted_rows,quarantined,insert_failures,commit_failures}_total`, gauge `astryon_clickhouse_sink_buffered`. Scrape target assumes port 9102 at `/metrics`.
- Logs: gateway uses `libs/go-common/pkg/logging` (slog); sink uses stdlib `log`. Alloy collects stdout of all containers; host-run processes are not collected.
- Not covered yet (no sources): processor backpressure/checkpoint (Flink), S3/Ceph, query API, cert expiry, backups.
Third-party names assumed: kafka-exporter (`kafka_consumergroup_lag`, `kafka_topic_partition_under_replicated_partition`), ClickHouse native (`ClickHouseAsyncMetrics_*`, `ClickHouseMetrics_*`), postgres-exporter (`pg_replication_lag_seconds` needs a custom query on replicas), node_exporter.
