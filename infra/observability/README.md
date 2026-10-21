# ASTRYON observability baseline (DPP-015)

The Collector receives OTLP on 4317/4318, bounds memory, batches telemetry and
exposes metrics for Prometheus on 8889. Trace/log export is deliberately
debug-only until the approved backend and retention policy are selected; do not
send tenant payloads, tokens or secret values in telemetry.

Prometheus rules cover ACK errors, consumer lag, ClickHouse disk capacity and
certificate expiry. A deployment owner must wire service discovery, exporters,
Alertmanager receivers and the referenced runbooks before enabling paging.
Thresholds are initial guardrails, not production SLOs; tune them after
benchmark and capacity evidence.

The Grafana JSON dashboard uses those metrics but is not provisioned
automatically: import it only after the Prometheus datasource and metric names
are verified in the target environment.

## Rules and dashboards from emitted metrics

`prometheus-rules.yaml` (recording rules `astryon:*`, alerts) and
`dashboards/*.json` (ingest, pipeline, files, retry-connectors,
query-aggregates, slo, export-replay; Grafana datasource variable `DS_PROMETHEUS`) use only metrics
that Go services emit today (`astryon_gateway_*`, `astryon_clickhouse_sink_*`,
`astryon_file_worker_*`, `astryon_file_api_*`, `astryon_connector_runner_*`,
`astryon_retry_*`, `astryon_query_api_*`, `astryon_ch_aggregator_*`,
`astryon_ch_reconcile_*`, `astryon_export_*`, `astryon_replay_controller_*`,
`astryon_replay_worker_*`, `astryon_quarantine_*`) plus the external `kafka_consumergroup_lag`
(kafka_exporter) and node_exporter filesystem series. Validate:

    docker run --rm --user $(id -u) --entrypoint promtool \
      -v $PWD/infra/observability:/w:ro prom/prometheus:v2.55.1 check rules /w/prometheus-rules.yaml
    for f in infra/observability/dashboards/*.json; do python3 -m json.tool $f >/dev/null; done

### SLO rules

Group `astryon-slo-*`: ACK latency SLI is the share of `outcome="ok"` gateway ACKs
slower than 250 ms (histogram bucket `le="0.25"`, the section 2 p95 target) against a
5 percent budget. Burn alerts: fast (1h and 5m > 6x), slow (6h and 30m > 2x). The p99
alert, query p95 (2.5 s), retry backlog (1000) and oldest-due age (600 s) thresholds
are initial guardrails, not agreed SLOs. Export-worker, replay and quarantine
metrics are loopback/opt-in per service (e.g. `EXPORT_METRICS_ADDR`); scrape jobs
must reach them.

### Remaining gaps

- Flink processor backpressure and checkpoint failures, and DLQ size produced by
  Flink (`services/processing` emits no metrics): need the Flink metrics reporter.
- Kafka under-replicated partitions / controller quorum, ClickHouse merges and bad
  parts, PG replication lag, S3/Ceph health, failed backups: need external
  exporters (kafka, clickhouse, postgres, ceph); no rules written.
- Scrape configuration, service discovery and Alertmanager receivers are
  deployment-owned. Sink metrics need its metrics addr set; connector-manager
  runner counters are on its own /metrics.
- Alert thresholds are unvalidated initial values: rules were only syntax checked
  with promtool, not fired against real series. The e2e latency SLO (5 s) has no
  metric.
