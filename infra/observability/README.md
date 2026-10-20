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
