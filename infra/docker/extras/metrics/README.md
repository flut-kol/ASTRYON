# Dev metrics endpoints (optional, DEV ONLY)

Nothing here is started by default. All ports bind to 127.0.0.1; credentials come from the environment (the same
`POSTGRES_*` variables as the dev stack, placeholder defaults), nothing is stored in the repo. There is no Prometheus server in
the dev compose; point your own at these targets.

## Flink processing job (profile `processing`)

The Flink image enables the bundled `flink-metrics-prometheus` plugin (`extras/flink/entrypoint.sh`). The mini cluster runs the
JobManager and TaskManager in one container, so the reporter uses a 2-port range: JobManager `9249`, TaskManager `9250`.

| Env (compose) | Default | Meaning |
|---|---|---|
| `FLINK_METRICS_PORT` | `9249-9250` | reporter port range inside the container (keep it 2 ports wide) |
| `FLINK_METRICS_JM_HOST_PORT` | `19249` | host port of the JobManager endpoint |
| `FLINK_METRICS_TM_HOST_PORT` | `19250` | host port of the TaskManager endpoint (job/operator metrics are here) |

Custom job counters (operator scope, exported as `flink_taskmanager_job_task_operator_<name>`):

| Metric | Operator | Meaning |
|---|---|---|
| `records_validated_total` | validate-normalize | envelopes that passed validation |
| `records_processed_total` | bounded-dedup | validated records that survived dedup and were emitted to `processed.events.v1` |
| `records_quarantined_total{reason}` | validate-normalize | quarantined records; `reason` is an `ERR_*` code from a fixed allow-list, anything else is `OTHER` (no tenant/source/event labels) |

The existing `dedup_unique_total`, `dedup_dropped_total`, `dedup_expirations_total` are exported the same way.

```bash
docker compose -f infra/docker/docker-compose.extras.yml --profile processing up -d --build processing
curl -s http://127.0.0.1:19250/metrics | grep -E 'records_(validated|processed|quarantined)_total'
```

A counter appears only after its first increment in that task (the quarantine counter once per reason).

## Kafka and PostgreSQL exporters (profile `metrics`)

```bash
docker compose -f infra/docker/docker-compose.extras.yml --profile metrics up -d kafka_exporter postgres_exporter
curl -s http://127.0.0.1:19308/metrics | head   # kafka_exporter  (KAFKA_EXPORTER_HOST_PORT)
curl -s http://127.0.0.1:19187/metrics | head   # postgres_exporter (POSTGRES_EXPORTER_HOST_PORT)
```

`postgres_exporter` connects to the dev `postgres` service with `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB`.
Outside dev use a dedicated read-only role (`pg_monitor`) and a secret store, not the application role.

## ClickHouse built-in `/metrics`

Merge the override into the dev stack (recreates the `clickhouse` service with a mounted `config.d` snippet and one extra port):

```bash
docker compose -f infra/docker/docker-compose.dev.yml -f infra/docker/extras/metrics/clickhouse-metrics.override.yml up -d clickhouse
curl -s http://127.0.0.1:19363/metrics | head   # CLICKHOUSE_METRICS_HOST_PORT
```

Re-run `scripts/dev/up.sh` (without the override) to return to the default.

## Verification status

Verified: `docker compose config` for all files; Flink image builds and, in a stand-alone container, serves
Prometheus text on 9249 (JobManager) and 9250 (TaskManager). Not verified live: the custom counters against a running job
with Kafka traffic, the exporters and the ClickHouse endpoint (images not started). Unit tests (`ProcessingMetricsTest`) cover counter
naming, the reason label and cardinality bounding.
