# Handover (DPP-070)

Документ для приймання експлуатації. Власників компонентів у репозиторії не названо, тому в таблиці стоїть `TODO`.
Значення секретів тут ніколи не наводяться, лише імена змінних.

## Власники компонентів

| Компонент | Власник | Runbook | Примітка |
|---|---|---|---|
| gateway | TODO | `docs/operations/runbooks/ingestion-failures.md` | алерт `AstryonGatewayAckErrors` |
| clickhouse-sink | TODO | `runbooks/stuck-consumer.md` | алерти sink + consumer lag |
| query-api | TODO | немає окремого | алерти `AstryonQueryBackendErrors`, `AstryonQueryThrottling` |
| file-ingestion | TODO | немає окремого | алерти `AstryonFileJobsFailing` та ін. |
| export-worker | TODO | немає окремого | |
| replay-controller / replay-worker | TODO | `runbooks/replay.md` | |
| quarantine | TODO | немає окремого | |
| connector-manager | TODO | немає окремого | |
| retry-controller | TODO | `runbooks/retry-controller.md` | |
| ch-aggregator | TODO | немає окремого | алерти `AstryonAggregatorStale`, `AstryonReconcileDrift` |
| admin-bff / admin-ui | TODO | немає окремого | |
| Kafka, PostgreSQL, ClickHouse, S3 | TODO | **відсутні** (перелік у `runbooks/README.md`) | |
| AAS (зовнішній) | власник AAS (`/var/apps/astranex/aas-auth`) | `docs/architecture/authentication.md` | конфіг клієнтів і scopes належить йому |
| On-call, ескалація | TODO | `docs/operations/incident-management.md` | розкладу немає |

## Запуск dev-стека

Потрібні Docker і Go 1.27. Усе нижче лише для розробки: статичні токени й паролі-заглушки.

```
make apps-up        # Kafka, PostgreSQL, ClickHouse, S3 mock + 11 сервісів в одному контейнері astryon_apps (dev-токен у .astryon-dev/)
make apps-logs      # логи контейнера застосунків
make apps-down
make extras-up      # Redis, Kafka Connect + Debezium, окремий PostgreSQL для CDC (infra/docker/EXTRAS.md)
make extras-down
make aggregator-run # одноразовий запуск ch-aggregator; ARGS=reconcile для звірки
make test           # має проходити перед здачею будь-якої зміни
```

Порти на 127.0.0.1: gateway 18080, query-api 18082, export-worker 18083, file-api 18085, quarantine 18086,
replay-controller 18087, connector-manager 18088, Kafka 19092, PostgreSQL 15432, ClickHouse 18124/19010, S3 mock 19200.
Деталі: `infra/docker/apps/README.md`. Режим AAS вмикається змінними `AAS_BASE_URL`, `AAS_ISSUER`, `AAS_AUDIENCE`,
`AAS_ALLOWED_AZP`, `AAS_ENV` (`prod` для живого realm); тоді статичний dev-токен перестає діяти.

## Де що лежить

- Індекс і статус: `PROJECT_INDEX.md`; план: `DEVELOPMENT_PLAN.md` (за розділами).
- Архітектура: `docs/architecture/` (`authentication.md`, `roles.md`, `failure-model.md`, `dpop-plan.md`).
- API: `docs/api/*.openapi.yaml`; помилки: `docs/errors/`; SDK: `docs/sdk/`, `libs/sdk-*`; онбординг джерел: `docs/onboarding/`.
- Операції: `docs/operations/` (rollback, incident-management, supply-chain, TLS/SASL-тест), runbooks у `docs/operations/runbooks/`.
- Перевірки: `docs/tests/` (acceptance, live-verification-checklist, sink-crash-restart, e2e-smoke), звіти `docs/benchmarks/`.
- Міграції: `db/postgres/migrations/` (0001–0014), `db/clickhouse/migrations/` (0001–0006); gate: `scripts/check-migrations.sh`.
- Спостережуваність: `infra/observability/` (правила Prometheus, дашборд, collector).
- Інфраструктура: `infra/docker`, `infra/kubernetes`, `infra/helm`, `infra/ansible`, `infra/kafka`; жодне не застосовувалося до справжнього кластера чи хостів.

## Інвентар змінних середовища (лише імена)

Секретні за змістом змінні позначені `(секрет)`; їхні значення живуть у secret manager або змонтованих файлах, не в репозиторії.

**Спільні для AAS-сервісів** (gateway, query-api, file-ingestion, export-worker, replay-controller, quarantine; у connector-manager
з префіксом `ASTRYON_`): `AAS_BASE_URL`, `AAS_ISSUER`, `AAS_AUDIENCE`, `AAS_ENV`, `AAS_ALLOWED_AZP`, `AAS_TOKEN_TYPE`;
DPoP: `AAS_PUBLIC_BASE_URL`, `AAS_REQUIRE_DPOP`, `AAS_REDIS_URL` (секрет), `AAS_REDIS_CA_FILE`; introspection (query-api):
`AAS_INTROSPECTION_CLIENT_ID`, `AAS_INTROSPECTION_KEY_FILE` (секрет), `AAS_INTROSPECTION_KEY_ID`.

**Kafka-клієнт** (gateway, replay-controller, replay-worker, quarantine, connector-manager): `ASTRYON_KAFKA_BROKERS`,
`ASTRYON_KAFKA_CLIENT_ID`, `ASTRYON_KAFKA_DELIVERY_TIMEOUT`, `ASTRYON_KAFKA_TLS`, `ASTRYON_KAFKA_TLS_CA_FILE`,
`ASTRYON_KAFKA_TLS_CERT_FILE`, `ASTRYON_KAFKA_TLS_KEY_FILE` (секрет), `ASTRYON_KAFKA_TLS_RELOAD_INTERVAL`,
`ASTRYON_KAFKA_SASL_MECHANISM`, `ASTRYON_KAFKA_SASL_USERNAME`, `ASTRYON_KAFKA_SASL_PASSWORD` (секрет).

**ClickHouse** (query-api, file-ingestion, export-worker, replay-controller, quarantine, ch-aggregator): `CH_URL`, `CH_DATABASE`,
`CH_USER`, `CH_PASSWORD` (секрет); sink: `CHSINK_CH_URL`, `CHSINK_CH_DATABASE`, `CHSINK_CH_USER`, `CHSINK_CH_PASSWORD` (секрет);
replay-worker: `REPLAYW_CH_URL`, `REPLAYW_CH_DATABASE`, `REPLAYW_CH_USER`, `REPLAYW_CH_PASSWORD` (секрет), `REPLAYW_CH_TABLE`.

**gateway:** `ASTRYON_GATEWAY_ADDR`; DEV: `ASTRYON_DEV_AUTH`, `ASTRYON_DEV_TOKEN` (секрет), `ASTRYON_DEV_TENANT_ID`, `ASTRYON_DEV_SCOPES`, `ASTRYON_DEV_FAKE_PUBLISHER`.

**clickhouse-sink:** `CHSINK_KAFKA_BROKERS`, `CHSINK_TOPIC`, `CHSINK_GROUP_ID`, `CHSINK_CH_TABLE`, `CHSINK_INTENT_TABLE`, `CHSINK_INTENT_WAIT`,
`CHSINK_BATCH_SIZE`, `CHSINK_BATCH_INTERVAL`, `CHSINK_MAX_RETRIES`, `CHSINK_RETRY_BACKOFF`, `CHSINK_RETRY_BACKOFF_MAX`,
`CHSINK_QUARANTINE_TOPIC`, `CHSINK_QUARANTINE_TIMEOUT`, `CHSINK_SHUTDOWN_TIMEOUT`, `CHSINK_METRICS_ADDR`,
`CHSINK_KAFKA_TLS`, `CHSINK_KAFKA_TLS_CA_FILE`, `CHSINK_KAFKA_TLS_CERT_FILE`, `CHSINK_KAFKA_TLS_KEY_FILE` (секрет),
`CHSINK_KAFKA_TLS_RELOAD_INTERVAL`, `CHSINK_KAFKA_SASL_MECHANISM`, `CHSINK_KAFKA_SASL_USERNAME`, `CHSINK_KAFKA_SASL_PASSWORD` (секрет).

**query-api:** `LISTEN_ADDR`, `QUERY_PG_URL` (секрет, містить пароль), `QUERY_VERSION_CACHE_SECONDS`, `QUERY_AGG_CACHE_SECONDS`,
`QUERY_METRICS_ADDR`; DEV: `QUERY_DEV_TOKEN` (секрет), `QUERY_DEV_TENANT`. Redis і ліміти (`QUERY_REDIS_*`, `QUERY_*` ліміти) описані в `services/query-api/README.md`.

**file-ingestion:** `LISTEN_ADDR`, `FILEJOBS_PG_URL`, `FILEOBJ_PG_URL`, `FILEQUOTA_PG_URL` (усі секрет), `FILES3_ENDPOINT`, `FILES3_BUCKET`,
`FILES3_ACCESS_KEY`, `FILES3_SECRET_KEY` (секрет), `FILE_S3_REGION`, `FILE_CONTENT_TYPES`, `FILE_CREDENTIALS_POLICY`,
`FILE_CREDENTIALS_POLICY_OVERRIDES`, `FILE_ENTITY_ID_FIELD`, `FILE_EVENT_TIME_FIELD`, `FILE_ERRORS_TABLE`, `FILE_EVENTS_TABLE`,
`FILE_RETRY_MODE`, `FILE_WORKER_ID`, `FILE_METRICS_ADDR`; DEV: `FILE_API_DEV_TOKEN` (секрет), `FILE_API_DEV_TENANT`.

**export-worker:** `LISTEN_ADDR`, `EXPORT_PG_URL` (секрет), `EXPORT_S3_ENDPOINT`, `EXPORT_S3_ACCESS_KEY`, `EXPORT_S3_SECRET_KEY` (секрет),
`S3_ENDPOINT`, `S3_BUCKET`, `S3_REGION`, `S3_ACCESS_KEY`, `S3_SECRET_KEY` (секрет), `EXPORT_CH_URL`, `EXPORT_CH_DB`, `EXPORT_CH_USER`,
`EXPORT_CH_PASSWORD` (секрет), `EXPORT_CH_HEADER_TIMEOUT`, `EXPORT_CH_IDLE_TIMEOUT`, `EXPORT_CH_MAX_DURATION`, `EXPORT_FINISH_RETRY`,
`EXPORT_LEASE_SECONDS`, `EXPORT_MAX_CONCURRENT_PER_TENANT`, `EXPORT_MAX_PER_DAY`, `EXPORT_SWEEP_SECONDS`, `EXPORT_SWEEP_GRACE_SECONDS`,
`EXPORT_URL_TTL_SECONDS`, `EXPORT_WORKERS`, `EXPORT_WORKER_ID`, `EXPORT_PPROF_ADDR`; DEV: `EXPORT_DEV_TOKEN` (секрет), `EXPORT_DEV_TENANT`.

**replay-controller:** `REPLAY_ADDR`, `REPLAY_PG_URL` (секрет), `REPLAY_PG_ADMIN_URL` (секрет), `REPLAY_AUDIT_MODE`, `REPLAY_AUDIT_TIMEOUT`,
`REPLAY_RECONCILE_GROUP`; DEV: `REPLAY_DEV_TOKEN` (секрет), `REPLAY_DEV_TENANT_ID`, `REPLAY_DEV_SCOPES`.

**replay-worker:** `REPLAYW_PG_URL` (секрет), `REPLAYW_GROUP_ID`, `REPLAYW_JOBS_TOPIC`, `REPLAYW_OUT_TOPIC`, `REPLAYW_PAGE_SIZE`, `REPLAYW_RUNNING_LEASE`.

**quarantine:** `LISTEN_ADDR`, `QUARANTINE_CH_TABLE`, `QUARANTINE_REPLAY_TOPIC`; DEV: `QUARANTINE_ENABLE_DEV_AUTH`, `QUARANTINE_DEV_TOKEN` (секрет), `QUARANTINE_DEV_TENANT`.

**connector-manager:** `ASTRYON_DATABASE_URL` (секрет), `CONNPULL_PG_URL` (секрет), `CDC_POS_PG_URL` (секрет), `ASTRYON_CONNECTOR_ADMIN_ADDR`,
`ASTRYON_CONNECTOR_METRICS_ADDR`, `ASTRYON_CONNECTOR_SYNC_INTERVAL`, `ASTRYON_WORKER_ID`, `ASTRYON_CDC_TOPIC`, `ASTRYON_CDC_GROUP`,
`ASTRYON_CDC_CONNECTOR_ID`, `ASTRYON_CDC_SOURCE_ID`, `ASTRYON_CDC_TENANT_ID`, `ASTRYON_CDC_SCHEMA_ID`, `ASTRYON_CDC_PRIMARY_KEY`;
SecretRef конекторів: `ASTRYON_SECRET_<NAME>` або `ASTRYON_SECRET_<NAME>_FILE` (секрет); DEV: `ASTRYON_CONNECTOR_DEV_TOKEN` (секрет), `ASTRYON_CONNECTOR_DEV_TENANT`.

**retry-controller:** `DATABASE_URL` (секрет), `ASTRYON_RETRY_DISPATCH_TOPIC`, `ASTRYON_RETRY_METRICS_ADDR`.

**ch-aggregator:** `REGISTRY_PG_URL` (секрет), `AGG_FAIL_ON_GAPS`, `AGG_METRICS_FILE`, `AGG_RECONCILE_METRICS_FILE`.

**admin-bff:** `AAS_BASE_URL`, `BFF_ADDR`, `BFF_PUBLIC_ORIGIN`, `BFF_AFTER_LOGIN_URL`, `BFF_COOKIE_INSECURE` (лише dev, http), `BFF_CLIENT_ID`,
`BFF_CLIENT_KEY_FILE` (секрет), `BFF_CLIENT_KEY_ID`, `BFF_CLIENT_SECRET` (секрет, лише dev), `BFF_SCOPES`, `BFF_SESSION_MAX_AGE`, `BFF_UPSTREAMS`.

Змінні з `_IT_`, `EXPORT_E2E*`, `EXPORT_LIVE`, `FILEINGEST_E2E`, `FILE*_LIVE` — лише для env-gated інтеграційних тестів.
Список зібраний із коду `cmd/` і `internal/` сервісів; повні описи й значення за замовчуванням у README відповідного сервісу.

## Що передається незавершеним

Див. «Відомі прогалини» у `readiness-review.md` і «Відомі обмеження» у `release-notes-v1.0.0-rc0.md`. Найважливіше: Flink-нормалізація перевірена лише на одному dev-кластері (повідомлення без `tenant_id` зациклює рестарти),
продуктивність нижча за ціль, HA/DR/chaos не перевірені, canary-умови (DPP-067) не визначені, вхід користувача через BFF не пройдений.
