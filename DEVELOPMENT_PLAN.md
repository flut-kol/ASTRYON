# Distributed Data Platform — повний план реалізації

**Версія:** 1.0  
**Дата:** 2026-10-20  
**Формат:** on-premises, horizontally scalable, realtime + batch  
**Завдання:** збір, обробка, нормалізація, дедуплікація, зберігання та пошук змішаних даних  
**Цільове навантаження:** 100 млн – 1 млрд+ записів/добу (після підтвердження performance-тестами)  
**Розрахункове:** 1 млрд записів/добу ≈ 11 574 записи/с у середньому; проєктний короткочасний пік 120 000 записів/с; умовний середній розмір 1 KiB.

> Цифри — вимоги до проєктування, а не гарантована продуктивність. Для OCR, документів, великих записів і складних join потрібен окремий профіль навантаження. Не купувати повний кластер до POC і benchmark.

## 1. Архітектурні рішення (ADR)

| ID | Рішення | Пояснення |
|---|---|---|
| ADR-001 | Go для ingestion/control/query/sink сервісів | Продуктивність, ефективна конкуренція, прості двійкові релізи |
| ADR-002 | Apache Kafka (KRaft) | Надійний транспорт та replay у межах retention |
| ADR-003 | Apache Flink + Java для stateful realtime | Checkpoints, event-time, state, window processing |
| ADR-004 | Go/Python workers і Spark за потреби для batch | Важкі файли, Parquet, паралельні backfill |
| ADR-005 | ClickHouse для аналітики | OLAP, матеріалізовані представлення, TTL, шардинг |
| ADR-006 | PostgreSQL HA тільки для control plane | Tenant, джерела, конфігурація, метадані задач; не для журналу 1 млрд подій/день |
| ADR-007 | S3-compatible storage (Ceph RGW) | Незмінні сирі файли, архів, snapshots/checkpoints |
| ADR-008 | OpenSearch опціональний | Підключати лише коли запити потребують повнотекстового пошуку |
| ADR-009 | Чинний AAS Auth API для OIDC/RBAC | ASTRYON не розгортає власний Keycloak: використовує `/var/apps/astranex/aas-auth` через його HTTPS OAuth/OIDC API; сервісна авторизація і tenant enforcement лишаються на стороні ASTRYON |
| ADR-010 | Kubernetes + Helm/Argo CD | Версіоноване розгортання і масштабування сервісів |
| ADR-011 | At-least-once між різнорідними сховищами + idempotent sinks | Flink exactly-once state не означає автоматичне exactly-once для ClickHouse/OpenSearch |
| ADR-012 | Великі об'єкти тільки у S3; Kafka містить посилання | Стабільний розмір повідомлень та адекватні витрати на брокери |
| ADR-013 | CDC — Debezium/Kafka Connect | Не розробляти власний читач WAL/binlog на першій ітерації |
| ADR-014 | Data Lake / Iceberg — після PoC | Не ускладнювати перший production-мінімум без перевіреної потреби |

## 2. Цілі та SLO

- 100 млн/добу на ранньому production; цільове масштабування до 1 млрд+/добу після підтвердження на реальних даних.
- Пікові 120k подій/с для узгодженого профілю малого повідомлення; тривалий пік лише за достатньої місткості sink і backpressure.
- p95 ACK від ingestion gateway **до 250 мс у LAN** при справному Kafka та штатному навантаженні — **ціль**, а не SLA до тестування.
- p95 end-to-end до доступності в аналітиці **до 5 с** для realtime малого повідомлення в штатному режимі — ціль для benchmark.
- 99,9% місячної доступності ingestion/query API після production gate.
- Жодної втрати **підтверджених** подій при відмові одного вузла всередині допустимого failure model; Kafka `acks=all`, RF=3, min ISR=2, належний розподіл по вузлах, producer retries/idempotence.
- Відсутність необлікованих дублікатів у визначених бізнес-результатах (контрактна дедуплікація за `event_id`, `pipeline_version`, `tenant_id`; механізм перевіряється тестами).
- Окремі SLO для batch (імпорт TB/год, SLA completion залежно від даних), OCR (документів/год) та аналітичних запитів.
- RPO/RTO для відмови майданчика визначаються після вибору віддаленого backup/DR; не обіцяти RPO=0 для катастрофи цілої локації.

## 3. Логічний конвеєр

```text
REST/gRPC  ────────────────┐
CDC (Debezium) ────────────┤
External connector ────────┼──> validation -> Kafka raw.events -> Flink -> Kafka processed.events -> CH/OpenSearch/S3
Files (CSV, JSON, XML) ────┘           │               │                        │
                                      │               └──> quarantine / DLQ     └──> reconciliation
Large documents / media ──> S3 object + object reference -> batch scheduler -> workers -> Parquet/S3 or Kafka

PostgreSQL HA: tenants, sources, jobs, metadata, replay requests, quotas, audit
Query API: ClickHouse / optional OpenSearch; tenant-level access checks
Admin UI: status, sources, jobs, pipelines, quality, replay, metrics, permissions
```

*Потокові та пакетні шляхи сходяться на спільному контракті нормалізованих записів, але не змушують великі Parquet-файли робити повний обхід через Kafka.*

## 4. Монорепозиторій

```text
distributed-data-platform/
  README.md
  DEVELOPMENT_PLAN.md
  docs/
    architecture/
      context.md
      authentication.md
      sequence-ingest.md
      sequence-batch.md
      failure-model.md
      decisions/
    api/
      openapi.yaml
    events/
      schemas/
    operations/
      runbooks/
      backup-restore.md
      scaling.md
    tests/
      performance-profile.md
      acceptance.md
  services/
    gateway/
    ingestion/
    control-plane/
    connector-manager/
    file-ingestion/
    batch-orchestrator/
    clickhouse-sink/
    search-sink/
    query-api/
    retry-controller/
  flink/
    common/
    validation-job/
    normalize-job/
    dedup-job/
  workers/
    file-parser/
    batch-transform/
    ocr-adapter/
  web/
    admin-ui/
  libs/
    go-common/
    event-contracts/
  db/
    postgres/migrations/
    clickhouse/migrations/
    seeds/
  infra/
    ansible/
    terraform/
    kubernetes/
      base/
      overlays/dev/
      overlays/stage/
      overlays/prod/
    helm/
    argocd/
    kafka/
    clickhouse/
    ceph/
  tests/
    contract/
    integration/
    e2e/
    load/
    chaos/
  scripts/
    bootstrap/
    dev/
    release/
  .github/workflows/
```

## 5. Вимоги до data contracts

### 5.1 Основний Event Envelope

| Поле | Тип | Правило |
|---|---|---|
| event_id | UUID/ULID | Унікальність на рівні source/tenant; повторний submit той самий ID |
| tenant_id | UUID | Обов'язковий, із валідованого токена, не довіряти довільному заголовку |
| source_id | UUID | Зареєстроване джерело |
| schema_id/schema_version | string/int | Версіонована сумісна схема |
| event_time | timestamp UTC | Час джерела або явно позначена відсутність |
| ingested_at | timestamp UTC | Серверний час ingestion |
| entity_key | string? | Partition key, якщо застосовний; не є PII у відкритому вигляді |
| trace_id | string | Спостережуваність |
| payload_format | enum | json/protobuf/avro/parquet-reference |
| payload | bytes/object | Для малого повідомлення |
| object_ref | URI? | S3-об'єкт із контрольованого bucket для великого вмісту |
| checksum | SHA-256 | Контроль цілісності файлів |
| processing_version | int | Можливість переобчислення версій |

Контракт відслідковує зміни полів, декодування, сумісність версій, чутливі поля, обмеження розміру, коректність timestamp; raw payload за замовчуванням зберігається незмінним у data lake, де це дозволяє політика retention.

### 5.2 Початкові Kafka topics

| Topic | Призначення | Партиції на старті (гіпотеза) | Retention |
|---|---|---:|---|
| raw.events.v1 | Вхідні валідні конверти | 96 | 7 днів |
| validated.events.v1 | Після валідації | 96 | 3 дні |
| processed.events.v1 | Результат нормалізації | 96 | 3 дні |
| ingestion.filejobs.v1 | Нові файли/пакети | 24 | 7 днів |
| processing.jobstatus.v1 | Стан виконання | 24 | 7 днів |
| events.quarantine.v1 | Схеми/дані, що не проходять контракт | 24 | 14 днів |
| events.dlq.v1 | Невідновлювані помилки | 24 | 14 днів |
| audit.events.v1 | Машинні аудиторські події | 24 | 7 днів + довше у S3 |

Для критичних event topics: replication factor=3, producer acks=all, min.insync.replicas=2, producer idempotence. Для partition key: tenant_id + entity_key там, де важливий порядок за сутністю; обов'язковий тест на hot partitions і нерівномірні tenant-и. **Кількість партицій не фіксується назавжди** — затверджується після POC. Довгострокові scheduled retries — окремий scheduler із PostgreSQL/чергою станів; Kafka не надає нативної довільної затримки повідомлень.

### 5.3 Data semantics

- Подія вважається прийнятою тільки після durable ack від Kafka або завершеного commit S3-об'єкта + зареєстрованої задачі.
- `event_id` не генерується заново на повторній спробі того самого logical event.
- Dedup window та стратегія source-of-truth чітко конфігуруються; недопустимо нескінченно тримати мільярди UUID у оперативному Flink state.
- Нормалізація має зберігати посилання на оригінал, `pipeline_version` та помилки якості.
- ClickHouse sink реалізує перевірену ідемпотентність або узгоджену модель ревізій; *не можна* обіцяти, що ReplacingMergeTree миттєво прибере дублікати з усіх запитів.
- Aggregations проходять reconciliation з первинним реєстром; replay у нову `processing_version` не подвоює показники.
- PII маскується у логах; поля за потреби токенізуються або шифруються.

## 6. PostgreSQL control plane (схеми / сутності)

Обов'язкові сутності/таблиці: `tenants`, `principals`, `roles`, `role_bindings`, `api_keys` (hash only), `sources`, `source_credentials` (лише secret references), `source_schemas`, `connectors`, `connector_runs`, `pipelines`, `pipeline_versions`, `processing_rules`, `ingestion_batches`, `file_objects`, `jobs`, `job_attempts`, `scheduled_retries`, `replay_requests`, `sink_configs`, `sink_checkpoints`, `tenant_quotas`, `audit_log`, `outbox`, `notifications`.

Вимоги до кожної міграції: forward-only SQL + rollback plan, індекси, FK, tenant scope, soft deletion там, де доречно, UTC timestamptz, audit, інтеграційні тести. **Не зберігати status для кожної з мільярда подій за добу в PostgreSQL** — тільки агрегати, job/batch metadata, відхилення та окремі requested samples.

## 7. ClickHouse design

- Таблиці `events_normalized_local`, `events_normalized_distributed`, `processing_errors_local`, `processing_quality_daily`, `source_metrics_hourly`, `pipeline_metrics_hourly`, `entities_index` (за потреби).
- Місячні/денні партиції визначаються тестами; початкова гіпотеза: `PARTITION BY toYYYYMM(event_time)` для великих стабільних fact tables.
- `ORDER BY` на основі найчастіших фільтрів (`tenant_id`, `event_date`, `source_id`, `event_id`), не копіювати універсальний ключ без workload-тестів.
- Початкова модель: 2 shards × 2 replicas, Keeper quorum. Резервні копії незалежно від реплікації.
- Вставка великими пакетами, перевірка `parts`, merges, granularity та insert latency; малі insert-и агрегувати в sink.
- Розподіл shard-key за вимірюваннями skew/hot tenants; якщо в майбутньому доведеться reshuffle, передбачити міграційний інструмент.
- TTL/холодні дані: гарячі таблиці обмежені retention; сирі факти та історичні snapshot у S3.
- Модель пошуку: CH для структурованих фільтрів та агрегацій, OpenSearch лише для справді необхідного full-text.

## 8. API scope v1

### Public ingestion

- `POST /v1/events` — 1 подія, авторизоване приймання, idempotency key.
- `POST /v1/events/batch` — bounded microbatch із чіткими лімітами.
- `POST /v1/uploads` — створення multipart upload / presigned URL (короткий TTL).
- `POST /v1/uploads/{id}/complete` — перевірка checksum, розміру, формату, запуск file job.
- `GET /v1/ingestions/{id}` — aggregate status, accepted/rejected/pending counts.

### Control plane

- `GET/POST/PATCH /v1/sources` — джерела, credentials references, disable/enable.
- `GET/POST /v1/connectors` — конектори, тести з'єднання, обмеження мережі.
- `GET/POST /v1/pipelines` — правила, версии, workflow.
- `GET /v1/jobs` та `GET /v1/jobs/{id}` — стан batch/stream jobs.
- `POST /v1/jobs/{id}/cancel` — контрольована зупинка.
- `POST /v1/replays` — створити replay із обмеженим діапазоном і version.
- `GET /v1/quality/summary` — якість, quarantine, пропущені.
- `GET /v1/metrics/summary` — бізнес-метрики, не internal Prometheus metrics.

### Query & exports

- `POST /v1/query/events` — структуровані параметри фільтрації, pagination/cursor.
- `POST /v1/query/aggregate` — whitelist агрегатів, hard limits.
- `POST /v1/exports` — асинхронний export, S3 output.
- `GET /v1/exports/{id}` — стан, тимчасове посилання після перевірки авторизації.

### Стандарти API

OpenAPI 3.1; типізовані клієнти; `X-Request-ID`; одна структура помилки (`code`, `message`, `details`, `trace_id`); пагінація cursor; rate limit з 429/Retry-After; вимоги auth на кожну операцію; OpenTelemetry spans; формалізовані API versioning і deprecation policy. Не дозволяти довільний SQL з браузера.

## 9. Роботи за спринтами (по 2 тижні)

### Sprint 01 — Weeks 1–2: Discovery, ADR, workload

- **DPP-001** Скласти каталог типів джерел, SLA, форматів, розмірів подій, потреби CDC/ETL/OCR, безпеку.
- **DPP-002** Зібрати representative data generator: small JSON, wide JSON, files, skewed tenant distribution, duplicates, late events, invalid schemas.
- **DPP-003** Узгодити цілі: average, burst, retention, query concurrency, max batch object, RPO/RTO, cost envelope.
- **DPP-004** Зафіксувати ADR; визначити межі сервісів, data contract та ownership.
- **DPP-005** Створити монорепозиторій, issue templates, branch strategy, Definition of Done.
- **Gate:** репрезентативні тестові дані та підписаний measurable acceptance profile.

### Sprint 02 — Weeks 3–4: Vertical slice + benchmarks

- **DPP-006** Підняти dev Kafka, PostgreSQL, ClickHouse, локальний S3 emulator через Docker Compose.
- **DPP-007** Реалізувати мінімальний ingestion API -> Kafka.
- **DPP-008** Реалізувати idempotent processor -> ClickHouse.
- **DPP-009** Побудувати load generator з metrics: events/s, MB/s, p95 ack, lag, sink throughput, failures.
- **DPP-010** Протестувати 10k, 25k, 50k, 120k events/s для 1 KiB даних, зафіксувати bottlenecks.
- **Gate:** вимірювання визначають початкові ресурси та кількість партицій; рішення про закупівлю/розширення вузлів.

### Sprint 03 — Weeks 5–6: Platform / IaC / Security foundation

- **DPP-011** Ansible playbooks: OS, users, hardening, NTP, disks, interfaces, sysctl.
- **DPP-012** Kubernetes dev/staging; Helm charts, Argo CD GitOps, конфігураційні overlays.
- **DPP-013** CI lint, unit tests, SAST, dependency scanning, SBOM, container registry signing/provenance plan.
- **DPP-014** Secrets manager, інтеграція з чинним AAS Auth API, реєстрація ASTRYON clients/audiences/scopes, TLS і mTLS у внутрішній мережі де доречно. Контракт: `docs/architecture/authentication.md`.
- **DPP-015** Prometheus, Grafana, OTel collector, централізовані логи.
- **Gate:** один commit у main проходить тести та автоматично розгортається у dev; доступ за ролями.

### Sprint 04 — Weeks 7–8: Kafka HA / Object Store / PostgreSQL HA

- **DPP-016** Kafka KRaft: окремий controller quorum, broker placement, topics-as-code, ACL, TLS/SASL.
- **DPP-017** PostgreSQL HA (Patroni), PgBouncer, backups/WAL archiving, restore test.
- **DPP-018** Ceph RGW/S3: monitors, storage classes/pools, buckets, lifecycle, IAM, encryption.
- **DPP-019** Network tests, rack/PDU failure domains, NIC/LACP bonds, BMC, disk health.
- **DPP-020** Infrastructure runbooks, capacity alerts, dashboards.
- **Gate:** контрольоване падіння одного брокера/PG-node/object storage OSD не знищує підтверджені записи в заданій failure model.

### Sprint 05 — Weeks 9–10: Ingestion + schema management

- **DPP-021** Реалізувати multi-tenant ingestion, token auth, ACL, request/body limits, quotas.
- **DPP-022** Event envelope library з Protobuf/Avro/JSON schemas і сумісністю версій.
- **DPP-023** Batch HTTP endpoint з partial failures та idempotency.
- **DPP-024** Schema validation, quarantine topic, diagnostic error codes.
- **DPP-025** Integration tests: retry, duplicate submit, broker failover, malformed payload, unauthorized tenant.
- **Gate:** publish/consume контракт стабільний, documented OpenAPI, idempotency обґрунтовано.

### Sprint 06 — Weeks 11–12: Basic processing / CH storage / MVP

- **DPP-026** Flink job validation + normalize, checkpoints до S3.
- **DPP-027** Basic dedup для bounded key/window, вимірювання state growth.
- **DPP-028** ClickHouse schemas, distributed tables, batched writer, reconciliation.
- **DPP-029** Query API: tenant-filtered search, basic aggregation, cursor pagination.
- **DPP-030** Basic Admin UI: ingestion counters, jobs, sources, health.
- **Gate: MVP:** ingest -> process -> store -> query; rollback and replay test; перший performance report.

### Sprint 07 — Weeks 13–14: File ingestion / batch foundation

- **DPP-031** Presigned multipart uploads, checksum, malware/content checks where applicable, lifecycle.
- **DPP-032** Streaming CSV/NDJSON/Parquet loaders; bounded-memory parser; quarantine partial failures.
- **DPP-033** Batch job state machine: NEW, QUEUED, RUNNING, RETRYING, SUCCEEDED, FAILED, CANCELLED.
- **DPP-034** Distributed workers із leases/heartbeats, work stealing або queue-based partitioning.
- **DPP-035** Large batch -> normalized Parquet S3 / ClickHouse bulk ingest, із lineage; avoid forcing files via Kafka.
- **Gate:** restart посеред великого файлу не створює неконтрольованих дублікатів, є прогрес і resume.

### Sprint 08 — Weeks 15–16: Connectors + CDC

- **DPP-036** Connector registry, service configs, health probes, credentials via secret refs.
- **DPP-037** PostgreSQL and MySQL CDC через Debezium/Kafka Connect.
- **DPP-038** REST pull connector з rate limiting, checkpoints, incremental cursor, backoff.
- **DPP-039** Синхронізація snapshot + tail CDC; schema evolution.
- **DPP-040** UI + API керування конекторами, connection testing і контроль доступу.
- **Gate:** restart/failover CDC зберігає позицію та контрольовано перевідтворює події.

### Sprint 09 — Weeks 17–18: Reliability + replay

- **DPP-041** Long retry scheduler, backoff/jitter, max attempts, error classification.
- **DPP-042** Quarantine/DLQ browser і replay із явним scope, permission та dry-run.
- **DPP-043** Versioned pipelines і replay без подвоєння бізнес-агрегатів.
- **DPP-044** Reconciliation: counts/checksums per source/partition/date, gaps, drift detection.
- **DPP-045** Data quality counters, incident alerts, job audit.
- **Gate:** помилкове повідомлення не блокує партицію назавжди; є контрольований replay/repair.

### Sprint 10 — Weeks 19–20: Search and query optimization

- **DPP-046** Оптимізувати ClickHouse partitioning, sorting, TTL, materialized views.
- **DPP-047** Benchmark query profiles з різними tenant distributions, concurrent requests.
- **DPP-048** Якщо виправдано: OpenSearch і sink + index rollover/retention.
- **DPP-049** Export jobs з rate limits, audit, quota, signed S3 downloads.
- **DPP-050** Tenant-scoped query ACL, query timeouts, memory/row caps, cache strategies.
- **Gate:** p95 API і query SLO на реальних даних; heavy tenants не зупиняють інших.

### Sprint 11 — Weeks 21–22: Admin platform and hardening

- **DPP-051** Фінальна Admin UI: overview, connectors, pipelines, batches, errors, replays, sinks.
- **DPP-052** Ролі: platform-admin, tenant-admin, operator, analyst, auditor, service-account.
- **DPP-053** Audit actions and security events, 2FA via IdP, session policies.
- **DPP-054** Documentation, OpenAPI SDK generation, error catalog, onboarding guides.
- **DPP-055** Accessibility, UI e2e, operational dashboards.
- **Gate:** повний цикл може виконати оператор із мінімально необхідними правами.

### Sprint 12 — Weeks 23–24: Load, saturation, observability

- **DPP-056** Load tests на 100 млн/день еквівалент + 1 млрд/день еквівалент.
- **DPP-057** Short burst 120k events/s з backlog drain, backpressure.
- **DPP-058** Disk/CPU/RAM/network saturation і skew/hot keys.
- **DPP-059** Benchmark query+ingestion одночасно, verify isolation.
- **DPP-060** Tuning: Kafka partitions, brokers, producer batching, Flink parallelism/checkpoints, CH inserts/parts.
- **Gate:** performance report, максимальна стабільна швидкість, capacity curve, дії при деградації.

### Sprint 13 — Weeks 25–26: Chaos / backups / DR

- **DPP-061** Kill broker / controller / Flink TaskManager / CH replica / PG primary / Ceph OSD tests.
- **DPP-062** Відновити backups PostgreSQL, ClickHouse та критичних S3 метаданих у чистому середовищі.
- **DPP-063** Checkpoint+replay correctness testing із reconciliation.
- **DPP-064** Процедури maintenance: rolling upgrade, reshard/rebalance, secret rotation.
- **DPP-065** Підтвердити disaster recovery план, фактичні RTO/RPO.
- **Gate:** усі критичні failover/restore runbooks виконані та підписані.

### Sprint 14 — Weeks 27–28: Production rollout

- **DPP-066** Production readiness review: security, ops, cost, support, observability.
- **DPP-067** Canary traffic 1% -> 10% -> 50% -> 100% за наперед заданими умовами переходу.
- **DPP-068** Rollback strategy, feature flags, schema compatibility gates.
- **DPP-069** On-call rotation, incident severity, escalation, SLO/error budget.
- **DPP-070** Release notes, документація handover, operational acceptance.
- **Gate:** production приймає та обробляє реальні дані з погодженими SLO, а не лише synthetic peak.

### Sprint 15 — Weeks 29–30: Operational stabilization

- **DPP-071** Проаналізувати профілі реального навантаження, переглянути capacity model.
- **DPP-072** Оптимізувати cost per million events і storage lifecycle.
- **DPP-073** Bug fixing і стабільність після rollout.
- **DPP-074** Спланувати v1.1: advanced full-text, lakehouse/Iceberg, AI/OCR workers, self-service pipeline builder.
- **Gate:** фінальний звіт про throughput, failures, data quality, SLO, витрати та backlog.

## 10. Матриця залежностей

```text
ADR + workload / samples
   ├──> Event Envelope + schemas ──> Ingestion ──> Processing ──> CH sink ──> Query API
   ├──> Hardware sizing ────────────> Kafka/Ceph/CH cluster ──> Performance
   ├──> Security design ────────────> IAM + tenant enforcement ──> Admin UI
   └──> S3 contracts ───────────────> File ingestion ──> Batch workers

Kafka -> Flink state/checkpoints -> replay -> reconciliation
PostgreSQL -> control plane -> connector orchestration -> admin workflows
Observability + backups + failover tests -> production readiness gate
```

## 11. Тестування та acceptance criteria

### Unit / Contract / Integration

- Go: unit tests для envelope, quotas, idempotency, HTTP/gRPC APIs, Kafka producer retries.
- Flink: event-time windows, state snapshot/restore, out-of-order arrivals, duplicated events, late data, schema transitions.
- Contracts: backward/forward rules, Protobuf/Avro schema compatibility, generated client tests.
- Integration (Testcontainers або інтеграційне середовище): Kafka, ClickHouse, PG, S3 emulator, чинний AAS Auth API (без підняття другого Keycloak у ASTRYON), connector CDC.
- Security: tenant isolation, RBAC, IDOR, SSRF у джерелах, secret exposure, SQL injection, unsafe file uploads.

### Load tests (обов'язково окремо)

| Test ID | Профіль | Умови приймання |
|---|---|---|
| PERF-01 | 10k events/s, 1 KiB, 60 хв | без втрат confirmed events; метрики, стабільний lag |
| PERF-02 | 25k events/s, 1 KiB, 60 хв | без втрат, ресурси не виснажуються |
| PERF-03 | 50k events/s, 1 KiB, 30 хв | latency, sink throughput, lag у виміряних межах |
| PERF-04 | 120k events/s, 1 KiB, 10 хв | приймання піку в заданій конфігурації; backlog прогнозовано зникає |
| PERF-05 | 1 млрд/добу еквівалент, 24 год | стабільна обробка реалістичного міксу; немає невирішених data gaps |
| PERF-06 | Skew: tenant X 80% traffic | ізоляція, без uncontrolled hot partition bottlenecks |
| PERF-07 | 1+ TB batch backfill | bounded memory, restart/retry, контроль дублювання |
| PERF-08 | Одночасно 20/100/500 query clients | p95 latency і query budgets задокументовані |

На всіх тестах збирати: producer throughput, Kafka lag, ISR, disk latency, Flink backpressure/checkpoint duration, sink insert rate, ClickHouse merges/parts, query p95/p99, CPU/RAM/IO, S3 throughput, error rate, total data reconciliation.

### Chaos і DR

- Відмова 1 Kafka broker при потоці; перевірка confirmed ack, leader election, replay.
- Відмова 1 Kafka controller, але quorum лишається.
- Відмова Flink worker під час stateful processing; checkpoint restore.
- Відмова CH replica, перевірка query routing та read consistency.
- Відмова primary PG, автоматичне перемикання без split brain.
- Відмова OSD/host Ceph, recovery та перевірка bucket accessibility.
- Невалідний schema migration — rollback без corrupt event stream.
- Повний тест відновлення в чистому середовищі із backup, а не лише перевірка наявності файлів backup.

## 12. Security / privacy checklist

- KYC не передбачається системою без бізнес-вимоги; для користувачів є OIDC та MFA.
- Єдине джерело identity — чинний AAS Auth API; ASTRYON не читає його PostgreSQL/Keycloak напряму й не дублює користувачів або паролі.
- JWT/OIDC tokens із чіткою audience, issuer, expiry, scope; server-side authz кожного endpoint.
- Discovery/JWKS беруться з AAS Auth API; `iss`, `aud`, `azp`, `env`, `typ`, `exp`, `nbf`, `iat`, `jti`, token binding і scopes перевіряються fail-closed. Недоступність auth не дозволяє нові входи/токени, але локальна перевірка вже виданого JWT працює до `exp` із контрольованим JWKS cache.
- Tenant ID встановлює trusted auth middleware; не приймати його довільним параметром без перевірки.
- Secrets тільки у vault/secret manager; ротація, не записувати у логи/репозиторій.
- Kafka ACL; TLS на транспорті; ізоляція кластерної мережі; RBAC для control-plane.
- Files: allowlist типів, захист від zip bombs, обмеження размера/часу, scanning за політикою.
- Connectors: захист від SSRF, egress allowlists, без передачі внутрішніх токенів назовні.
- S3 presigned URLs короткоживучі, scoped на об'єкт, checksum/tenant контроль.
- Маскування PII, терміни зберігання, audit, документоване видалення в усіх репліках і похідних наборах.
- Залежності: SCA, image scan, SBOM, secrets scan, incident response, регулярні обновлення.

## 13. Observability / alerts

**Dashboards:** overview, ingest, Kafka, Flink, batch jobs, CH, storage S3/Ceph, PG, query API, tenant quotas, quality, failed events.

**Alerts:** ack errors, Kafka under-replicated partitions, controller quorum issues, consumer lag growth, processor backpressure, checkpoint failures, DLQ rate, CH disks >70/80%, merges backlog, bad parts, PG replication lag, S3 error rate, Ceph degraded pools, failed backups, query p95, auth failures, expiration certs.

**Incident runbooks:** stuck consumer, Kafka partition skew, S3 errors, corrupted uploads, giant batch, CH overloaded merge pool, PG failover, Flink restore, replay, tenant offboarding, secret compromise.

## 14. Ролі і команда

| Роль | Потреба | Основні задачі |
|---|---:|---|
| Architect / Tech Lead | 1 | ADR, contracts, code reviews, acceptance |
| Backend Go | 2 | API, ingestion, control-plane, sinks, query |
| Data Engineer (Flink/Kafka) | 2 | Stateful jobs, CDC, replay, tuning |
| SRE / DevOps | 1–2 | Kubernetes, IaC, storage, Kafka/CH HA, observability |
| Frontend | 1 | Admin UI |
| QA / Performance | 1 | Contract/e2e/load/chaos, release gates |
| Security | shared | Threat model, IAM/SSRF/secrets, penetration test |

AI-агент може генерувати сервіси, тести, migrations, Helm, документи й runbooks, але production-перевірки безпеки, failover, performance та hardware sizing мають пройти незалежний review.

## 15. Definition of Done для кожної задачі

- Код у репозиторії; рев'ю пройдено; build і lint без помилок.
- Unit + інтеграційні тести включено до CI; contract tests де змінюється взаємодія.
- Міграції та rollback/recovery plan, якщо зміна торкається стану/сховища.
- OpenAPI/event schema актуальні; змінено version і сумісність перевірено.
- Метрики, логи, traces, алерти й health endpoints додані.
- Tenant isolation та authorizations покриті security тестами.
- Підтримуються timeout, cancellation, backpressure, idempotent retry.
- Додано runbook або виправлено існуючий.
- Пройдено acceptance тест зі спринту; реліз можна відкотити.

## 16. Шаблон задачі для AI-агента

```text
Ти senior engineer проєкту Distributed Data Platform. Реалізуй ТІЛЬКИ задачу <DPP-ID> відповідно до DEVELOPMENT_PLAN.md.

Правила:
1. Ознайомся з архітектурними рішеннями, актуальними контрактами, схемами, існуючим кодом та тестами.
2. Не ламай API/схеми без explicit versioning та backward compatibility plan.
3. Надай завершену реалізацію: повні файли й конфігурації, не уривки.
4. Усе, що сприймає мережеві дані, перевіряє tenant, формат, обмеження й права доступу.
5. Усі операції, де можливий повтор, мають retry/idempotence і тести на це.
6. Не використовуй PostgreSQL як per-event журнал для мільярдів записів.
7. Додай unit/integration/contract tests та онови документацію.
8. Не хардкодь ключі, паролі, токени, production endpoints.
9. Після реалізації запусти доступні lint, test, build, report actual results.
10. Для auth використовуй контракт `docs/architecture/authentication.md` і чинний AAS Auth API; не копіюй auth-сервіс у репозиторій і не вигадуй другий identity API. Для інших невиконаних залежностей створи адаптер/інтерфейс.

Поверни:
- список змінених файлів;
- короткий опис архітектурного рішення;
- команди запуску локально та тести;
- підтверджені результати тестів;
- known limitations;
- наступну залежну задачу.
```

## 17. Що НЕ робити у першому релізі

- Не писати власні Kafka, CDC чи distributed KV engine.
- Не впроваджувати одночасно Kafka+Pulsar+RabbitMQ без окремої технічної вимоги.
- Не класти всі payload у PostgreSQL.
- Не використовувати Ceph HDD як базові диски Kafka/ClickHouse.
- Не намагатися зробити end-to-end exactly-once декларативно без тесту sink semantics.
- Не вводити OpenSearch і Iceberg без підтверджених сценаріїв використання.
- Не купувати фіксовані 22 сервери без реального тесту міксу даних.
- Не змішувати tenants без перевірки authorization у query, exports, batch і replays.

## 18. Релізні рівні

- **Lab / PoC:** тиждень 4. Підтверджена технологічна можливість і bottleneck map.
- **MVP:** тиждень 12. Ingestion, normalize, CH, query, базове UI, авторизація.
- **Beta:** тиждень 18–22. CDC, batch, replay, full admin, telemetry.
- **Production Candidate:** тиждень 26. Чітко виміряні performance та resilience.
- **Production:** тиждень 28–30. Canary rollout + операційний супровід.

## 19. Джерела для інженерної перевірки

- Apache Kafka documentation: https://kafka.apache.org/documentation/
- Apache Flink fault tolerance: https://nightlies.apache.org/flink/flink-docs-stable/docs/learn-flink/fault_tolerance/
- Apache Flink source/sink guarantees: https://nightlies.apache.org/flink/flink-docs-stable/docs/connectors/datastream/guarantees/
- Ceph hardware recommendations: https://docs.ceph.com/en/tentacle/start/hardware-recommendations/
- Kafka Connect integration guidance: https://docs.confluent.io/platform/current/connect/userguide.html

**Наступний крок після затвердження плану:** почати з DPP-001–DPP-005, створити репозиторій та перший PoC зі згенерованими даними, зафіксувати базову performance-таблицю.
