# Release notes: v1.0.0-rc0 (кандидат, не випущений)

Цей документ описує стан гілки `staghorn` на коміті `3473f0e` і незакомічені зміни поруч. Теґу релізу немає; публікація
образів не виконувалася (`docs/operations/supply-chain.md`: release workflow ще не введений). **Production-готовність не
підтверджена** (`readiness-review.md`). Статуси нижче: *verified live* = перевірено на справжніх залежностях у dev-стеку,
*unit/fake* = лише тести з підмінами, *not verified* = немає доказу.

## Можливості за сервісами

| Компонент | Що є | Перевірено |
|---|---|---|
| `services/gateway` | `POST /v1/events[/batch]`, ідемпотентні повтори, quarantine невалідних подій, Kafka (franz-go), `GET /metrics`; AAS JWT, dev-токен | verified live: Kafka SASL_SSL і mTLS з hot reload, відмова брокера (ack лише після Kafka), реальний AAS discovery/JWKS і токен `astryon-dev-ingest`. Бенчмарк: 45 318,1 подій/с, p95 ACK 420,3 мс (`docs/benchmarks/dpp-010-2026-10-20.md`), цілі 120k і 250 мс не досягнуті |
| `services/clickhouse-sink` | `processed.events.v1` → ClickHouse, токен дедуплікації, commit після insert, quarantine/DLQ, метрики й алерти | verified live: kill -9, fault-injection, 2 шарди, 2 репліки з Keeper, Kafka TLS/SASL/mTLS, quarantine на реальному брокері; опційна intent-таблиця (0004). Sink читає `raw.events.v1` напряму, див. обмеження |
| `services/query-api` | `/v1/query/events`, `/v1/query/aggregate`, summary-ендпоінти, tenant лише з claim, ліміти (token-bucket, concurrency), опційний Redis, опційний кеш aggregate, `processing_version` за активною версією | verified live: ClickHouse 24.8, Redis single/TLS/Cluster/Sentinel failover, tenant isolation на реальному ClickHouse. Кеш aggregate — unit-tested only. Реальний токен AAS для query-api не перевірявся |
| `services/file-ingestion` | multipart-завантаження, CSV/NDJSON з resumable checkpoints, класифікатор (credentials/PII/geo), квоти, PG job store з fenced leases, список завантажень | verified live: e2e на MinIO + PG + CH. Не готово: per-source mapping, Parquet, malware scan |
| `services/export-worker` | асинхронні export-jobs (csv/ndjson), pre-signed URL, квоти, PG-диспетчер | verified live: ClickHouse + PG + MinIO, відмови PG/CH, SIGTERM, kill -9. `EXPORT_CH_HEADER_TIMEOUT` не валідований |
| `services/replay-controller` | планування replay з новим `processing_version`, dry-run, activate/rollback, читання списку, audit у Kafka | verified live: PG 16 (міграції 0001–0014, RLS), Kafka audit, e2e з реальними controller+worker+sink (500/30 000/300 000 подій). activation і aggregates у тому ж стеку не покриті |
| `services/replay-worker` | копіювання попередньої версії з ClickHouse у `processed.events.v1` з новою версією | verified live (див. вище, включно з kill -9 і lease-виправленням) |
| `services/quarantine` | перегляд, dry-run replay, публікація replay-jobs | unit/fake та виправлений запит до реального ClickHouse (коміт `9702d2a`). Журнал quarantine/DLQ не інгестується, виконання replay не реалізоване. Java-продюсер quarantine у `services/processing` не перевірений |
| `services/connector-manager` | реєстр конекторів, REST pull з cursor-after-emit, адмін-API, Debezium-CDC consumer з позиціями в PG | verified live: PG 16 (RLS), Kafka. події Debezium (snapshot, insert, update) з demo-таблиці дійшли до Kafka, CDC-парсер на них не запускався; Debezium для PostgreSQL/MySQL у проді не розгорнуто |
| `services/retry-controller` | планувальник retry, backoff/jitter, `pkg/retryclient` | smoke на dev PG; міст у file-ingestion вимкнений за замовчуванням, live не запускався |
| `services/ch-aggregator` | перерахунок агрегатів, `reconcile` (дублікати, drift, прогалини, контрольні суми), метрики для Prometheus | verified live: CH 24.8 + PG 16, одноразовий запуск у docker |
| `services/admin-bff` | OIDC Authorization Code + PKCE, серверні сесії, CSRF, проксі з bearer | fake AAS (`go test -race`); редірект на реальну сторінку входу AAS; вхід користувача не виконувався |
| `web/admin-ui` | огляд, uploads, events, quality, connectors, exports, quarantine, replays; режим BFF | vitest (11) і build; проксі до сервісів перевірений на кодах відповідей |
| `libs/event-contracts`, `libs/go-common`, `libs/sdk-*` | контракт конверта, спільна перевірка AAS JWT, introspection, DPoP-ядро, generated Go-клієнти | unit/contract; реальний AAS у `tests/aas-live` |
| `services/processing` (Java, Flink) | нормалізація `raw` → `validated/processed/quarantine` | зібраний jar, 19 юніт-тестів; джоба у міні-кластері Flink 1.20.3 (dev) обробила 1351 подію `raw` → `processed`, невалідна версія схеми потрапила в quarantine; повідомлення без валідного `tenant_id` зациклює рестарти (див. обмеження) |
| `db/`, `infra/` | PG 0001–0014, ClickHouse 0001–0006 (TTL: події 400 д, агрегати 800 д, значення за замовчуванням), dev-стек, Ansible, Kustomize/Helm | міграції застосовуються на PG 16 і CH 24.8; Kubernetes/Helm перевірені lint/kubeconform, у кластер не застосовувались; Ansible не запускався на хостах |
| Dev-стек | `make apps-up` (11 сервісів в одному контейнері, dev-токен), `make extras-up` (Redis, Kafka Connect/Debezium, окремий CDC PostgreSQL), `make aggregator-run` | запущено і перевірено на одному хості (2026-10-21) |

## Авторизація і scopes

AAS — єдине джерело identity. Реалізована scope-модель перелічена в `docs/architecture/authentication.md` і
`docs/architecture/roles.md`; клієнти `astryon-dev-ingest` і `astryon-portal`. Фактичне створення ролей у AAS ще
не виконане.

## Відомі обмеження

1. **Продуктивність:** 45,3k подій/с проти цілі 120k; p95 ACK 420 мс проти 250 мс; тест 10k подій/с також не вклався в 250 мс.
2. **Нормалізація:** Flink-джоба перевірена лише на одному міні-кластері dev (без checkpoint-відновлення, HA, навантаження); повідомлення без валідного `tenant_id` валить її в нескінченні рестарти (потрібне рішення: `events.dlq.v1` чи карантин без тенанта); sink читає `raw.events.v1` напряму. Event-time агрегація і edge-випадки Flink не перевірені.
3. **HA/DR:** немає доказів для кількох вузлів, chaos, restore і RPO/RTO.
4. **Auth:** вхід користувача через BFF не пройдений; токени AAS для всіх сервісів, крім gateway, не проганялися наскрізно; DPoP без Redis-store; сесії BFF в пам'яті.
5. **Query:** великий tenant (2,7–8 млн рядків) коштує 0,7–2,1 с на aggregate; 16 паралельних hot-tenant запитів вичерпують пам'ять вузла 6 GB (`docs/benchmarks/dpp-047-2026-10-21.md`).
6. **Replay/export:** `EXPORT_CH_HEADER_TIMEOUT` не валідований; один worker; без multi-broker.
7. **Quarantine:** журнал не інгестується, replay лише плануєтся і публікується.
8. **CDC:** Debezium перевірений на demo-таблиці; CDC-consumer не запускався на живому топіку.
9. **CI/supply chain:** не обов'язкові перевірки, немає підпису образів і release workflow.
10. **Міграції:** PG 0009–0013 належать цій гілці; перенумерування при злитті з `flut_kol/ASTRYON`.
11. **Dev-стек:** статичні токени і паролі-заглушки, лише для розробки.

## Питання сумісності

Контракт подій змінюється адитивно; міграції forward-only (перевіряє `scripts/check-migrations.sh`). Зміна імен scopes
(`quarantine:read-payload` → `quarantine-payload:read`, `quarantine:replay` → `quarantine-replay:read`,
`quarantine:replay-publish` → `quarantine-replay:write`, `connectors:manage` → `connectors:write`) ламає токени зі старими
іменами; старі імена в realm не існували.
