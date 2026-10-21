# Production readiness review (DPP-066)

Стан репозиторію: гілка `staghorn`, останній коміт на момент написання `3473f0e`. Усі твердження нижче спираються на файли
репозиторію; де доказу немає, так і написано. Нових вимірювань цей документ не містить.

## Висновок: NO-GO

**Готовність до production не підтверджена.** Жоден із критеріїв виходу §18 (Production Candidate / Production) не має
доказу: немає HA/multi-node результатів, немає chaos-, restore- і DR-навчань, ціль продуктивності не досягнута, а
правила canary (DPP-067) не визначені. Рішення про розгортання з реальними даними приймати не на основі цього документа.

## Критерії

Статуси: **met** (доказ є), **partially met** (частково, межі вказані), **not met** (вимога перевірена і не виконана),
**not evidenced** (доказів у репозиторії немає).

| Критерій | Статус | Доказ або чого бракує |
|---|---|---|
| Security: SAST/SCA/secrets/SBOM | partially met | Визначено в `.github/workflows/ci.yml` і `security.yml`, опис у `docs/operations/supply-chain.md`; там же сказано, що без required status check на `main` це лише evidence, а не gate. Незмінні SHA для actions, підпис образів, registry не налаштовані. gosec чистий по модулях (коміт `e74897a`). |
| Security: tenant isolation | partially met | tenant береться лише з claim (правило проєкту). Тести: `services/query-api` `TestIntegrationTenantIsolationRealClickHouse` (потрібен `QUERY_CH_IT_URL`), `tests/aas-live/tenant_isolation_test.go` (через generated SDK зі stub-ClickHouse), RLS-тести replay/connector-manager на PG16. Наскрізного тесту всіх endpoint-ів немає (`docs/architecture/roles.md`, «Не зроблено»). |
| Security: transport і секрети | partially met | Kafka SASL_SSL і mTLS перевірені на одному захищеному брокері (`docs/operations/kafka-tls-sasl-test.md`). TLS між іншими компонентами (PG, ClickHouse, S3, Redis у проді), ротація секретів під навантаженням, vault: not evidenced. `docs/operations/runbooks/secret-rotation.md` — чернетка без репетиції. |
| Auth (AAS) | partially met | Реальний AAS: discovery і JWKS перевірені (`libs/go-common/auth/realaas_test.go`, `tests/aas-live`); реальний токен `astryon-dev-ingest` проходив gateway (`docs/tests/acceptance.md`, Ingestion). Усі 13 scopes ASTRYON додані до realm і видні в discovery; інші сервіси з токеном AAS не перевірені (сервіси відповідають 401 без токена). DPoP: ядро є, Redis-store і live-тест відсутні (`docs/architecture/dpop-plan.md`). Вхід користувача через admin-bff **жодним користувачем не виконувався**; BFF перевірений лише проти fake AAS і до redirect-сторінки реального AAS. Role→scope матриця (`roles.md`) — вимога до AAS, ролі в AAS не створені. |
| Performance | not met | `docs/benchmarks/dpp-010-2026-10-20.md`: ціль 120k подій/с і p95 ACK 250 мс **не досягнута**; максимум 45 318,1 прийнятих подій/с (запит 120 000), p95 ACK 420,3 мс; навіть при 10k подій/с p95 ACK 360,3 мс. Одновузлова dev-конфігурація (macOS, RF=1, plaintext). Це лише gateway→Kafka, не end-to-end. PERF-01…08 (§11) не виконані: у репозиторії немає звітів на 60 хв, 1 млрд/добу, skew, backfill. `dpp-047-2026-10-21.md`: хвостові tenants p95 ≤ 118 мс при 16 клієнтах, але tenant із 2,7–8 млн рядків коштує 0,7–2,1 с на aggregate, а 16 паралельних hot-tenant запитів перевищують ліміт 2 GB і вбивають вузол на 6 GB. |
| Observability | partially met | Правила Prometheus (`infra/observability/prometheus-rules.yaml`, promtool-перевірені) для gateway, sink, query-api, aggregator, file-ingestion; метрики `/metrics` у сервісах. Alertmanager, exporters, service discovery, дашборди не розгорнуті (`infra/observability/README.md`); пороги — «initial guardrails». Трасування: not evidenced (trace/log export свідомо debug-only). |
| Ops: runbooks | partially met | `docs/operations/runbooks/`: retry-controller, ingestion-failures, stuck-consumer, replay, secret-rotation. Відсутні (перелік у `runbooks/README.md`): Kafka, PostgreSQL, S3, ClickHouse, secret compromise як окремий runbook. Жоден не репетирувався. |
| Ops: rollout і rollback | not met | `docs/operations/release-rollback.md` — політика-чернетка; жодного rollback не виконано, runtime feature flags немає. **DPP-067 (canary 1%→10%→50%→100%) не визначений:** умов переходу в репозиторії немає. DPP-068 gates (`scripts/check-migrations.sh`, `check-scopes.sh`) підключені до CI як job `repo-gates`, на GitHub не запускалися. |
| Ops: support / on-call | not evidenced | `docs/operations/incident-management.md` описує severity і error budget, але розкладу чергувань, контактів, Alertmanager і SLI recording rules немає. |
| DR / backup | not evidenced | Plan §11 вимагає restore у чистому середовищі; доказів немає: restore PostgreSQL/ClickHouse/S3, DPP-061 (chaos), DPP-062, DPP-065 (RTO/RPO) не виконувалися. `docs/architecture/failure-model.md`: HA і RPO/RTO «потребують окремих доказів». |
| HA / multi-node | not evidenced | Усі результати на одновузлових dev-стеках (Kafka RF=1, один ClickHouse). Виняток: dedup перевірений на 2 шардах і 2 репліках із Keeper (`docs/tests/sink-crash-restart.md`, див. PROJECT_INDEX); це не відмовостійкість кластера. Redis Cluster/Sentinel failover у query-api перевірено live. |
| Data quality | partially met | Дедуплікація sink (токен чанку, intent-таблиця) перевірена kill -9 і fault-injection (`docs/tests/sink-crash-restart.md`); `ch-aggregator reconcile` перевірений на живих даних (CH 24.8 + PG 16). Replay e2e з реальними бінарниками пройшов (500/30 000/300 000 подій) і знайшов баги, виправлені (`docs/tests/live-verification-checklist.md`). Не покрито: activation і aggregates у тому ж стеку, multi-broker, TLS/SASL, більше одного worker. Конвеєр нормалізації (Flink) не підтверджений — див. нижче. |
| Cost | not evidenced | Моделі вартості, обсягів зберігання, retention-розрахунку немає; TTL у ClickHouse 0006 — значення за замовчуванням до DPP-003. DPP-072 не почато. |
| Documentation / SDK | partially met | OpenAPI для п'яти сервісів, generated Go-клієнти (`libs/sdk-*`, перевірка drift у CI), `docs/errors/`, `docs/onboarding/`. Клієнти лише Go. |

## Відомі прогалини (прямо)

1. **Продуктивність:** 45,3k подій/с проти цілі 120k; p95 ACK 420 мс проти 250 мс. Вузьке місце не встановлене (у звіті сказано, що профілювання не зроблене).
2. **Немає HA/multi-node свідчень** для Kafka, PostgreSQL, ClickHouse, S3, Flink.
3. **Немає chaos-, restore- і DR-навчань.** Жодний пункт «Chaos і DR» з §11 не виконаний.
4. **Flink-конвеєр нормалізації (`raw.events.v1` → `processed.events.v1`) не підтверджений.** Агент зібрав jar (17 юніт-тестів) і запустив міні-кластер Flink 1.20.3: джоба доходить до RUNNING, але падає на першому записі через Kryo/`RawRecord` (`UnsupportedOperationException … can't get field offset on a record class`), у вихідні топіки події не потрапляють. Оновлення 2026-10-21: record-типи замінені на POJO (`RawRecord`, `OutputRecord`, `AcceptedRecord`), 19 юніт-тестів; на dev-стенді джоба обробила 1351 подію в `processed.events.v1`, невалідна `schema_version` потрапила в quarantine. **Нова знахідка:** повідомлення без валідного `tenant_id` викликає `IllegalStateException` у `EnvelopeProcessor.reject` і нескінченні рестарти джоби, що суперечить гейту спринту 9 (помилкове повідомлення не блокує партицію). Checkpoint-відновлення, HA, навантаження не перевірені. Поки що `clickhouse-sink` читає `raw.events.v1` напряму (PROJECT_INDEX, `docs/tests/e2e-smoke.md`).
5. **AAS перевірений частково:** див. рядок Auth; вхід користувача не виконувався; клієнт `astryon-portal` і scopes застосовані до realm (prod, `env=prod`), без користувацького e2e.
6. **admin-bff:** сесії лише в пам'яті процесу; back-channel logout і DPoP-токени не підтримуються (`services/admin-bff/README.md`); Admin UI у режимі BFF запущений, але вхід не пройдений.
7. **DPP-067 (canary)** без визначених умов переходу; **DPP-048 (OpenSearch)** не вирішений; **cost** без моделі.
8. **Live-перевірки незавершені** (`docs/tests/live-verification-checklist.md`): `EXPORT_CH_HEADER_TIMEOUT` не валідований; activation і aggregates у replay-стеку; TLS/SASL quarantine produce.
9. **CI** не є обов'язковим gate (required checks, незмінні action SHA, підпис образів).
10. **Міграції:** нумерація PG 0009–0013 належить цій гілці (`PROJECT_INDEX.md`); потрібне перенумерування при злитті з `flut_kol/ASTRYON`, якщо там з'являться свої 0009+.

## Що потрібно для повторного рішення

Мінімально: виміряні PERF-01…04 на багатовузловому стенді; chaos-набір і restore у чистому середовищі за §11;
підтверджена Flink-нормалізація; користувацький e2e через AAS; визначені умови DPP-067; модель вартості; обов'язкові CI-перевірки;
runbooks для Kafka, PostgreSQL, S3, ClickHouse. Кожен гейт фіксує команду/звіт, дату, середовище, власника і винятки
(`docs/tests/acceptance.md`).
