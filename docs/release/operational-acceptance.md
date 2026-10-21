# Operational acceptance checklist (DPP-070)

Усі пункти **не виконані** на момент написання. Позначка `[x]` ставиться лише з посиланням на команду або звіт, дату,
середовище, власника і винятки (`docs/tests/acceptance.md`: юніт-тести не замінюють реальний збій кластера, restore чи тест AAS).
Джерела вимог: `DEVELOPMENT_PLAN.md` §2, §11, §12, §13, §15; статус: `readiness-review.md`.

## 1. Продуктивність і SLO (§2, §11)

- [ ] PERF-01 10k подій/с, 1 KiB, 60 хв: без втрат підтверджених подій, стабільний lag
- [ ] PERF-02 25k подій/с, 60 хв
- [ ] PERF-03 50k подій/с, 30 хв
- [ ] PERF-04 120k подій/с, 10 хв: пік прийнято, backlog зникає (попередній результат: 45 318,1 подій/с, ціль не досягнута)
- [ ] PERF-05 еквівалент 1 млрд/добу, 24 год
- [ ] PERF-06 skew: tenant X 80% трафіку, ізоляція
- [ ] PERF-07 backfill 1+ TB: обмежена пам'ять, restart/retry
- [ ] PERF-08 20/100/500 паралельних query-клієнтів, p95 задокументовано
- [ ] p95 ACK ≤ 250 мс у LAN при штатному навантаженні (попередньо виміряно 420,3 мс)
- [ ] p95 end-to-end до аналітики ≤ 5 с (не вимірювалося)
- [ ] SLI доступності визначені як recording rules; 99,9%/міс відстежується

## 2. Надійність і DR (§11 «Chaos і DR»)

- [ ] Відмова одного Kafka-брокера під потоком: підтверджений ack, leader election, replay
- [ ] Відмова Kafka-контролера при збереженому кворумі
- [ ] Відмова Flink TaskManager: відновлення з checkpoint
- [ ] Відмова репліки ClickHouse: маршрутизація запитів, консистентність читання
- [ ] Відмова primary PostgreSQL без split brain
- [ ] Відмова OSD/хоста Ceph і доступність бакета
- [ ] Невдала міграція схеми: rollback без псування потоку подій
- [ ] Повне відновлення PostgreSQL, ClickHouse і критичних S3-метаданих у чистому середовищі з backup
- [ ] Фактичні RTO/RPO підтверджені, DR-план підписаний
- [ ] Підтверджена Flink-нормалізація `raw` → `processed` (поточний стан: джоба падає на першому записі, виправлення в процесі)

## 3. Безпека (§12)

- [ ] CI: обов'язкові статус-перевірки на `main`, незмінні SHA для actions, підпис образів за digest
- [ ] Тест tenant isolation для query, exports, batch, replays на всіх endpoint-ах (не лише query)
- [ ] Негативні AAS-тести для кожного сервісу: wrong issuer/audience/env/azp, прострочений, відсутній scope
- [ ] Користувацький вхід через admin-bff (Authorization Code + PKCE, MFA в AAS), logout, session policy
- [ ] Ролі platform-admin, tenant-admin, operator, analyst, auditor, service-account створені в AAS і відповідають `docs/architecture/roles.md`
- [ ] Kafka ACL, TLS між усіма компонентами, мережева ізоляція
- [ ] Секрети лише в secret manager; ротація відпрацьована на живій системі (`runbooks/secret-rotation.md`)
- [ ] SSRF/egress allowlist конекторів, перевірка файлів (zip bombs, malware scan за політикою)
- [ ] Маскування PII, терміни зберігання, видалення в усіх репліках і похідних наборах задокументовані й перевірені

## 4. Спостережуваність (§13)

- [ ] Prometheus, Alertmanager, exporters, service discovery розгорнуті
- [ ] Усі алерти з §13 (ack errors, under-replicated partitions, lag, backpressure, checkpoint failures, DLQ, диски CH, merges, replication lag PG, S3, Ceph, backups, query p95, auth failures, сертифікати) існують і ведуть на runbook
- [ ] Дашборди overview, ingest, Kafka, Flink, CH, PG, query API, tenant quotas, quality, failed events імпортовані й перевірені
- [ ] Трасування й експорт логів за затвердженим бекендом і політикою зберігання

## 5. Runbooks (§13)

Існують чернетки: retry-controller, ingestion-failures, stuck-consumer, replay, secret-rotation. Жоден не репетирувався.
- [ ] Runbook Kafka (партиції, ACL/TLS, lag) написано і відпрацьовано
- [ ] Runbook PostgreSQL (доступність, connection exhaustion, міграція, restore)
- [ ] Runbook S3/object storage
- [ ] Runbook ClickHouse (insert/query, replica/disk, міграції, вартість запитів)
- [ ] Runbook secret compromise відпрацьований
- [ ] Runbooks для giant batch, skew партицій, tenant offboarding, Flink restore, corrupted uploads
- [ ] Кожен runbook виконаний у навчанні і підписаний власником компонента

## 6. Rollout і підтримка (DPP-067…069)

- [ ] Умови переходу canary 1% → 10% → 50% → 100% визначені наперед (зараз відсутні)
- [ ] Rollback образу, версії pipeline і схеми відпрацьований (`docs/operations/release-rollback.md`)
- [ ] Feature flags або інший механізм вимкнення без рестарту (зараз лише env + рестарт)
- [ ] Скрипти `check-migrations.sh` і `check-scopes.sh` пройшли як обов'язкові перевірки на GitHub
- [ ] Розклад on-call, ескалація, канали, Alertmanager receivers
- [ ] Політика error budget прийнята, SLI зібрані
- [ ] Модель вартості на мільйон подій і життєвий цикл зберігання (DPP-072)

## 7. Дані і якість

- [ ] Reconcile (`ch-aggregator reconcile`) без дрейфу на реальних даних за визначений період
- [ ] Replay: activation і rollback перевірені разом з агрегатами, більше одного worker, multi-broker
- [ ] `EXPORT_CH_HEADER_TIMEOUT` перевірений на найповільнішому реалістичному запиті
- [ ] Debezium → connector-manager CDC: restart/failover зберігає позицію на живому потоці (gate спринту 08)
- [ ] Контрольована міграція нумерації PG 0009–0013 при злитті з `flut_kol/ASTRYON`

## Підпис

| Роль | Ім'я | Дата | Рішення |
|---|---|---|---|
| Власник платформи | TODO | TODO | TODO |
| Безпека | TODO | TODO | TODO |
| Експлуатація | TODO | TODO | TODO |
