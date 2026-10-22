# Інциденти, severity, on-call, error budget (DPP-069)

Статус: політика-чернетка. Команди, графіки чергувань, канали та інструмент пейджингу не визначені — це рішення
власників; нижче лише структура, прив'язана до наявних алертів і SLO (`DEVELOPMENT_PLAN.md` §2, §13).

## Severity

| Sev | Критерій | Реакція (цілі, не SLA) |
|---|---|---|
| SEV1 | підтверджені події можуть бути втрачені; крос-tenant доступ до даних; компрометація секрету; ingestion API недоступний | негайний пейдж, incident commander, оновлення кожні 30 хв |
| SEV2 | p95/ACK або end-to-end SLO порушено; lag росте без стабілізації; query/export недоступні для частини tenants | пейдж у робочий час + чергова зміна, оновлення щодня до стабілізації |
| SEV3 | деградація без впливу на клієнтів (reconcile drift, throttling, quarantine росте) | тікет, розбір у наступний робочий день |

## Прив'язка алертів (`infra/observability/prometheus-rules.yaml`)

- SEV1-кандидати: `AstryonGatewayAckErrors`, `AstryonReconcileDrift` (підтверджений), `AstryonQueryDPoPReplay`.
- SEV2: `AstryonKafkaConsumerLagGrowing`, `AstryonSinkStalled`, `AstryonSinkInsertFailures`, `AstryonQueryBackendErrors`,
  `AstryonClickHouseDiskHigh`, `AstryonAggregatorStale`.
- SEV3: `AstryonQueryThrottling`, `AstryonSinkQuarantineRising`, `AstryonFileInvalidRecordsHigh`, `AstryonReconcileCheckFailing`.

Це пропозиція; рівні підтвердити після навантажувальних тестів (пороги в README observability — «initial guardrails»).

## Ролі в інциденті

Incident commander (рішення й комунікація), operations lead (діагностика за runbook), scribe (хронологія без payload і
секретів), component owner (ескалація). Ручні дії над даними фіксуються в audit; runbook-и: `runbooks/`.

## SLO та error budget

- Доступність ingestion/query API 99,9%/міс → бюджет ≈ 43 хв недоступності на 30 днів (розрахунок, не виміряне).
- p95 ACK ≤ 250 мс і p95 end-to-end ≤ 5 с — **цілі**; виміряне: p95 ACK 420 мс при максимумі 45,3k подій/с
  (`docs/benchmarks/dpp-010-2026-10-20.md`), тобто ціль наразі не досягнута.
- Політика: витрачено >50% бюджету за період — заморозка не-критичних релізів, пріоритет надійності; 100% — лише fix-и.
- Метрики доступності (SLI) ще не визначені як recording rules; потрібно до production gate.

## Post-incident

Blameless review протягом 5 робочих днів для SEV1/SEV2: хронологія, причина, що спрацювало, action items з власниками.

## Не зроблено

Розклад чергувань, ескалаційні контакти, інтеграція Alertmanager, SLI recording rules, навчальні «game days».
