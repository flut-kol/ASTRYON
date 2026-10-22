# Rollback і сумісність схем (DPP-068)

Статус: політика-чернетка. Жодного rollback не виконано в реальному середовищі; feature flags як механізм у коді
відсутні (див. «Що є насправді»).

## Правила

1. Міграції PostgreSQL і ClickHouse forward-only, нумеровані без пропусків, перший рядок — `-- ROLLBACK PLAN:`.
   Перевіряє `scripts/check-migrations.sh` (також відхиляє `DROP/TRUNCATE/DELETE` без `-- DESTRUCTIVE-OK: <причина>`).
2. Expand → migrate → contract: схема спочатку лише розширюється (nullable/нові таблиці), новий код вмикається,
   старі об'єкти видаляються окремою пізнішою міграцією після одного повного релізного циклу.
3. Кожна версія сервісу має працювати з схемою N і N-1: реліз коду, що потребує міграції N, — лише після її застосування;
   відкат коду на N-1 не вимагає відкату схеми.
4. Відкат схеми в prod — нова forward-міграція, не `DROP` (ROLLBACK PLAN-рядки з `DROP` — лише non-prod).
5. Події: контракт `libs/event-contracts` змінюється лише адитивно; нові поля optional, версія в конверті.

## Що відкочується і як

| Об'єкт | Відкат | Обмеження |
|---|---|---|
| Образ сервісу | попередній тег (Kustomize/Helm) | stateless; consumers ідемпотентні (`failure-model.md`) |
| Pipeline/processing version | `POST /v1/replays/{id}/activate?rollback=true` (PG-атомарно, audit) | дані нової версії лишаються; запити читають активну версію |
| Retention TTL (CH 0006) | зняти/збільшити TTL; уже видалені рядки не повертаються | відкат не відновлює дані |
| Нова міграція | forward-виправлення; для індексів/guard-ів — `DROP INDEX` (напр. 0014) | лише якщо ROLLBACK PLAN це дозволяє |
| Opt-in режими (`CHSINK_INTENT_TABLE`, `QUERY_AGG_CACHE_SECONDS`, `FILE_RETRY_MODE`, `REPLAY_AUDIT_MODE`) | змінити env і перезапустити | це конфігурація, не runtime feature flags |

## Що є насправді

- Runtime feature flags (вмикання без рестарту) немає: перемикачі — env-змінні на рестарт. Якщо знадобляться
  для canary (DPP-067), це окреме рішення з власним сховищем і audit.
- Gate у CI: `scripts/check-migrations.sh` і `scripts/check-scopes.sh` підключені як job `repo-gates` у
  `.github/workflows/ci.yml` (не запускався на GitHub; required check ще не налаштований).
- Rollback runbook-и для Kafka/ClickHouse/PG не виконувалися; потрібні DPP-061/062/065.
