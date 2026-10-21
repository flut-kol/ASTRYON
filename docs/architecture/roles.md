# Ролі та scopes (DPP-052)

ASTRYON не має власного IdP: ролі й scopes видає AAS (`authentication.md`). Сервіс перевіряє лише **scopes** з
валідованого токена; роль — це набір scopes, який власник AAS призначає в realm-конфігурації. Роль не замінює
object-level/tenant authorization: `tenant_id` береться лише з claim.

## Scopes, що реально перевіряє код

Джерело правди — константи в коді; `scripts/check-scopes.sh` падає, якщо scope з коду відсутній у цьому файлі.

| Scope | Сервіс | Доступ |
|---|---|---|
| `astryon:events:write` | gateway | `POST /v1/events[/batch]` |
| `astryon:query:read` | query-api | query/aggregate/summary |
| `astryon:uploads:write` | file-ingestion | upload planning, complete, list, get |
| `astryon:export:write` / `astryon:export:read` | export-worker | create / inspect export |
| `astryon:connectors:write` | connector-manager | list/get/create/pause/resume |
| `astryon:replay:write` / `astryon:replay:read` | replay-controller | create, activate / list, get |
| `astryon:quarantine:read` | quarantine | metadata browser |
| `astryon:quarantine-payload:read` | quarantine | masked original sample (grant separately) |
| `astryon:quarantine-replay:read` | quarantine | replay dry-run |
| `astryon:quarantine-replay:write` | quarantine | publish replay jobs (grant separately) |
| `astryon:data:read`, `astryon:secrets:read`, `astryon:pii:read` | file-ingestion profiles | required read scope per data class, not an endpoint scope |

Розбіжність із початковою моделлю в `authentication.md` (`replays:write`, `exports:*`, `jobs:*`, `sources:*`,
`pipelines:*`, `metrics:read`, `platform:admin`): код використовує форми з таблиці вище. Перед реєстрацією клієнтів
в AAS узгодити імена; ця таблиця описує те, що код приймає сьогодні.

## Ролі

`x` = входить у роль. Service-account — окремий client на інтеграцію, не людська роль.

| Scope | platform-admin | tenant-admin | operator | analyst | auditor | service-account (source) |
|---|:-:|:-:|:-:|:-:|:-:|:-:|
| events:write | | | | | | x |
| query:read | x | x | x | x | x | |
| uploads:write | x | x | x | | | x |
| export:read | x | x | x | x | x | |
| export:write | x | x | | x | | |
| connectors:write | x | x | x | | | |
| replay:read | x | x | x | | x | |
| replay:write | x | x | x | | | |
| quarantine:read | x | x | x | | x | |
| quarantine-payload:read | x | x | | | | |
| quarantine-replay:read | x | x | x | | | |
| quarantine-replay:write | x | x | | | | |

Принципи: мінімально необхідні права (gate спринту 11: повний цикл оператором без admin); `read-payload` і
`replay-publish` не входять у `operator` і `auditor`, бо payload може містити неприхований ввід, а publish має
побічні ефекти; auditor лише читає. Platform-admin діє в межах tenant із claim — крос-tenant доступ не існує в
коді жодного сервісу, тож ця роль його не дає.

## Не зроблено

- Ролі не створені в AAS (realm-конфігурація належить власнику AAS); матриця — вимога до неї.
- Admin UI/BFF не приховує дії за роллю; сервери повертають 403 за відсутнім scope.
- Тест «endpoint → scope» наскрізно (як обіцяє `authentication.md`) поки лише на рівні сервісів, не централізований.
