# Інтеграція ASTRYON з AAS Auth

## Рішення

ASTRYON **не містить і не розгортає власний auth-сервіс**. Єдиний identity provider уже реалізований у
`/var/apps/astranex/aas-auth`. ASTRYON споживає його тільки через HTTPS OAuth 2.0 / OpenID Connect API.

Заборонено:

- копіювати код `aas-auth` у репозиторій ASTRYON;
- читати базу Keycloak/PostgreSQL напряму;
- зберігати паролі користувачів або створювати паралельну таблицю identity;
- приймати `tenant_id`, ролі або scopes із довільних HTTP-заголовків чи параметрів запиту;
- використовувати `client_secret`: клієнти AAS автентифікуються `private_key_jwt`, а приватні ключі зберігаються у secret manager.

## Публічний API

Базова адреса задається змінною `AAS_BASE_URL`. Жоден production hostname не хардкодиться в коді.

| Призначення | Endpoint |
|---|---|
| OIDC discovery | `${AAS_BASE_URL}/.well-known/openid-configuration` |
| OAuth authorization server metadata | `${AAS_BASE_URL}/.well-known/oauth-authorization-server` |
| Авторизація користувача | `${AAS_BASE_URL}/oauth2/authorize` |
| Видача токена / client credentials / token exchange | `${AAS_BASE_URL}/oauth2/token` |
| JWKS | `${AAS_BASE_URL}/oauth2/jwks` |
| Introspection для L3 ресурсів | `${AAS_BASE_URL}/oauth2/introspect` |
| Revocation | `${AAS_BASE_URL}/oauth2/revoke` |
| Health | `${AAS_BASE_URL}/healthz` |

`issuer` завжди береться з discovery metadata і порівнюється з token claim `iss` буквально. Endpoint-и також
беруться з metadata; таблиця вище є контрактом і діагностичною підказкою, а не приводом конструювати URL вручну.

## Реєстрація ASTRYON у чинному AAS

До ввімкнення інтеграції власник AAS додає конфігурацію ASTRYON у
`/var/apps/astranex/aas-auth/realm-config/` та застосовує її штатним `make config`. Мінімальний набір:

1. `astryon-portal` — confidential OIDC client для Admin UI/BFF, Authorization Code + PKCE S256, точні redirect URI без wildcard, `private_key_jwt`, back-channel logout.
2. ASTRYON gateway — service client із `private_key_jwt`, token exchange і окремим audience URI.
3. Resource-server audience для ingestion/control/query API. За потреби сервіси отримують окремі audiences; один спільний audience дозволений лише доки вони справді мають однакову trust boundary.
4. Machine clients для джерел даних — окремий client на інтеграцію, `tenant_id` як підписаний claim, мінімальні optional scopes, L2/DPoP за замовчуванням.

Реєстрація є конфігурацією чинного сервісу, а не кодом ASTRYON. Її contract test має перевіряти discovery,
audiences, redirect URI, scopes, `tenant_id`, token exchange та revocation до розгортання ASTRYON.

## Початкова модель scopes

Назви відповідають формату AAS `<namespace>:<resource>:<action>` без wildcard. Endpoint вимагає всі заявлені для
нього scopes; роль не замінює object-level authorization.

| Scope | Доступ |
|---|---|
| `astryon:events:write` | submit single/batch events |
| `astryon:events:read` | query events у дозволеному tenant |
| `astryon:uploads:write` | create/complete uploads |
| `astryon:sources:read`, `astryon:sources:write` | read/change sources |
| `astryon:pipelines:read`, `astryon:pipelines:write` | read/change pipelines |
| `astryon:jobs:read`, `astryon:jobs:write` | inspect/cancel jobs |
| `astryon:replays:write` | create replay requests |
| `astryon:exports:read`, `astryon:exports:write` | inspect/create exports |
| `astryon:metrics:read` | business metrics API |
| `astryon:platform:admin` | privileged control-plane operations |

Остаточна матриця endpoint → audience → scopes зберігається поруч з OpenAPI і тестується автоматично.

## Потоки

### Admin UI

Браузер працює через BFF: Authorization Code + PKCE S256, обов'язковий MFA забезпечує AAS. BFF тримає токени
server-side і видає браузеру `Secure`, `HttpOnly`, `SameSite` session cookie. Токени не зберігаються в
`localStorage`/`sessionStorage`. Logout завершує локальну сесію та викликає OIDC logout/back-channel cleanup.

### Machine-to-gateway

Джерело отримує короткоживучий access token через `client_credentials` + `private_key_jwt`; для L2 токен
прив'язаний DPoP. `tenant_id` надходить лише як підписаний claim. Ingestion gateway перевіряє токен і записує
`tenant_id`, `client_id`, `sub`, `jti` та scopes у trusted request context; payload не може їх перевизначити.

### Gateway-to-service

Gateway робить token exchange на audience конкретного resource server або отримує provider token від власного
імені та передає підписаний `X-AAS-Subject` за правилами AAS. Внутрішній сервіс приймає `X-AAS-Subject` тільки з
валідним gateway token і ніколи не довіряє цьому заголовку самостійно.

### Service-to-service

Фонові workers використовують окремі service clients і `client_credentials`; не перевикористовують browser token
та не отримують scopes ширші за свою задачу.

## Перевірка access token у Go-сервісах

Готова Python-бібліотека `/var/apps/astranex/aas-auth/libs/aas_auth` є референсною реалізацією протоколу, але
Go-сервіси ASTRYON мають один спільний middleware у `libs/go-common/auth`, а не власні часткові перевірки.
Middleware fail-closed перевіряє:

- схему `Authorization` (`Bearer` або `DPoP`), дозволений signing algorithm і `kid`;
- підпис через JWKS з bounded cache та контрольованим refresh на невідомому `kid`;
- `iss`, service-specific `aud`, `exp`, `nbf` (перевіряється, якщо присутній: AAS його не видає), `iat`, обов'язковий `jti`, token `typ`;
- claim середовища `env`, дозволений `azp`, scopes і token binding `cnf`;
- DPoP proof: `htu`, HTTP method, `ath`, clock skew та одноразовість proof `jti`; у кількох репліках replay store спільний;
- для L3 — `active=true` через introspection;
- `tenant_id` та object ownership перед кожною операцією з source/job/export/replay/event.

Результат middleware — типізований `Principal`; downstream-код не декодує JWT повторно і не читає identity з
неперевірених claims.

## Конфігурація ASTRYON

Секрети не мають значень за замовчуванням і не потрапляють у Git.

| Variable | Призначення |
|---|---|
| `AAS_BASE_URL` | public URL AAS proxy для discovery/token operations |
| `AAS_ISSUER` | очікуваний issuer; на deploy звіряється з discovery |
| `AAS_ENV` | очікуване середовище `dev`, `test` або `prod` |
| `AAS_CLIENT_ID` | client ID конкретного ASTRYON workload |
| `AAS_PRIVATE_KEY_FILE` | secret-mounted PEM для `private_key_jwt` |
| `AAS_AUDIENCE` | audience конкретного resource server |
| `AAS_ALLOWED_AZP` | allowlist gateway/portal clients |
| `AAS_TOKEN_TYPE` | очікуваний claim `typ`; за замовчуванням `at+jwt`, реальний AAS (Keycloak) видає `Bearer` |
| `AAS_REDIS_URL` | shared DPoP replay store для кількох реплік |

TLS verification увімкнена завжди поза локальним dev. CA bundle монтується як secret/config map; `verify=false`
не є допустимим production fallback.

## Відмови та спостережуваність

- Недоступність AAS блокує новий login, token exchange, introspection і refresh, але локально валідний JWT може
  працювати до `exp`; fail-open після `exp` заборонений.
- JWKS cache переживає коротку недоступність AAS, має TTL і метрики refresh/error/unknown-kid.
- 401 означає відсутній/невалідний token; 403 — валідний principal без потрібного scope або object access.
- Метрики: discovery/JWKS/token/introspection latency та errors, auth 401/403 за кодом причини, DPoP replay,
  tenant mismatch. Токени, assertions, DPoP proofs, cookies і приватні claims не логуються.

## Acceptance criteria

- У репозиторії ASTRYON немає копії `aas-auth`, Keycloak image/realm або auth-бази.
- Contract test читає discovery чинного AAS і перевіряє issuer та endpoint-и.
- Позитивні тести покривають browser flow, machine client, token exchange, tenant context і потрібні scopes.
- Негативні тести покривають wrong issuer/audience/env/azp, expired token, unknown `kid`, відсутній scope,
  tenant mismatch, DPoP replay/wrong `htu`/wrong `ath`, forged `X-AAS-Subject` і revoked L3 token.
- Ротація signing key проходить без рестарту сервісів і без приймання токена з невідомим ключем.
- Усі API з OpenAPI мають declared auth requirement або явне `public` виключення, перевірене lint-тестом.
