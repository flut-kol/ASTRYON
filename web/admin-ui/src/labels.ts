// Human-readable text for machine values that come from the servers: error codes, job/export/replay statuses, source types,
// sensitivities, content labels, quarantine reason codes, quality flags, timestamps. Free-form data (ids, payloads, server
// messages) is shown as received. Unknown values fall back to a humanized form of the code ("some_code" -> "Some code").
import { getLang, t } from './i18n';

type Pair = [en: string, ua: string];
const M: Record<string, Record<string, Pair>> = {
  status: {
    NEW: ['New', 'Нове'], QUEUED: ['Queued', 'У черзі'], RUNNING: ['Running', 'Виконується'], RETRYING: ['Retrying', 'Повторна спроба'],
    SUCCEEDED: ['Succeeded', 'Успішно'], FAILED: ['Failed', 'Помилка'], CANCELLED: ['Cancelled', 'Скасовано'], QUARANTINED: ['Quarantined', 'У карантині'],
    REQUESTED: ['Requested', 'Запрошено'], APPROVED: ['Approved', 'Схвалено'], COMPLETED: ['Completed', 'Завершено'],
    ok: ['OK', 'Працює'], unavailable: ['Unavailable', 'Недоступний'],
  },
  sourceType: {
    unknown: ['Unknown', 'Невідомий'], telemetry: ['Telemetry', 'Телеметрія'], geo: ['Geodata', 'Геодані'], report: ['Report', 'Звіт'], export: ['Export', 'Експорт'],
    config: ['Configuration', 'Конфігурація'], log: ['Log', 'Журнал'], credentials_bundle: ['Credentials bundle', 'Набір облікових даних'],
    personal_data: ['Personal data', 'Персональні дані'], financial: ['Financial', 'Фінансові дані'], media_meta: ['Media metadata', 'Метадані медіа'], reference: ['Reference data', 'Довідкові дані'],
    rest_pull: ['REST pull', 'REST-опитування'], cdc: ['Change data capture', 'Відстеження змін (CDC)'],
  },
  kind: {
    kafka: ['Kafka', 'Kafka'], http_push: ['HTTP push', 'HTTP-надсилання'], s3_files: ['S3 files', 'Файли S3'], sftp: ['SFTP', 'SFTP'], database: ['Database', 'База даних'], api_pull: ['API pull', 'API-опитування'],
    process_file: ['File processing', 'Обробка файлу'],
    REST_PULL: ['REST pull', 'REST-опитування'], POSTGRES_CDC: ['PostgreSQL CDC', 'PostgreSQL CDC (відстеження змін)'], MYSQL_CDC: ['MySQL CDC', 'MySQL CDC (відстеження змін)'],
  },
  sensitivity: { public: ['Public', 'Публічна'], internal: ['Internal', 'Внутрішня'], restricted: ['Restricted', 'Обмежена'] },
  label: {
    contains_geo: ['Contains geodata', 'Містить геодані'], contains_credentials: ['Contains credentials', 'Містить облікові дані'], contains_pii: ['Contains personal data', 'Містить персональні дані'],
  },
  flag: {
    late: ['Late arrival', 'Запізніле надходження'], duplicate: ['Duplicate', 'Дублікат'], out_of_order: ['Out of order', 'Порушена послідовність'],
    missing_value: ['Missing value', 'Відсутнє значення'], outlier: ['Outlier', 'Викид'],
  },
  stage: { normalize: ['Normalization', 'Нормалізація'], validate: ['Validation', 'Валідація'], parse: ['Parsing', 'Розбір'], sink: ['Storage write', 'Запис у сховище'], enrich: ['Enrichment', 'Збагачення'] },
  retention: { short: ['Short', 'Коротке'], standard: ['Standard', 'Стандартне'], long: ['Long', 'Довге'] },
  reason: {
    BAD_VALUE: ['Invalid value', 'Некоректне значення'], BODY_INVALID: ['Invalid request body', 'Некоректне тіло запиту'], CREDENTIALS_DETECTED: ['Credentials detected in content', 'У вмісті виявлено облікові дані'],
    FILE_TOO_LARGE: ['File too large', 'Файл завеликий'], CDC_MALFORMED_EVENT: ['Malformed change event', 'Пошкоджена подія змін'], ERR_JSON_MALFORMED: ['Malformed JSON', 'Пошкоджений JSON'],
    ERR_CHECKSUM_INVALID: ['Invalid checksum', 'Некоректна контрольна сума'], ERR_CHECKSUM_MISMATCH: ['Checksum mismatch', 'Контрольна сума не збігається'], ERR_CHECKSUM_REQUIRED: ['Checksum required', 'Потрібна контрольна сума'],
    ERR_ENTITY_KEY_INVALID: ['Invalid entity key', 'Некоректний ключ сутності'], ERR_EVENT_ID_INVALID: ['Invalid event id', 'Некоректний id події'],
    ERR_EVENT_TIME_FUTURE: ['Event time is in the future', 'Час події в майбутньому'], ERR_EVENT_TIME_MISSING: ['Event time is missing', 'Відсутній час події'],
    ERR_EVENT_TIME_NOT_UTC: ['Event time is not in UTC', 'Час події не в UTC'], ERR_EVENT_TIME_TOO_OLD: ['Event time is too old', 'Час події занадто давній'],
    ERR_INGESTED_AT_INVALID: ['Invalid ingestion time', 'Некоректний час прийому'], ERR_OBJECT_REF_INVALID: ['Invalid object reference', 'Некоректне посилання на об’єкт'],
    ERR_PAYLOAD_FORMAT_INVALID: ['Invalid payload format', 'Некоректний формат даних'], ERR_PAYLOAD_MALFORMED: ['Malformed payload', 'Пошкоджені дані'],
    ERR_PAYLOAD_TOO_LARGE: ['Payload too large', 'Дані завеликі'], ERR_PAYLOAD_XOR_OBJECT_REF: ['Payload and object reference are mutually exclusive', 'Дані та посилання на об’єкт взаємовиключні'],
    ERR_PROCESSING_VERSION_INVALID: ['Invalid processing version', 'Некоректна версія обробки'], ERR_SCHEMA_ID_INVALID: ['Invalid schema id', 'Некоректний id схеми'],
    ERR_SCHEMA_INVALID: ['Invalid schema', 'Некоректна схема'], ERR_SCHEMA_VERSION_INVALID: ['Invalid schema version', 'Некоректна версія схеми'],
    ERR_SOURCE_ID_INVALID: ['Invalid source id', 'Некоректний id джерела'], ERR_TENANT_ID_INVALID: ['Invalid tenant id', 'Некоректний id тенанта'], ERR_TRACE_ID_INVALID: ['Invalid trace id', 'Некоректний trace id'],
  },
  error: {
    invalid_request: ['The request is invalid.', 'Запит некоректний.'], invalid_range: ['The time range is invalid.', 'Некоректний часовий діапазон.'],
    invalid_cursor: ['The page cursor is invalid or expired.', 'Курсор сторінки некоректний або застарів.'], invalid_limit: ['The page size is invalid.', 'Некоректний розмір сторінки.'],
    invalid_metric: ['Unknown metric.', 'Невідома метрика.'], invalid_group_by: ['Unsupported grouping.', 'Непідтримуване групування.'], invalid_spec: ['The export parameters are invalid.', 'Некоректні параметри експорту.'],
    invalid_connector: ['The connector settings are invalid.', 'Некоректні налаштування конектора.'], invalid_state: ['This action is not possible in the current state.', 'Ця дія неможлива в поточному стані.'],
    unauthenticated: ['You are not signed in or the session has expired.', 'Ви не увійшли або сесія завершилась.'], forbidden: ['You do not have permission for this action.', 'У вас немає прав для цієї дії.'],
    not_found: ['Not found.', 'Не знайдено.'], not_configured: ['This feature is not configured on the server.', 'Ця функція не налаштована на сервері.'],
    unavailable: ['The service is temporarily unavailable.', 'Сервіс тимчасово недоступний.'], upstream_error: ['A backend service returned an error.', 'Внутрішній сервіс повернув помилку.'],
    upstream_unavailable: ['A backend service is unavailable.', 'Внутрішній сервіс недоступний.'], audit_unavailable: ['The audit log is unavailable, so the action was refused.', 'Журнал аудиту недоступний, тому дію відхилено.'],
    AUDIT_UNAVAILABLE: ['The audit log is unavailable, so the action was refused.', 'Журнал аудиту недоступний, тому дію відхилено.'],
    rate_limited: ['Too many requests. Try again in a moment.', 'Забагато запитів. Спробуйте за мить.'], too_many_concurrent_queries: ['Too many queries are running. Try again in a moment.', 'Виконується забагато запитів. Спробуйте за мить.'],
    limiter_unavailable: ['The rate limiter is unavailable.', 'Обмежувач запитів недоступний.'], body_too_large: ['The request is too large.', 'Запит завеликий.'], shutting_down: ['The service is shutting down.', 'Сервіс завершує роботу.'],
    queue_full: ['The queue is full. Try again later.', 'Черга заповнена. Спробуйте пізніше.'], quota_exceeded: ['The quota is exceeded.', 'Квоту перевищено.'],
    row_cap_exceeded: ['The export exceeds the row limit.', 'Експорт перевищує ліміт рядків.'], byte_cap_exceeded: ['The export exceeds the size limit.', 'Експорт перевищує ліміт розміру.'],
    candidate_limit_exceeded: ['Too many records for a replay.', 'Забагато записів для повтору.'], presign_failed: ['Could not prepare the upload link.', 'Не вдалося підготувати посилання для завантаження.'],
    export_failed: ['The export failed.', 'Експорт завершився помилкою.'], emit_failed: ['Publishing to the pipeline failed.', 'Не вдалося опублікувати в пайплайн.'], mapping_failed: ['Mapping the data failed.', 'Помилка відображення даних.'],
    parser_unavailable: ['The file parser is unavailable.', 'Парсер файлів недоступний.'], secret_unavailable: ['A required secret is unavailable.', 'Потрібний секрет недоступний.'],
    version_regression: ['A newer processing version is already active.', 'Вже активна новіша версія обробки.'], rollback_unavailable: ['Rollback is not possible.', 'Відкат неможливий.'],
    replay_not_completed: ['The replay has not completed yet.', 'Повтор ще не завершено.'], replay_conflict: ['A conflicting replay already exists.', 'Вже існує конфліктний повтор.'],
    REPLAY_UNAVAILABLE: ['The replay service is unavailable.', 'Сервіс повторів недоступний.'], REPLAY_SCOPE_TOO_LARGE: ['The replay scope is too large.', 'Обсяг повтору завеликий.'],
    REPLAY_PUBLISH_FAILED: ['Publishing the replay failed.', 'Не вдалося опублікувати повтор.'], INVALID_REQUEST: ['The request is invalid.', 'Запит некоректний.'], INVALID_CURSOR: ['The page cursor is invalid or expired.', 'Курсор сторінки некоректний або застарів.'],
    NOT_FOUND: ['Not found.', 'Не знайдено.'], FORBIDDEN: ['You do not have permission for this action.', 'У вас немає прав для цієї дії.'], UNAUTHENTICATED: ['You are not signed in or the session has expired.', 'Ви не увійшли або сесія завершилась.'],
    UNAVAILABLE: ['The service is temporarily unavailable.', 'Сервіс тимчасово недоступний.'], internal: ['Internal server error.', 'Внутрішня помилка сервера.'], backend_error: ['A backend error occurred.', 'Помилка на боці сервера.'],
    network_error: ['The server could not be reached.', 'Не вдалося зв’язатися із сервером.'], bad_response: ['The server sent an unreadable response.', 'Сервер надіслав нечитабельну відповідь.'],
    part_upload_failed: ['Uploading a file part failed.', 'Не вдалося завантажити частину файлу.'], etag_unreadable: ['The storage did not expose the part checksum (CORS).', 'Сховище не надало контрольну суму частини (CORS).'],
    file_too_large: ['File too large.', 'Файл завеликий.'],
  },
};
const HTTP: Record<number, Pair> = {
  400: M.error.invalid_request, 401: M.error.unauthenticated, 403: M.error.forbidden, 404: M.error.not_found, 409: ['The action conflicts with the current state.', 'Дія суперечить поточному стану.'],
  429: M.error.rate_limited, 500: M.error.internal, 501: M.error.not_configured, 502: M.error.upstream_error, 503: M.error.unavailable, 504: ['The service did not respond in time.', 'Сервіс не відповів вчасно.'],
};

const pick = (p: Pair) => (getLang() === 'ua' ? p[1] : p[0]);
/** "some_CODE" -> "Some code"; used for values without a dictionary entry. */
export function humanize(v: string): string {
  const s = v.replace(/^ERR_/, '').replace(/[_.]+/g, ' ').trim().toLowerCase();
  return s ? s[0].toUpperCase() + s.slice(1) : v;
}
export type Kind = 'kind' | 'status' | 'sourceType' | 'sensitivity' | 'label' | 'flag' | 'stage' | 'retention' | 'reason';
/** Display text for a known server enum value (unknown values are humanized, empty stays empty). */
export function human(kind: Kind, v: string | undefined | null): string {
  if (!v) return '';
  const p = M[kind][v];
  return p ? pick(p) : humanize(v);
}
/** Friendly sentence for an API error: by error code, else by HTTP status; undefined when neither is known. */
export function errorText(code: string, status: number): string | undefined {
  const p = M.error[code] ?? M.error[code.toLowerCase()] ?? (status ? HTTP[status] : undefined);
  return p ? pick(p) : undefined;
}

const locale = () => (getLang() === 'ua' ? 'uk-UA' : 'en-GB');
/** Localized date and time; the original value stays available as the title and datetime attributes. */
export function fmtTime(iso: string | undefined | null): string {
  if (!iso) return '';
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? iso : new Intl.DateTimeFormat(locale(), { dateStyle: 'medium', timeStyle: 'medium' }).format(d);
}
export function timeEl(iso: string | undefined | null): Node | string {
  if (!iso) return '';
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return iso;
  const el = document.createElement('time');
  el.dateTime = iso; el.title = iso; el.textContent = fmtTime(iso);
  return el;
}
export function fmtBytes(n: number): string {
  if (!Number.isFinite(n)) return '';
  const u = [t('B'), t('KiB'), t('MiB'), t('GiB')];
  let i = 0, v = n;
  while (v >= 1024 && i < u.length - 1) { v /= 1024; i++; }
  return `${i ? v.toFixed(v < 10 ? 1 : 0) : v} ${u[i]}`;
}
