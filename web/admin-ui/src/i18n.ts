// Two UI languages: English (source strings) and Ukrainian. Code passes the English text to t(); the Ukrainian
// dictionary is keyed by that text, so a missing entry falls back to English instead of showing a key.
// Placeholders are {name}. Server strings (error messages, ids, payloads) are never translated.
export type Lang = 'en' | 'ua';
export const LANGS: { id: Lang; label: string; htmlLang: string; name: string }[] = [
  { id: 'en', label: 'EN', htmlLang: 'en', name: 'English' },
  { id: 'ua', label: 'UA', htmlLang: 'uk', name: 'Українська' },
];
const KEY = 'astryon_admin_lang';

const UA: Record<string, string> = {
  // shell
  'Skip to main content': 'Перейти до основного вмісту',
  'Main': 'Головна навігація',
  'Language': 'Мова',
  'Theme': 'Тема',
  'System': 'Системна',
  'Light': 'Світла',
  'Dark': 'Темна',
  'Sign in': 'Увійти',
  'Sign out': 'Вийти',
  'signed in{who}': 'виконано вхід{who}',
  ' as {subject}': ' як {subject}',
  'checking session...': 'перевірка сесії...',
  'session service unreachable': 'сервіс сесій недоступний',
  'Not found': 'Не знайдено',
  'Unknown page': 'Невідома сторінка',
  'Bearer token': 'Токен доступу (Bearer)',
  'bearer token (dev static or AAS)': 'токен доступу (dev або AAS)',
  'Set': 'Застосувати',
  'Clear': 'Очистити',
  'token set (session only)': 'токен задано (лише в сесії)',
  'no token': 'немає токена',
  // navigation / titles
  'Overview': 'Огляд',
  'Uploads': 'Завантаження',
  'Events': 'Події',
  'Quality': 'Якість',
  'Quality and metrics': 'Якість і метрики',
  'Connectors': 'Конектори',
  'Exports': 'Експорти',
  'Quarantine': 'Карантин',
  'Replays': 'Повтори (replay)',
  'Jobs': 'Завдання',
  'Sources': 'Джерела',
  'File API': 'File API', 'Text search': 'Пошук тексту', 'Access & roles': 'Доступ і ролі', 'Ingestion': 'Завантаження', 'ClickHouse explorer': 'Огляд ClickHouse', 'Administration': 'Адміністрування',
  'Upload intake, processing jobs and content-policy outcomes in one place.': 'Завантаження, завдання обробки та результати політик вмісту в одному місці.',
  'Find indexed tokens in normalized file content without writing SQL.': 'Пошук індексованих токенів у нормалізованому вмісті без написання SQL.',
  'Role and scope visibility for the same data sources exposed in this console.': 'Перегляд ролей і дозволів для тих самих джерел даних, що доступні в цій консолі.',
  'Roles are managed by the external AAS': 'Ролі керуються зовнішнім AAS', 'This console does not mutate roles or permissions. The validated token is the source of truth; changes are made in AAS and take effect on the next session.': 'Ця консоль не змінює ролі чи дозволи. Перевірений токен є джерелом істини; зміни вносяться в AAS і діють у новій сесії.',
  'Role catalogue': 'Каталог ролей', 'Reference matrix; effective access is always checked by each service.': 'Довідкова матриця; фактичний доступ завжди перевіряє кожен сервіс.', 'Data domains and menu access': 'Домени даних і доступ до меню', 'Open': 'Відкрити',
  'Platform administration': 'Адміністрування платформи', 'Tenant administration': 'Адміністрування тенанта', 'Operations': 'Операції', 'Analytics': 'Аналітика', 'Audit': 'Аудит', 'Service account': 'Сервісний обліковий запис',
  'Loading preview...': 'Завантаження перегляду...', 'View safe preview': 'Переглянути безпечний вміст', 'File content': 'Вміст файлу', 'Only bounded, safe text is shown; sensitive values remain hidden.': 'Показується лише обмежений безпечний текст; чутливі значення приховано.', 'Metadata only': 'Лише метадані', 'Content is hidden by policy or this file type is unsupported.': 'Вміст приховано політикою або цей тип файлу не підтримується.',
  // common
  'Loading...': 'Завантаження...',
  'any': 'будь-який',
  '(profile default)': '(за профілем)',
  'Filter': 'Фільтрувати',
  'Search': 'Шукати',
  'Load': 'Завантажити',
  'Refresh': 'Оновити',
  'Newer': 'Новіші',
  'Older': 'Старіші',
  'Previous': 'Назад',
  'Next': 'Далі',
  'From': 'Від',
  'To': 'До',
  'Source ID': 'ID джерела',
  'Source ID (optional)': 'ID джерела (необов’язково)',
  'source uuid': 'uuid джерела',
  'Source type': 'Тип джерела',
  'Sensitivity': 'Чутливість',
  'Status': 'Статус',
  'Label': 'Мітка',
  'Tag': 'Тег',
  'Proc. version': 'Версія обробки',
  'Event type': 'Тип події',
  'Entity': 'Сутність',
  'Format': 'Формат',
  'Max rows': 'Макс. рядків',
  'actions': 'дії',
  'yes': 'так',
  'no': 'ні',
  'invalid time range': 'некоректний часовий діапазон',
  'API not available in this build.': 'API недоступне в цій збірці.',
  '{what}: API not available in this build.': '{what}: API недоступне в цій збірці.',
  'sign in again': 'увійдіть знову',
  'check the token in the box above': 'перевірте токен у полі вгорі',
  // overview
  'Gateway health': 'Стан шлюзу',
  'unreachable': 'недоступний',
  'metrics not available': 'метрики недоступні',
  'no counters yet': 'лічильників ще немає',
  'metric': 'метрика',
  'labels': 'мітки',
  'value': 'значення',
  'Gateway counters': 'Лічильники шлюзу',
  'Gateway counters (public /metrics)': 'Лічильники шлюзу (публічний /metrics)',
  'File API and query API expose no public health endpoint in this build; their pages report errors inline.': 'File API та query API не мають публічного health-ендпоінта в цій збірці; їхні сторінки показують помилки на місці.',
  // uploads
  'Upload': 'Завантаження файлів',
  'Choose CSV or NDJSON files to upload': 'Оберіть CSV або NDJSON файли для завантаження',
  'File drop zone (or use the file chooser)': 'Зона перетягування файлів (або скористайтеся вибором файлів)',
  'Drop CSV / NDJSON files here or ': 'Перетягніть сюди CSV / NDJSON файли або ',
  'Tags (k=v,...)': 'Теги (к=з,...)',
  'starting': 'початок',
  'queued, file {file}, job {job}': 'у черзі, файл {file}, завдання {job}',
  'Show upload {id}': 'Показати завантаження {id}',
  'No uploads match.': 'Немає завантажень за цим фільтром.',
  'id': 'id', 'type': 'тип', 'sensitivity': 'чутливість', 'committed': 'зафіксовано', 'status': 'статус', 'error': 'помилка',
  'bytes': 'байти', 'tags': 'теги', 'created': 'створено',
  'Upload {id}': 'Завантаження {id}',
  'Upload details': 'Деталі завантаження',
  'field': 'поле',
  'source': 'джерело', 'source type': 'тип джерела',
  'job': 'завдання', 'not completed yet': 'ще не завершено',
  'error code': 'код помилки', 'content labels (counts only)': 'мітки вмісту (лише кількість)',
  'profile': 'профіль',
  'Held back by content policy: nothing reached the sink.': 'Затримано політикою вмісту: нічого не потрапило у сховище.',
  // events / quality
  'The query API requires a source id unless a processing version is given (no tenant-wide default). If the source has no active version, enter a processing version.':
    'Query API вимагає ID джерела, якщо не вказано версію обробки (типового значення для всього тенанта немає). Якщо в джерела немає активної версії, введіть версію обробки.',
  'time': 'час', 'entity': 'сутність', 'flags': 'прапорці', 'event id': 'id події', 'payload': 'дані',
  'No events.': 'Подій немає.',
  'Per-source metrics': 'Метрики за джерелами',
  'Per-source quality': 'Якість за джерелами',
  'Quality flags': 'Прапорці якості',
  'no data': 'немає даних',
  // connectors
  'Pause connector {name}': 'Призупинити конектор {name}',
  'Resume connector {name}': 'Відновити конектор {name}',
  'Pause': 'Призупинити',
  'Resume': 'Відновити',
  'No connectors.': 'Конекторів немає.',
  'Connector manager is not running or not reachable': 'Менеджер конекторів не запущений або недоступний',
  'name': 'назва', 'kind': 'вид', 'enabled': 'увімкнено', 'secret ref': 'посилання на секрет',
  // exports
  'No exports started from this browser session. The API has no list endpoint; jobs are tracked by id.': 'У цій сесії браузера експорти не запускались. API не має списку; завдання відстежуються за id.',
  'Range up to 31 days; processing version is required. The tenant comes from your token. Download links are short-lived and fetched again on refresh.':
    'Діапазон до 31 дня; версія обробки обов’язкова. Тенант береться з вашого токена. Посилання для завантаження короткочасні та оновлюються при оновленні.',
  'Start export': 'Почати експорт',
  'processing version is required': 'версія обробки обов’язкова',
  'queued {id}': 'у черзі {id}',
  'loading': 'завантаження',
  'rows': 'рядки', 'format': 'формат', 'download': 'завантаження',
  'download (until {until})': 'завантажити (до {until})',
  // replays
  'Read-only list. Starting and activating replays is not offered here.': 'Список лише для читання. Запуск та активація повторів тут не пропонуються.',
  'No replays.': 'Повторів немає.',
  'Replay controller is not running, not reachable or has no PostgreSQL configured': 'Контролер повторів не запущений, недоступний або без налаштованого PostgreSQL',
  'target version': 'цільова версія', 'range': 'діапазон', 'completed': 'завершено',
  // quarantine
  'Quarantine is empty.': 'Карантин порожній.',
  'Quarantine service is not running or not reachable': 'Сервіс карантину не запущений або недоступний',
  'quarantined at': 'у карантині з', 'stage': 'етап', 'reason code': 'код причини', 'reason': 'причина', 'event': 'подія', 'batch/job': 'пакет/завдання', 'pipeline v': 'версія пайплайна',
  'Replay dry run': 'Пробний запуск повтору',
  'Occurred from': 'Сталося від', 'Occurred to': 'Сталося до', 'Pipeline version': 'Версія пайплайна',
  'Count candidates': 'Порахувати кандидатів',
  'source id, time range and pipeline version are required': 'потрібні ID джерела, часовий діапазон і версія пайплайна',
  '{count} candidate record(s); {state}. Dry run only: nothing was published.': '{count} записів-кандидатів; {state}. Лише пробний запуск: нічого не опубліковано.',
  'within the replay limit': 'в межах ліміту повтору',
  'not executable (too large)': 'не виконується (завелике)',
  // validation / lib messages
  'to must be after from': '«До» має бути пізніше за «Від»',
  'range must not exceed 31 days': 'діапазон не може перевищувати 31 день',
  'processing version must be a non-negative integer': 'версія обробки має бути невід’ємним цілим числом',
  'max rows must be 1..10000000': 'макс. рядків має бути від 1 до 10000000',
  'empty file': 'порожній файл',
  'New connector': 'Новий конектор', 'Name': 'Назва', 'Kind': 'Вид', 'Secret reference': 'Посилання на секрет', 'Config (JSON, optional)': 'Конфігурація (JSON, необов’язково)', 'Create': 'Створити',
  'The secret is entered as a reference (for example env://NAME), never as the secret value. The tenant comes from your token.': 'Секрет вводиться як посилання (наприклад env://NAME), а не як сам секрет. Тенант береться з вашого токена.',
  'config is not valid JSON': 'конфігурація не є коректним JSON', 'connector created: {name}': 'конектор створено: {name}',
  'source id must be a UUID': 'ID джерела має бути UUID', 'name is required (up to 128 characters)': 'назва обов’язкова (до 128 символів)', 'unknown connector kind': 'невідомий вид конектора',
  'secret ref must look like env://NAME (a reference, never the secret itself)': 'посилання на секрет має мати вигляд env://NAME (це посилання, а не сам секрет)',
  'config must be a JSON object': 'конфігурація має бути JSON-об’єктом', 'config has too many keys (max 32)': 'у конфігурації забагато ключів (макс. 32)',
  'Publish replay': 'Опублікувати повтор', 'I understand and confirm the publication': 'Я розумію наслідки й підтверджую публікацію',
  'Replay queued: job {job}, {count} record(s). The action was written to the audit log.': 'Повтор поставлено в чергу: завдання {job}, записів: {count}. Дію записано в журнал аудиту.',
  'This publishes {count} quarantined record(s) of source {source} ({from} – {to}) to the pipeline version {version}. It cannot be undone.': 'Буде опубліковано записів із карантину: {count} (джерело {source}, {from} – {to}) у версію пайплайна {version}. Скасувати це неможливо.',
  'Requires the separate replay-publish permission; the action is audited.': 'Потрібне окреме право на публікацію повторів; дія записується в аудит.',
  'No jobs.': 'Завдань немає.', 'No sources.': 'Джерел немає.', 'File API is not running or not reachable': 'File API не запущений або недоступний',
  'Read-only list of file processing jobs. Content labels show counts only, never values.': 'Список завдань обробки файлів лише для читання. Мітки вмісту показують лише кількість, без значень.',
  'Read-only list. Connection settings and credentials are never shown.': 'Список лише для читання. Налаштування підключення та облікові дані не показуються.',
  'attempts': 'спроби', 'processed': 'оброблено', 'failed': 'з помилкою', 'finished': 'завершено', 'updated': 'оновлено', 'connectors': 'конектори',
  'Sign in again.': 'Увійдіть знову.',
  'Check the token in the box above.': 'Перевірте токен у полі вгорі.',
  'trace': 'trace',
  'size': 'розмір', '{n} fields': 'полів: {n}', 'retention': 'зберігання', 'export': 'експорт', 'allowed': 'дозволено', 'not allowed': 'заборонено',
  'B': 'Б', 'KiB': 'КіБ', 'MiB': 'МіБ', 'GiB': 'ГіБ',
};

let lang: Lang = initial();
function initial(): Lang {
  try {
    const s = localStorage.getItem(KEY);
    if (s === 'en' || s === 'ua') return s;
  } catch { /* storage blocked */ }
  const nav = typeof navigator !== 'undefined' ? (navigator.language || '').toLowerCase() : '';
  return nav.startsWith('uk') ? 'ua' : 'en';
}
export const getLang = (): Lang => lang;
export function setLang(l: Lang) {
  lang = l;
  try { localStorage.setItem(KEY, l); } catch { /* ignore */ }
  applyLang();
}
export function applyLang() {
  document.documentElement.lang = LANGS.find((x) => x.id === lang)!.htmlLang;
}
const fill = (s: string, vars?: Record<string, string | number>) => (vars ? s.replace(/\{(\w+)\}/g, (m, k: string) => (k in vars ? String(vars[k]) : m)) : s);

/** Translate an English UI string (with optional {placeholders}); unknown strings are returned unchanged. */
export function t(en: string, vars?: Record<string, string | number>): string {
  return fill(lang === 'ua' ? (UA[en] ?? en) : en, vars);
}
/** For messages that may come from library code ("tag "x" must be key=value", "part 2 PUT failed (500)"). */
export function tMsg(msg: string): string {
  if (lang === 'en') return msg;
  if (UA[msg]) return UA[msg];
  let m = /^tag "(.*)" must be key=value$/.exec(msg);
  if (m) return `тег «${m[1]}» має бути у форматі ключ=значення`;
  m = /^file larger than (\d+) MiB: use a CLI uploader$/.exec(msg);
  if (m) return `файл більший за ${m[1]} МіБ: скористайтеся CLI-завантажувачем`;
  m = /^part (\d+) PUT failed \((\d+)\)$/.exec(msg);
  if (m) return `не вдалося завантажити частину ${m[1]} (${m[2]})`;
  return msg;
}
