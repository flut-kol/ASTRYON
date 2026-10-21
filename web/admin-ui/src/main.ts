import './style.css';
import { clear, h, svg } from './dom';
import { errorText, fmtBytes, fmtTime, human, timeEl } from './labels';
import { applyLang, getLang, LANGS, setLang, t, tMsg } from './i18n';
import { applyTheme, getTheme, setTheme, THEMES, type Theme } from './theme';
import { getSource, getToken, loginUrl, setSource, setToken, takeTokenFromHash } from './auth';
import { ApiError, type Ctx } from './http';
import { parseProm } from './prom';
import { getUpload, listUploads, parseTags, previewUpload, SENSITIVITIES, SOURCE_TYPES, STATUSES, uploadFile, type UploadItem } from './upload';
import { fmtNum, lastHours, localToRfc3339, metricsSummary, qualitySummary, queryEvents } from './query';
import { CONNECTOR_KINDS, createConnector, listConnectors, parseConfig, setConnectorEnabled, validateConnector } from './connectors';
import { createExport, getExport, isTerminal, knownExports, rememberExport, validateSpec, type ExportJob } from './exports';
import { canPublish, listQuarantine, planReplay, publishReplay, type ReplayRequest } from './quarantine';
import { listJobs, retryUpload } from './jobs';
import { listSources } from './sources';
import { listReplays, REPLAY_STATUSES } from './replays';
import { searchText } from './textSearch';

const env = import.meta.env;
const B = {
  gateway: env.VITE_GATEWAY_BASE ?? '/api/gateway',
  files: env.VITE_FILES_BASE ?? '/api/files',
  query: env.VITE_QUERY_BASE ?? '/api/query',
  connectors: env.VITE_CONNECTORS_BASE ?? '/api/connectors',
  exports: env.VITE_EXPORTS_BASE ?? '/api/exports',
  quarantine: env.VITE_QUARANTINE_BASE ?? '/api/quarantine',
  replays: env.VITE_REPLAYS_BASE ?? '/api/replays',
  s3Proxy: env.VITE_S3_PROXY ?? (env.DEV ? '/api/s3' : undefined),
};
const BFF = env.VITE_AUTH_MODE === 'bff';
const ctx: Ctx = { getToken, bff: BFF };

applyTheme();
applyLang();

const frag = takeTokenFromHash(location.hash);
if (frag) { setToken(frag); history.replaceState(null, '', location.pathname + location.search + '#/'); }

const ROUTES: [string, string, (el: HTMLElement) => void | Promise<void>][] = [
  ['overview', 'Overview', overview], ['uploads', 'Uploads', uploads], ['events', 'Events', events],
  ['quality', 'Quality', quality], ['connectors', 'Connectors', connectors], ['exports', 'Exports', exportsPage], ['quarantine', 'Quarantine', quarantinePage], ['replays', 'Replays', replaysPage], ['jobs', 'Jobs', jobsPage], ['sources', 'Sources', sourcesPage],
  ['files', 'File API', filesPage], ['search', 'Text search', textSearchPage], ['access', 'Access & roles', accessPage],
];

function na(what: string) { return h('div', { class: 'na' }, t('{what}: API not available in this build.', { what })); }
function errBox(e: unknown): HTMLElement {
  if (e instanceof ApiError) {
    const nice = errorText(e.code, e.status);
    const hint = e.status === 401 || e.status === 403 ? ` ${BFF ? t('Sign in again.') : t('Check the token in the box above.')}` : '';
    const tech = [e.code, e.message && e.message !== e.code ? e.message : '', e.traceId ? `${t('trace')} ${e.traceId}` : ''].filter(Boolean).join(' · ');
    return h('div', { class: 'err', role: 'alert' }, nice ? `${nice}${hint}` : e.message || e.code, nice || hint ? h('div', { class: 'mut small' }, tech) : e.traceId ? ` [${t('trace')} ${e.traceId}]` : '');
  }
  return h('div', { class: 'err', role: 'alert' }, e instanceof Error ? tMsg(e.message) : String(e));
}
// Async result areas announce their content politely (loading, results, errors) to screen readers.
const live = (cls = '') => h('div', { role: 'status', 'aria-live': 'polite', ...(cls ? { class: cls } : {}) });
const field = (label: string, input: HTMLElement) => h('label', {}, t(label), input);
function table(head: string[], rows: (string | number | Node | (string | Node)[])[][], caption = '') {
  return h('table', {}, caption ? h('caption', { class: 'sr-only' }, t(caption)) : null,
    h('thead', {}, h('tr', {}, head.map((x) => h('th', { scope: 'col' }, x ? t(x) : h('span', { class: 'sr-only' }, t('actions')))))),
    h('tbody', {}, rows.map((r) => h('tr', {}, r.map((c) => h('td', {}, c))))));
}
function sourceInput() { return h('input', { size: '38', placeholder: t('source uuid'), value: getSource() }); }

async function overview(el: HTMLElement) {
  const out = live(); out.append(t('Loading...'));
  el.append(h('div', { class: 'page-hero' },
    h('div', { class: 'eyebrow' }, t('Operations console')),
    h('h2', {}, t('Overview')),
    h('p', { class: 'hero-copy' }, t('A calm, tenant-safe view of ingestion, processing and data quality.')),
  ), out);
  const kp = (l: string, v: string, cls = '', detail = '') => h('div', { class: 'card kpi' },
    h('span', { class: 'kpi-label' }, l), h('b', { class: cls }, v), detail ? h('span', { class: 'kpi-detail' }, detail) : null);
  const kpis: Node[] = [];
  try {
    const r = await fetch(`${B.gateway}/healthz`);
    const j = (await r.json()) as { status?: string };
    kpis.push(kp(t('Gateway health'), human('status', String(j.status ?? r.status)), r.ok ? 'ok' : 'err'));
  } catch { kpis.push(kp(t('Gateway health'), t('unreachable'), 'err')); }
  let counters: Node = h('span', { class: 'mut' }, t('metrics not available'));
  try {
    const r = await fetch(`${B.gateway}/metrics`);
    if (r.ok) {
      const metrics = parseProm(await r.text()).filter((x) => x.name.startsWith('astryon_') && x.name.endsWith('_total'));
      const total = metrics.find((x) => x.name === 'astryon_gateway_ack_total' && !Object.keys(x.labels).length)?.value ?? 0;
      const errors = metrics.find((x) => x.name === 'astryon_gateway_ack_errors_total' && !Object.keys(x.labels).length)?.value ?? 0;
      kpis.push(kp(t('Accepted events'), fmtNum(total), 'ok', t('gateway acknowledgements')));
      kpis.push(kp(t('Ack errors'), fmtNum(errors), errors ? 'err' : 'ok', t('last exposed counter')));
      const s = metrics;
      counters = s.length ? table(['metric', 'labels', 'value'], s.map((x) => [x.name, Object.entries(x.labels).map(([k, v]) => `${k}=${v}`).join(' '), fmtNum(x.value)]), 'Gateway counters') : h('span', { class: 'mut' }, t('no counters yet'));
    }
  } catch { /* leave default */ }
  if (!kpis.length) kpis.push(kp(t('Accepted events'), '—', '', t('waiting for gateway metrics')), kp(t('Ack errors'), '—', '', t('waiting for gateway metrics')));
  clear(out);
  const service = (name: string, state: string, cls: string, note: string) => h('div', { class: 'service-row' },
    h('span', { class: `status-dot ${cls}` }), h('div', { class: 'service-copy' }, h('b', {}, name), h('span', { class: 'mut small' }, note)), h('span', { class: `status-pill ${cls}` }, state));
  out.append(h('div', { class: 'kpis' }, kpis),
    h('div', { class: 'overview-grid' },
      h('div', { class: 'card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('System health')), h('p', { class: 'mut small' }, t('Live reachability from this session'))), h('span', { class: 'live-badge' }, h('span', { class: 'status-dot ok' }), t('Live'))),
        h('div', { class: 'service-list' },
          service('Gateway', t('Healthy'), 'ok', t('Ingestion and public metrics')),
          service('Query API', t('Available on demand'), 'neutral', t('Open Events or Quality to check')),
          service('File API', t('Available on demand'), 'neutral', t('Open Uploads or Jobs to check')),
          service('Control plane', t('Protected'), 'neutral', t('Connectors, sources and replays')))),
      h('div', { class: 'card quick-card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('Quick actions')), h('p', { class: 'mut small' }, t('Common operator workflows')))),
        h('div', { class: 'quick-actions' },
          h('a', { class: 'quick-action', href: '#/uploads' }, svg(ICONS.uploads, 18), h('span', {}, h('b', {}, t('Upload data')), h('small', {}, t('CSV, NDJSON or Parquet')))),
          h('a', { class: 'quick-action', href: '#/events' }, svg(ICONS.events, 18), h('span', {}, h('b', {}, t('Explore events')), h('small', {}, t('Tenant-scoped search')))),
          h('a', { class: 'quick-action', href: '#/quality' }, svg(ICONS.quality, 18), h('span', {}, h('b', {}, t('Review quality')), h('small', {}, t('Metrics and flags')))),
          h('a', { class: 'quick-action', href: '#/quarantine' }, svg(ICONS.quarantine, 18), h('span', {}, h('b', {}, t('Handle quarantine')), h('small', {}, t('Dry-run before replay')))),
        ))),
    h('div', { class: 'card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('Gateway counters')), h('p', { class: 'mut small' }, t('Public /metrics, no tenant labels')))), counters),
    h('div', { class: 'callout' }, h('span', { class: 'callout-icon' }, 'i'), h('div', {}, h('b', {}, t('Privacy by design')), h('p', { class: 'mut small' }, t('Tenant identity comes from the validated token. The UI never sends tenant_id in a query, form or header.')))));
}

function uploadForm(onDone: () => void) {
  const src = sourceInput();
  const st = h('select', {}, SOURCE_TYPES.map((x) => h('option', { value: x }, human('sourceType', x))));
  const sens = h('select', {}, h('option', { value: '' }, t('(profile default)')), SENSITIVITIES.map((x) => h('option', { value: x }, human('sensitivity', x))));
  const tags = h('input', { size: '30', placeholder: 'purpose=demo,team=ops' });
  const log = live('mut');
  const fileIn = h('input', { type: 'file', 'aria-label': t('Choose CSV or NDJSON files to upload'), accept: '.csv,.ndjson,.jsonl,text/csv,application/x-ndjson', multiple: true });
  const drop = h('div', { class: 'drop', role: 'group', 'aria-label': t('File drop zone (or use the file chooser)') }, t('Drop CSV / NDJSON files here or '), fileIn);
  const run = async (files: File[]) => {
    clear(log);
    let tg: Record<string, string>;
    try { tg = parseTags(tags.value); } catch (e) { log.append(errBox(e)); return; }
    setSource(src.value);
    for (const f of files) {
      const line = h('div', {}, `${f.name}: `); const msg = h('span', {}, t('starting')); line.append(msg); log.append(line);
      try {
        const r = await uploadFile(ctx, B.files, { file: f, source_id: src.value.trim(), source_type: st.value, sensitivity: sens.value || undefined, tags: tg }, { s3Proxy: B.s3Proxy, onProgress: (m) => { msg.textContent = m; } });
        msg.textContent = t('queued, file {file}, job {job}', { file: r.file_object_id, job: r.job_id }); msg.className = 'ok';
      } catch (e) { msg.replaceWith(errBox(e)); }
    }
    onDone();
  };
  drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('over'); });
  drop.addEventListener('dragleave', () => drop.classList.remove('over'));
  drop.addEventListener('drop', (e) => { e.preventDefault(); drop.classList.remove('over'); void run([...(e.dataTransfer?.files ?? [])]); });
  fileIn.addEventListener('change', () => { void run([...(fileIn.files ?? [])]); fileIn.value = ''; });
  return h('div', { class: 'card' }, h('h3', {}, t('Upload')),
    h('div', { class: 'row' }, field('Source ID', src), field('Source type', st), field('Sensitivity', sens), field('Tags (k=v,...)', tags)), drop, log);
}

async function uploads(el: HTMLElement) {
  el.append(h('h2', {}, t('Uploads')));
  const list = live(); const detail = live();
  const f = {
    source_type: h('select', {}, h('option', { value: '' }, t('any')), SOURCE_TYPES.map((x) => h('option', { value: x }, human('sourceType', x)))),
    sensitivity: h('select', {}, h('option', { value: '' }, t('any')), SENSITIVITIES.map((x) => h('option', { value: x }, human('sensitivity', x)))),
    status: h('select', {}, h('option', { value: '' }, t('any')), STATUSES.map((x) => h('option', { value: x }, human('status', x)))),
    label: h('select', {}, h('option', { value: '' }, t('any')), ['contains_geo', 'contains_credentials', 'contains_pii'].map((x) => h('option', { value: x }, human('label', x)))),
    tag: h('input', { size: '16', placeholder: 'key=value' }),
  };
  let cursors: string[] = []; // stack of cursors for prev/next
  const load = async (cursor?: string) => {
    clear(list); list.append(t('Loading...'));
    try {
      const q: Record<string, string> = {};
      for (const [k, v] of Object.entries(f)) if (v.value) q[k] = v.value;
      if (cursor) q.cursor = cursor;
      const p = await listUploads(ctx, B.files, q);
      clear(list);
      const rows = p.items.map((u) => [
        h('button', { class: 'lnk', 'aria-label': t('Show upload {id}', { id: u.file_object_id }), onclick: () => void showDetail(u.file_object_id) }, u.file_object_id.slice(0, 8)),
        human('sourceType', u.source_type), human('sensitivity', u.sensitivity), u.committed ? t('yes') : t('no'), human('status', u.job?.status) || '-', u.job?.error ? (errorText(u.job.error, 0) ?? human('reason', u.job.error)) : '', Object.keys(u.job?.labels ?? {}).map((k) => human('label', k)).join(', ') || '-', fmtBytes(u.size_bytes),
        Object.entries(u.tags).map(([k, v]) => `${k}=${v}`).join(' '), timeEl(u.created_at),
      ]);
      list.append(rows.length ? table(['id', 'type', 'sensitivity', 'committed', 'status', 'error', 'labels', 'bytes', 'tags', 'created'], rows, 'Uploads') : h('div', { class: 'mut' }, t('No uploads match.')));
      const nav = h('div', { class: 'row' });
      if (cursors.length) nav.append(h('button', { onclick: () => { cursors.pop(); void load(cursors.at(-1)); } }, t('Newer')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { cursors.push(p.next_cursor!); void load(p.next_cursor); } }, t('Older')));
      list.append(nav);
    } catch (e) { clear(list); list.append(errBox(e)); }
  };
  const showDetail = async (id: string) => {
    clear(detail);
    try { detail.append(detailCard(await getUpload(ctx, B.files, id))); } catch (e) { detail.append(errBox(e)); }
  };
  const apply = () => { cursors = []; void load(); };
  el.append(uploadForm(apply),
    h('div', { class: 'card' }, h('div', { class: 'row' },
      field('Source type', f.source_type), field('Sensitivity', f.sensitivity), field('Status', f.status), field('Label', f.label), field('Tag', f.tag),
      h('button', { class: 'pri', onclick: apply }, t('Filter'))), list), detail);
  await load();
}

function detailCard(u: UploadItem) {
  const j = u.job;
  const quarantined = j?.status === 'QUARANTINED';
  const previewOut = live('preview-output');
  const previewButton = h('button', { type: 'button', class: 'pri', onclick: async () => {
    clear(previewOut); previewOut.append(t('Loading preview...'));
    try {
      const p = await previewUpload(ctx, B.files, u.file_object_id); clear(previewOut);
      if (p.mode === 'content') previewOut.append(h('pre', { class: 'file-preview' }, p.content ?? ''));
      else previewOut.append(h('div', { class: 'callout' }, h('b', {}, t('Metadata only')), h('p', { class: 'mut small' }, p.reason ?? t('Content is hidden by policy or this file type is unsupported.'))));
    } catch (e) { clear(previewOut); previewOut.append(errBox(e)); }
  } }, t('View safe preview'));
  return h('div', { class: 'card' }, h('h3', {}, t('Upload {id}', { id: u.file_object_id })),
    table(['field', 'value'], [
      [t('source'), u.source_id], [t('source type'), human('sourceType', u.source_type)], [t('sensitivity'), human('sensitivity', u.sensitivity)], [t('committed'), u.committed ? t('yes') : t('no')], [t('size'), `${fmtBytes(u.size_bytes)} (${fmtNum(u.size_bytes)} ${t('bytes')})`],
      [t('tags'), Object.entries(u.tags).map(([k, v]) => `${k}=${v}`).join(' ') || '-'],
      [t('job'), j?.id ?? t('not completed yet')],
      [t('status'), h('span', { class: quarantined ? 'warn' : j?.status === 'FAILED' ? 'err' : '' }, human('status', j?.status) || '-')],
      [t('error'), j?.error ? (errorText(j.error, 0) ?? human('reason', j.error)) : '-'],
      [t('content labels (counts only)'), Object.entries(j?.labels ?? {}).map(([k, v]) => `${human('label', k)}: ${v.count} (${t('{n} fields', { n: v.fields.length })})`).join('; ') || '-'],
      [t('profile'), u.profile ? `${t('retention')}: ${human('retention', u.profile.retention_class) || '-'}; ${t('export')}: ${u.profile.export_allowed === undefined ? '-' : u.profile.export_allowed ? t('allowed') : t('not allowed')}` : '-'],
    ], 'Upload details'), quarantined ? h('p', { class: 'warn' }, t('Held back by content policy: nothing reached the sink.')) : null,
    h('div', { class: 'preview-panel' }, h('div', { class: 'card-heading' }, h('div', {}, h('h4', {}, t('File content')), h('p', { class: 'mut small' }, t('Only bounded, safe text is shown; sensitive values remain hidden.'))), previewButton), previewOut));
}

function rangeControls() {
  const d = lastHours(24);
  const loc = (iso: string) => { const x = new Date(iso); x.setMinutes(x.getMinutes() - x.getTimezoneOffset()); return x.toISOString().slice(0, 16); };
  return {
    from: h('input', { type: 'datetime-local', value: loc(d.from) }), to: h('input', { type: 'datetime-local', value: loc(d.to) }),
    source: sourceInput(), pv: h('input', { size: '6', placeholder: 'e.g. 1', value: env.VITE_DEFAULT_PROCESSING_VERSION ?? '' }),
  };
}
const sourceNote = () => h('p', { class: 'mut' }, t('The query API requires a source id unless a processing version is given (no tenant-wide default). If the source has no active version, enter a processing version.'));

async function events(el: HTMLElement) {
  el.append(h('h2', {}, t('Events')));
  const c = rangeControls();
  const et = h('input', { size: '16' }); const en = h('input', { size: '16' });
  const out = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(out); out.append(t('Loading...'));
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    if (!from || !to) { clear(out); out.append(errBox(new Error('invalid time range'))); return; }
    setSource(c.source.value);
    try {
      const p = await queryEvents(ctx, B.query, { from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value ? Number(c.pv.value) : undefined, event_type: et.value.trim(), entity_id: en.value.trim(), cursor });
      clear(out);
      out.append(p.items.length ? table(['time', 'type', 'entity', 'flags', 'event id', 'payload'], p.items.map((e) => [timeEl(e.event_time), e.event_type, e.entity_id ?? '', (e.quality_flags ?? []).map((f) => human('flag', f)).join(', '), e.event_id, (e.payload ?? '').slice(0, 120)]), 'Events') : h('div', { class: 'mut' }, t('No events.')));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Previous')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Next')));
      out.append(nav);
    } catch (e) { clear(out); out.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, sourceNote(), h('div', { class: 'row' },
    field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), field('Event type', et), field('Entity', en),
    h('button', { class: 'pri', onclick: () => { stack = []; void load(); } }, t('Search'))), out));
}

async function quality(el: HTMLElement) {
  el.append(h('h2', {}, t('Quality and metrics')));
  const c = rangeControls(); const out = live();
  const load = async () => {
    clear(out); out.append(t('Loading...'));
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    if (!from || !to) { clear(out); out.append(errBox(new Error('invalid time range'))); return; }
    setSource(c.source.value);
    const r = { from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value || undefined };
    const [m, q] = await Promise.allSettled([metricsSummary(ctx, B.query, r), qualitySummary(ctx, B.query, r)]);
    clear(out);
    const grid = (title: string, rows: Record<string, unknown>[] | undefined) => {
      const cols = [...new Set((rows ?? []).flatMap((x) => Object.keys(x)))];
      return h('div', { class: 'card' }, h('h3', {}, t(title)), rows?.length ? table(cols, rows.map((x) => cols.map((k) => (typeof x[k] === 'object' ? JSON.stringify(x[k]) : fmtNum(x[k])))), title) : h('span', { class: 'mut' }, t('no data')));
    };
    if (m.status === 'fulfilled') out.append(grid('Per-source metrics', m.value.sources)); else out.append(errBox(m.reason));
    if (q.status === 'fulfilled') out.append(grid('Per-source quality', q.value.sources), grid('Quality flags', q.value.flags)); else out.append(errBox(q.reason));
  };
  el.append(h('div', { class: 'card' }, sourceNote(), h('div', { class: 'row' }, field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), h('button', { class: 'pri', onclick: () => void load() }, t('Load')))), out);
}

async function connectors(el: HTMLElement) {
  el.append(h('h2', {}, t('Connectors')));
  const out = live();
  const src = sourceInput();
  const nm = h('input', { size: '20', maxlength: '128' });
  const kd = h('select', {}, CONNECTOR_KINDS.map((x) => h('option', { value: x }, human('kind', x))));
  const sr = h('input', { size: '26', placeholder: 'env://NAME', autocomplete: 'off' });
  const cfg = h('input', { size: '26', placeholder: '{"url":"https://..."}' });
  const msg = live();
  const create = async () => {
    clear(msg);
    let config: Record<string, unknown>;
    try { config = parseConfig(cfg.value); } catch (e) { msg.append(h('div', { class: 'err', role: 'alert' }, e instanceof SyntaxError ? t('config is not valid JSON') : tMsg((e as Error).message))); return; }
    const n = { source_id: src.value.trim(), name: nm.value, kind: kd.value, secret_ref: sr.value.trim(), config };
    const bad = validateConnector(n);
    if (bad) { msg.append(h('div', { class: 'err', role: 'alert' }, tMsg(bad))); return; }
    setSource(src.value);
    try { const c = await createConnector(ctx, B.connectors, n); msg.append(h('div', { class: 'ok' }, t('connector created: {name}', { name: c.name }))); nm.value = ''; sr.value = ''; cfg.value = ''; await load(); } catch (e) { msg.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('h3', {}, t('New connector')), h('p', { class: 'mut' }, t('The secret is entered as a reference (for example env://NAME), never as the secret value. The tenant comes from your token.')),
    h('div', { class: 'row' }, field('Source ID', src), field('Name', nm), field('Kind', kd), field('Secret reference', sr), field('Config (JSON, optional)', cfg), h('button', { class: 'pri', onclick: () => void create() }, t('Create'))), msg), out);
  const load = async () => {
    clear(out);
    try {
      const p = await listConnectors(ctx, B.connectors);
      const toggle = (x: { id: string; enabled: boolean; name?: string }) => h('button', {
        'aria-label': t(x.enabled ? 'Pause connector {name}' : 'Resume connector {name}', { name: x.name ?? x.id }),
        onclick: async () => {
          try { await setConnectorEnabled(ctx, B.connectors, x.id, !x.enabled); await load(); } catch (e) { out.prepend(errBox(e)); }
        },
      }, t(x.enabled ? 'Pause' : 'Resume'));
      out.append(p.items.length ? table(['name', 'kind', 'enabled', 'source', 'secret ref', 'created', ''], p.items.map((x) => [x.name, human('kind', x.kind), h('span', { class: x.enabled ? 'ok' : 'warn' }, x.enabled ? t('yes') : t('no')), x.source_id, x.secret_ref ?? '', timeEl(x.created_at), toggle(x)]), 'Connectors') : h('div', { class: 'mut' }, t('No connectors.')));
    } catch (e) {
      if (e instanceof ApiError && e.unavailable) out.append(na(t('Connector manager is not running or not reachable')));
      else out.append(errBox(e));
    }
  };
  await load();
}

async function exportsPage(el: HTMLElement) {
  el.append(h('h2', {}, t('Exports')));
  const c = rangeControls();
  const fmt = h('select', {}, h('option', { value: 'ndjson' }, 'ndjson'), h('option', { value: 'csv' }, 'csv'));
  const et = h('input', { size: '16' }); const en = h('input', { size: '16' }); const mr = h('input', { size: '9', placeholder: '1000000' });
  const msg = live(); const list = live();
  const jobs = new Map<string, ExportJob | Error>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const draw = () => {
    clear(list);
    const ids = knownExports();
    if (!ids.length) { list.append(h('div', { class: 'mut' }, t('No exports started from this browser session. The API has no list endpoint; jobs are tracked by id.'))); return; }
    list.append(table(['id', 'status', 'rows', 'bytes', 'format', 'error', 'download'], ids.map((id) => {
      const j = jobs.get(id);
      if (!j) return [id, t('loading'), '', '', '', '', ''];
      if (j instanceof Error) return [id, h('span', { class: 'err' }, tMsg(j.message)), '', '', '', '', ''];
      return [id, h('span', { class: j.status === 'FAILED' ? 'err' : j.status === 'SUCCEEDED' ? 'ok' : '' }, human('status', j.status)), fmtNum(j.rows), fmtBytes(j.bytes), j.format.toUpperCase(), j.error_code ? (errorText(j.error_code, 0) ?? human('reason', j.error_code)) : '',
        j.download_url ? h('a', { href: j.download_url, rel: 'noopener' }, t('download (until {until})', { until: fmtTime(j.download_expires_at) || '?' })) : ''];
    }), 'Exports'));
  };
  const refresh = async () => {
    await Promise.all(knownExports().map(async (id) => {
      const cur = jobs.get(id);
      if (cur && !(cur instanceof Error) && isTerminal(cur) && !cur.download_url) return;
      try { jobs.set(id, await getExport(ctx, B.exports, id)); } catch (e) { jobs.set(id, e instanceof Error ? e : new Error(String(e))); }
    }));
    if (!list.isConnected) return;
    draw();
    const open = [...jobs.values()].some((j) => !(j instanceof Error) && !isTerminal(j));
    if (open) timer = setTimeout(() => void refresh(), 3000);
  };
  const start = async () => {
    clear(msg);
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    const spec = { from: from ?? '', to: to ?? '', processing_version: Number(c.pv.value), format: fmt.value as 'csv' | 'ndjson', source_id: c.source.value.trim(), event_type: et.value.trim(), entity_id: en.value.trim(), max_rows: mr.value ? Number(mr.value) : undefined };
    const bad = c.pv.value === '' ? 'processing version is required' : validateSpec(spec);
    if (bad) { msg.append(h('div', { class: 'err', role: 'alert' }, tMsg(bad))); return; }
    setSource(c.source.value);
    try {
      const r = await createExport(ctx, B.exports, spec);
      rememberExport(r.id); msg.append(h('div', { class: 'ok' }, t('queued {id}', { id: r.id })));
      clearTimeout(timer); void refresh();
    } catch (e) { msg.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, t('Range up to 31 days; processing version is required. The tenant comes from your token. Download links are short-lived and fetched again on refresh.')),
    h('div', { class: 'row' }, field('From', c.from), field('To', c.to), field('Source ID (optional)', c.source), field('Proc. version', c.pv), field('Format', fmt), field('Event type', et), field('Entity', en), field('Max rows', mr),
      h('button', { class: 'pri', onclick: () => void start() }, t('Start export'))), msg),
    h('div', { class: 'card' }, h('div', { class: 'row' }, h('button', { onclick: () => { clearTimeout(timer); void refresh(); } }, t('Refresh'))), list));
  draw(); await refresh();
}

async function jobsPage(el: HTMLElement) {
  el.append(h('h2', {}, t('Jobs')));
  const list = live(); let stack: string[] = [];
  const src = sourceInput();
  const st = h('select', {}, h('option', { value: '' }, t('any')), STATUSES.map((x) => h('option', { value: x }, human('status', x))));
  const load = async (cursor?: string) => {
    clear(list); list.append(t('Loading...'));
    try {
      const p = await listJobs(ctx, B.files, { source_id: src.value.trim() || undefined, status: st.value || undefined, cursor });
      clear(list);
      list.append(p.items.length ? table(['job', 'type', 'status', 'source', 'attempts', 'processed', 'failed', 'error', 'labels', 'created', 'finished', 'actions'],
        p.items.map((j) => [j.id, human('kind', j.kind), h('span', { class: j.status === 'FAILED' ? 'err' : j.status === 'SUCCEEDED' ? 'ok' : j.status === 'QUARANTINED' ? 'warn' : '' }, human('status', j.status)),
          j.source_id ?? '', `${j.attempt_count}/${j.max_attempts}`, fmtNum(j.events_processed), fmtNum(j.events_failed), j.error ? (errorText(j.error, 0) ?? human('reason', j.error)) : '',
          Object.entries(j.labels ?? {}).map(([k, n]) => `${human('label', k)}: ${n}`).join(', ') || '-', timeEl(j.created_at), timeEl(j.finished_at),
          j.file_object_id && (j.status === 'FAILED' || j.status === 'QUARANTINED') ? (() => {
            const b = h('button', {}, t('Retry'));
            b.onclick = async () => { b.setAttribute('disabled', 'true'); try { const r = await retryUpload(ctx, B.files, j.file_object_id!); b.replaceWith(h('span', { class: 'ok' }, r.mode === 'archive_inspect' ? t('Inspected') : t('Queued'))); } catch (e) { b.removeAttribute('disabled'); list.append(errBox(e)); } };
            return b;
          })() : '-']), 'Jobs') : h('div', { class: 'mut' }, t('No jobs.')));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Newer')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Older')));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na(t('File API is not running or not reachable')) : errBox(e));
    }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, t('Read-only list of file processing jobs. Content labels show counts only, never values.')),
    h('div', { class: 'row' }, field('Source ID', src), field('Status', st), h('button', { class: 'pri', onclick: () => { stack = []; setSource(src.value); void load(); } }, t('Filter'))), list));
  await load();
}

async function sourcesPage(el: HTMLElement) {
  el.append(h('h2', {}, t('Sources')));
  const list = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(list); list.append(t('Loading...'));
    try {
      const p = await listSources(ctx, B.connectors, cursor);
      clear(list);
      list.append(p.items.length ? table(['id', 'name', 'type', 'enabled', 'connectors', 'created', 'updated'],
        p.items.map((x) => [x.id, x.name, human('kind', x.kind), h('span', { class: x.enabled ? 'ok' : 'warn' }, x.enabled ? t('yes') : t('no')), fmtNum(x.connector_count), timeEl(x.created_at), timeEl(x.updated_at)]), 'Sources') : h('div', { class: 'mut' }, t('No sources.')));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Previous')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Next')));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na(t('Connector manager is not running or not reachable')) : errBox(e));
    }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, t('Read-only list. Connection settings and credentials are never shown.')), list));
  await load();
}

async function filesPage(el: HTMLElement) {
  el.append(h('div', { class: 'page-hero' },
    h('div', { class: 'eyebrow' }, t('Ingestion')),
    h('h2', {}, t('File API')),
    h('p', { class: 'hero-copy' }, t('Upload intake, processing jobs and content-policy outcomes in one place.'))));
  const out = live(); out.append(t('Loading...'));
  try {
    const [uploadsPage, jobsPageData] = await Promise.all([
      listUploads(ctx, B.files, { limit: '8' }),
      listJobs(ctx, B.files, { cursor: undefined }),
    ]);
    const uploadsCount = uploadsPage.items.length;
    const jobs = jobsPageData.items;
    const succeeded = jobs.filter((j) => j.status === 'SUCCEEDED').length;
    const held = jobs.filter((j) => j.status === 'QUARANTINED').length;
    const failed = jobs.filter((j) => j.status === 'FAILED').length;
    const card = (label: string, value: string, tone = '') => h('div', { class: 'card kpi' }, h('span', { class: 'kpi-label' }, label), h('b', { class: tone }, value));
    clear(out);
    out.append(h('div', { class: 'kpis' }, card(t('Recent uploads'), fmtNum(uploadsCount)), card(t('Succeeded jobs'), fmtNum(succeeded), 'ok'), card(t('Quarantined'), fmtNum(held), held ? 'warn' : 'ok'), card(t('Failed jobs'), fmtNum(failed), failed ? 'err' : 'ok')),
      h('div', { class: 'overview-grid' },
        h('div', { class: 'card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('Recent uploads')), h('p', { class: 'mut small' }, t('Open Uploads for filters and detail'))), h('a', { class: 'text-link', href: '#/uploads' }, t('View all'))),
          uploadsCount ? table(['id', 'type', 'status', 'size', 'created'], uploadsPage.items.map((u) => [h('a', { href: '#/uploads', class: 'text-link' }, u.file_object_id.slice(0, 12)), human('sourceType', u.source_type), h('span', { class: u.job?.status === 'FAILED' ? 'err' : u.job?.status === 'QUARANTINED' ? 'warn' : 'ok' }, human('status', u.job?.status) || t('pending')), fmtBytes(u.size_bytes), timeEl(u.created_at)]), 'Recent file uploads') : h('span', { class: 'mut' }, t('No uploads yet.'))),
        h('div', { class: 'card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('Processing jobs')), h('p', { class: 'mut small' }, t('Read-only operational snapshot'))), h('a', { class: 'text-link', href: '#/jobs' }, t('View jobs'))),
          jobs.length ? table(['job', 'status', 'processed', 'failed'], jobs.slice(0, 8).map((j) => [j.id.slice(0, 12), h('span', { class: j.status === 'FAILED' ? 'err' : j.status === 'QUARANTINED' ? 'warn' : j.status === 'SUCCEEDED' ? 'ok' : '' }, human('status', j.status)), fmtNum(j.events_processed), fmtNum(j.events_failed)]), 'Recent processing jobs') : h('span', { class: 'mut' }, t('No jobs yet.')))),
      h('div', { class: 'callout' }, h('span', { class: 'callout-icon' }, 'i'), h('div', {}, h('b', {}, t('File safety')), h('p', { class: 'mut small' }, t('Labels show counts only. Credentials and personal-data values never appear in the admin UI.')))));
  } catch (e) { clear(out); out.append(e instanceof ApiError && e.unavailable ? na(t('File API is not running or not reachable')) : errBox(e)); }
}

async function textSearchPage(el: HTMLElement) {
  el.append(h('div', { class: 'page-hero' },
    h('div', { class: 'eyebrow' }, t('ClickHouse explorer')),
    h('h2', {}, t('Text search')),
    h('p', { class: 'hero-copy' }, t('Find indexed tokens in normalized file content without writing SQL.'))));
  const c = rangeControls(); const q = h('input', { size: '34', maxlength: '128', placeholder: t('word or token'), autocomplete: 'off' }); const out = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(out); out.append(t('Searching...'));
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value), text = q.value.trim();
    if (!from || !to || !text) { clear(out); out.append(h('div', { class: 'err', role: 'alert' }, t('A search term and time range are required.'))); return; }
    try {
      const p = await searchText(ctx, B.query, { text, from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value ? Number(c.pv.value) : undefined, cursor });
      clear(out);
      if (p.items.length) {
        out.append(table(['time', 'type', 'entity', 'source', 'event', 'match'], p.items.map((x) => [timeEl(x.event_time), x.event_type ?? '-', x.entity_id ?? '-', x.source_id, x.event_id, x.snippet ?? text]), 'Text search results'),
          h('p', { class: 'mut small' }, t('Search is tenant-scoped and backed by the ClickHouse token index.')));
      } else out.append(h('div', { class: 'mut' }, t('No indexed matches.')));
      const nav = h('div', { class: 'row' }); if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Previous'))); if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Next'))); out.append(nav);
    } catch (e) { clear(out); out.append(e instanceof ApiError && e.unavailable ? na(t('Query API is not running or not reachable')) : errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('div', { class: 'row' }, field('Search token', q), field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), h('button', { class: 'pri', onclick: () => { stack = []; void load(); } }, t('Search'))), h('p', { class: 'mut small' }, t('Search uses normalized, masked file content. SQL and tenant_id are never sent from the browser.')), out));
}

async function accessPage(el: HTMLElement) {
  const roles: [string, string, string[]][] = [
    ['platform-admin', 'Platform administration', ['query:read', 'uploads:write', 'export:read', 'export:write', 'connectors:write', 'sources:read', 'jobs:read', 'replay:read', 'replay:write', 'quarantine:read', 'quarantine-payload:read', 'quarantine-replay:read', 'quarantine-replay:write']],
    ['tenant-admin', 'Tenant administration', ['query:read', 'uploads:write', 'export:read', 'export:write', 'connectors:write', 'sources:read', 'jobs:read', 'replay:read', 'replay:write', 'quarantine:read', 'quarantine-payload:read', 'quarantine-replay:read', 'quarantine-replay:write']],
    ['operator', 'Operations', ['query:read', 'uploads:write', 'connectors:write', 'sources:read', 'jobs:read', 'replay:read', 'replay:write', 'quarantine:read', 'quarantine-replay:read']],
    ['analyst', 'Analytics', ['query:read', 'export:read']],
    ['auditor', 'Audit', ['query:read', 'sources:read', 'jobs:read', 'quarantine:read']],
    ['service-account', 'Service account', ['events:write']],
  ];
  const domains: [string, string, string][] = [
    ['File API / uploads / jobs', 'uploads:write, jobs:read', '#/files'], ['Events / text search / quality', 'query:read', '#/events'],
    ['Sources / connectors', 'sources:read, connectors:write', '#/sources'], ['Exports', 'export:read, export:write', '#/exports'],
    ['Replays', 'replay:read, replay:write', '#/replays'], ['Quarantine', 'quarantine:read, quarantine:replay:write', '#/quarantine'],
  ];
  el.append(h('div', { class: 'page-hero' }, h('div', { class: 'eyebrow' }, t('Administration')), h('h2', {}, t('Access & roles')), h('p', { class: 'hero-copy' }, t('Role and scope visibility for the same data sources exposed in this console.'))),
    h('div', { class: 'callout' }, h('span', { class: 'callout-icon' }, 'i'), h('div', {}, h('b', {}, t('Roles are managed by the external AAS')), h('p', { class: 'mut small' }, t('This console does not mutate roles or permissions. The validated token is the source of truth; changes are made in AAS and take effect on the next session.')))),
    h('div', { class: 'card' }, h('div', { class: 'card-heading' }, h('div', {}, h('h3', {}, t('Role catalogue')), h('p', { class: 'mut small' }, t('Reference matrix; effective access is always checked by each service.')))),
      h('div', { class: 'role-grid' }, roles.map(([name, label, scopes]) => h('article', { class: 'role-card' }, h('h4', {}, name), h('p', { class: 'mut small' }, t(label)), h('div', { class: 'chips' }, scopes.map((s) => h('span', { class: 'chip' }, s))))))),
    h('div', { class: 'card' }, h('h3', {}, t('Data domains and menu access')), table(['menu / data domain', 'required scopes', 'open'], domains.map(([name, scope, href]) => [name, scope, h('a', { class: 'text-link', href }, t('Open'))]), 'Data domains')));
}

async function replaysPage(el: HTMLElement) {
  el.append(h('h2', {}, t('Replays')));
  const list = live(); let stack: string[] = [];
  const src = sourceInput();
  const st = h('select', {}, h('option', { value: '' }, t('any')), REPLAY_STATUSES.map((x) => h('option', { value: x }, human('status', x))));
  const load = async (cursor?: string) => {
    clear(list); list.append(t('Loading...'));
    try {
      const p = await listReplays(ctx, B.replays, { source_id: src.value.trim() || undefined, status: st.value || undefined, cursor });
      clear(list);
      list.append(p.items.length ? table(['created', 'source', 'target version', 'status', 'range', 'job', 'completed'],
        p.items.map((r) => [timeEl(r.created_at), r.source_id, r.processing_version, h('span', { class: r.status === 'FAILED' ? 'err' : r.status === 'COMPLETED' ? 'ok' : '' }, human('status', r.status)),
          r.time_from ? [timeEl(r.time_from), ' – ', timeEl(r.time_to)] : '', r.job_id ?? '', timeEl(r.completed_at)]), 'Replays') : h('div', { class: 'mut' }, t('No replays.')));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Newer')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Older')));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na(t('Replay controller is not running, not reachable or has no PostgreSQL configured')) : errBox(e));
    }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, t('Read-only list. Starting and activating replays is not offered here.')),
    h('div', { class: 'row' }, field('Source ID', src), field('Status', st), h('button', { class: 'pri', onclick: () => { stack = []; setSource(src.value); void load(); } }, t('Filter'))), list));
  await load();
}

async function quarantinePage(el: HTMLElement) {
  el.append(h('h2', {}, t('Quarantine')));
  const list = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(list); list.append(t('Loading...'));
    try {
      const p = await listQuarantine(ctx, B.quarantine, cursor);
      clear(list);
      list.append(p.items.length ? table(['quarantined at', 'stage', 'reason code', 'reason', 'source', 'event', 'batch/job', 'pipeline v'],
        p.items.map((r) => [timeEl(r.quarantined_at), human('stage', r.stage), human('reason', r.reason_code), r.reason ?? '', r.source_id ?? '', r.event_id ?? '', r.batch_id ?? r.job_id ?? '', r.pipeline_version ?? '']), 'Quarantine') : h('div', { class: 'mut' }, t('Quarantine is empty.')));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, t('Newer')));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, t('Older')));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na(t('Quarantine service is not running or not reachable')) : errBox(e));
    }
  };
  const d = lastHours(24 * 7);
  const loc = (iso: string) => { const x = new Date(iso); x.setMinutes(x.getMinutes() - x.getTimezoneOffset()); return x.toISOString().slice(0, 16); };
  const src = sourceInput(); const from = h('input', { type: 'datetime-local', value: loc(d.from) }); const to = h('input', { type: 'datetime-local', value: loc(d.to) });
  const pv = h('input', { size: '6', placeholder: '1' }); const plan = live(); const pub = live();
  // Publishing acts only on the exact request that was counted: editing any field withdraws the offer.
  for (const i of [src, from, to, pv]) i.addEventListener('input', () => clear(pub));
  const offer = (req: ReplayRequest, count: number) => {
    const ack = h('input', { type: 'checkbox', id: 'replay-ack' });
    const go = h('button', { class: 'pri', disabled: true }, t('Publish replay'));
    ack.addEventListener('change', () => { go.disabled = !ack.checked; });
    go.addEventListener('click', async () => {
      go.disabled = true; ack.disabled = true; // one publish per confirmation
      try {
        const r = await publishReplay(ctx, B.quarantine, req);
        clear(pub); pub.append(h('div', { class: 'ok' }, t('Replay queued: job {job}, {count} record(s). The action was written to the audit log.', { job: r.job_id, count: fmtNum(r.candidate_count) })));
      } catch (e) { clear(pub); pub.append(errBox(e)); }
    });
    clear(pub);
    pub.append(h('div', { class: 'card warnbox' }, h('h3', {}, t('Publish replay')),
      h('p', {}, t('This publishes {count} quarantined record(s) of source {source} ({from} – {to}) to the pipeline version {version}. It cannot be undone.', { count: fmtNum(count), source: req.source_id, from: fmtTime(req.occurred_from), to: fmtTime(req.occurred_to), version: req.pipeline_version })),
      h('p', { class: 'mut' }, t('Requires the separate replay-publish permission; the action is audited.')),
      h('label', { class: 'check' }, ack, t('I understand and confirm the publication')), h('div', { class: 'row' }, go)));
  };
  const dry = async () => {
    clear(plan); clear(pub);
    const a = localToRfc3339(from.value), b = localToRfc3339(to.value), v = Number(pv.value);
    if (!a || !b || !src.value.trim() || !Number.isInteger(v) || v < 1) { plan.append(h('div', { class: 'err', role: 'alert' }, t('source id, time range and pipeline version are required'))); return; }
    setSource(src.value);
    try {
      const req: ReplayRequest = { source_id: src.value.trim(), occurred_from: a, occurred_to: b, pipeline_version: v };
      const r = await planReplay(ctx, B.quarantine, req);
      if (canPublish(r)) offer(req, r.candidate_count);
      plan.append(h('div', { class: r.executable ? 'ok' : 'warn' }, t('{count} candidate record(s); {state}. Dry run only: nothing was published.', { count: `${r.candidate_count_is_lower_bound ? '>= ' : ''}${fmtNum(r.candidate_count)}`, state: t(r.executable ? 'within the replay limit' : 'not executable (too large)') })));
    } catch (e) { plan.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('h3', {}, t('Replay dry run')), h('div', { class: 'row' }, field('Source ID', src), field('Occurred from', from), field('Occurred to', to), field('Pipeline version', pv),
    h('button', { onclick: () => void dry() }, t('Count candidates'))), plan, pub), h('div', { class: 'card' }, list));
  await load();
}

let firstRender = true;
const ICONS: Record<string, string[]> = {
  overview: ['M3 12l9-8 9 8', 'M5 10v10h14V10'],
  uploads: ['M12 16V4', 'M7 9l5-5 5 5', 'M4 20h16'],
  events: ['M3 12h4l3-8 4 16 3-8h4'],
  quality: ['M12 3l8 3v6c0 5-3.5 8-8 9-4.5-1-8-4-8-9V6z', 'M9 12l2 2 4-4'],
  connectors: ['M9 7V3', 'M15 7V3', 'M6 7h12v4a6 6 0 0 1-12 0z', 'M12 17v4'],
  exports: ['M12 4v12', 'M7 11l5 5 5-5', 'M4 20h16'],
  quarantine: ['M12 3l9 16H3z', 'M12 10v4', 'M12 17v.01'],
  replays: ['M3 12a9 9 0 1 0 3-6.7', 'M3 4v5h5', 'M12 8v4l3 2'],
  jobs: ['M4 6h16', 'M4 12h16', 'M4 18h10'],
  sources: ['M4 6c0-1.7 3.6-3 8-3s8 1.3 8 3-3.6 3-8 3-8-1.3-8-3z', 'M4 6v12c0 1.7 3.6 3 8 3s8-1.3 8-3V6', 'M4 12c0 1.7 3.6 3 8 3s8-1.3 8-3'],
  files: ['M5 4h9l5 5v11H5z', 'M14 4v6h6', 'M8 14h8', 'M8 17h6'],
  search: ['M11 4a7 7 0 1 0 4.9 12L21 21', 'M11 8v6', 'M8 11h6'],
  access: ['M12 3l8 4v5c0 5-3.5 8-8 9-4.5-1-8-4-8-9V7z', 'M9 12l2 2 4-4'],
};
const THEME_ICONS: Record<Theme, string[]> = {
  system: ['M3 5h18v11H3z', 'M8 20h8', 'M12 16v4'],
  light: ['M12 8a4 4 0 1 0 0 8 4 4 0 0 0 0-8z', 'M12 2v2', 'M12 20v2', 'M4.9 4.9l1.4 1.4', 'M17.7 17.7l1.4 1.4', 'M2 12h2', 'M20 12h2', 'M4.9 19.1l1.4-1.4', 'M17.7 6.3l1.4-1.4'],
  dark: ['M21 13A9 9 0 1 1 11 3a7 7 0 0 0 10 10z'],
};
const NAV_COLLAPSED_KEY = 'astryon_admin_nav_collapsed';
function navIsCollapsed() { return localStorage.getItem(NAV_COLLAPSED_KEY) === '1'; }
function toggleNav() {
  localStorage.setItem(NAV_COLLAPSED_KEY, navIsCollapsed() ? '0' : '1');
  prefChange = true;
  render();
}
function logo() {
  const el = svg(['M4 19L12 4l8 15', 'M8 13h8'], 28);
  el.setAttribute('stroke-width', '2.2');
  el.style.cssText = 'color:var(--ac)';
  return el;
}
// Language and theme controls: segmented buttons with aria-pressed; a change re-renders the page in place.
function prefs(): HTMLElement {
  const lang = h('div', { class: 'seg', role: 'group', 'aria-label': t('Language') }, LANGS.map((l) => h('button', {
    type: 'button', id: `pref-lang-${l.id}`, lang: l.htmlLang, title: l.name, 'aria-label': l.name, 'aria-pressed': String(getLang() === l.id),
    onclick: () => { setLang(l.id); prefChange = true; render(); document.getElementById(`pref-lang-${l.id}`)?.focus(); },
  }, l.label)));
  const theme = h('div', { class: 'seg', role: 'group', 'aria-label': t('Theme') }, THEMES.map((k) => h('button', {
    type: 'button', id: `pref-theme-${k}`, 'aria-pressed': String(getTheme() === k), title: t(k === 'system' ? 'System' : k === 'light' ? 'Light' : 'Dark'),
    onclick: () => { setTheme(k); prefChange = true; render(); document.getElementById(`pref-theme-${k}`)?.focus(); },
  }, svg(THEME_ICONS[k], 14), h('span', {}, t(k === 'system' ? 'System' : k === 'light' ? 'Light' : 'Dark')))));
  return h('div', { class: 'session' }, lang, theme);
}
// Shared page shell: skip link, banner, labelled navigation with aria-current, focusable main landmark.
function shell(root: HTMLElement, page: string, session: HTMLElement): HTMLElement {
  const groups: [string, string[]][] = [['Monitor', ['overview', 'events', 'quality', 'search']], ['Operate', ['files', 'uploads', 'jobs', 'exports', 'quarantine', 'replays']], ['Manage', ['connectors', 'sources', 'access']]];
  const nav = h('nav', { 'aria-label': t('Main') }, groups.map(([label, keys]) => h('div', { class: 'nav-group' },
    h('span', { class: 'nav-label' }, t(label)), keys.map((key) => { const route = ROUTES.find(([k]) => k === key)!; return h('a', { href: `#/${key}`, title: t(route[1]), class: key === page ? 'on' : '', ...(key === page ? { 'aria-current': 'page' } : {}) }, svg(ICONS[key] ?? ICONS.jobs, 18), h('span', {}, t(route[1]))); }))));
  const main = h('main', { id: 'main', tabindex: '-1' });
  const collapsed = navIsCollapsed();
  root.classList.toggle('nav-collapsed', collapsed);
  const navButton = h('button', { class: 'icon-btn nav-toggle', type: 'button', title: t(collapsed ? 'Expand navigation' : 'Collapse navigation'), 'aria-label': t(collapsed ? 'Expand navigation' : 'Collapse navigation'), 'aria-pressed': String(collapsed), onclick: toggleNav }, svg(['M4 6h16', 'M4 12h16', 'M4 18h16'], 18));
  root.append(h('a', { class: 'skip', href: '#main', onclick: (e: Event) => { e.preventDefault(); main.focus(); } }, t('Skip to main content')),
    h('header', {}, h('div', { class: 'brand-wrap' }, navButton, h('h1', { class: 'brand' }, logo(), h('span', {}, 'ASTRYON Admin', h('small', {}, t('Data operations'))))), h('div', { class: 'tools' }, session, prefs())),
    h('aside', {}, nav), main);
  const label = ROUTES.find(([k]) => k === page)?.[1];
  document.title = `${t(label ?? 'Not found')} - ASTRYON Admin`;
  // Route changes move focus to the new content (not on first load, so the page title is read first).
  if (!firstRender && !prefChange) main.focus({ preventScroll: true });
  firstRender = false; prefChange = false;
  return main;
}
let prefChange = false;
function renderPage(main: HTMLElement, page: string) {
  const route = ROUTES.find(([k]) => k === page);
  if (!route) { main.append(na(t('Unknown page'))); return; }
  void Promise.resolve(route[2](main)).catch((e) => main.append(errBox(e)));
}
function render() {
  const root = document.getElementById('app')!;
  clear(root);
  const page = location.hash.replace(/^#\/?/, '') || 'overview';
  if (BFF) { renderBff(root, page); return; }
  const tok = h('input', { type: 'password', size: '24', placeholder: t('bearer token (dev static or AAS)'), 'aria-label': t('Bearer token'), autocomplete: 'off' });
  const status = h('span', { class: 'mut', role: 'status' }, getToken() ? t('token set (session only)') : t('no token'));
  const session = h('div', { class: 'session' },
    tok, h('button', { onclick: () => { setToken(tok.value); render(); } }, t('Set')),
    h('button', { onclick: () => { setToken(null); render(); } }, t('Clear')),
    loginUrl ? h('a', { href: loginUrl }, t('Sign in')) : null, status);
  renderPage(shell(root, page, session), page);
}
function renderBff(root: HTMLElement, page: string) {
  const who = h('span', { role: 'status' }, t('checking session...'));
  const session = h('div', { class: 'session' }, who);
  const main = shell(root, page, session);
  void fetch('/auth/me', { credentials: 'same-origin' }).then((r) => r.json() as Promise<{ authenticated?: boolean; subject?: string }>).then((m) => {
    clear(who);
    if (!m.authenticated) { who.append(h('a', { href: '/auth/login' }, t('Sign in'))); return; }
    who.append(t('signed in{who}', { who: m.subject ? t(' as {subject}', { subject: m.subject }) : '' }), ' ', h('button', {
      onclick: async () => {
        const r = await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin', headers: { 'X-Requested-With': 'astryon-admin' } });
        const j = (await r.json().catch(() => ({}))) as { logout_url?: string };
        location.href = j.logout_url ?? '/';
      },
    }, t('Sign out')));
  }).catch(() => { who.textContent = t('session service unreachable'); });
  renderPage(main, page);
}
window.addEventListener('hashchange', render);
render();
