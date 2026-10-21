import { clear, h } from './dom';
import { getSource, getToken, loginUrl, setSource, setToken, takeTokenFromHash } from './auth';
import { ApiError, type Ctx } from './http';
import { parseProm } from './prom';
import { getUpload, labelSummary, listUploads, parseTags, SENSITIVITIES, SOURCE_TYPES, STATUSES, uploadFile, type UploadItem } from './upload';
import { fmtNum, lastHours, localToRfc3339, metricsSummary, qualitySummary, queryEvents } from './query';
import { listConnectors } from './connectors';

const env = import.meta.env;
const B = {
  gateway: env.VITE_GATEWAY_BASE ?? '/api/gateway',
  files: env.VITE_FILES_BASE ?? '/api/files',
  query: env.VITE_QUERY_BASE ?? '/api/query',
  connectors: env.VITE_CONNECTORS_BASE ?? '/api/connectors',
  s3Proxy: env.VITE_S3_PROXY ?? (env.DEV ? '/api/s3' : undefined),
};
const ctx: Ctx = { getToken };

const frag = takeTokenFromHash(location.hash);
if (frag) { setToken(frag); history.replaceState(null, '', location.pathname + location.search + '#/'); }

const ROUTES: [string, string, (el: HTMLElement) => void | Promise<void>][] = [
  ['overview', 'Overview', overview], ['uploads', 'Uploads', uploads], ['events', 'Events', events],
  ['quality', 'Quality', quality], ['connectors', 'Connectors', connectors], ['jobs', 'Jobs', () => {}], ['sources', 'Sources', () => {}],
];

function na(what: string) { return h('div', { class: 'na' }, `${what}: API not available in this build.`); }
function errBox(e: unknown): HTMLElement {
  if (e instanceof ApiError) {
    if (e.status === 401 || e.status === 403) return h('div', { class: 'err' }, `${e.code}: ${e.message} (check the token in the box above)`);
    return h('div', { class: 'err' }, `${e.code}: ${e.message}`, e.traceId ? ` [trace ${e.traceId}]` : '');
  }
  return h('div', { class: 'err' }, e instanceof Error ? e.message : String(e));
}
const field = (label: string, input: HTMLElement) => h('label', {}, label, input);
function table(head: string[], rows: (string | number | Node)[][]) {
  return h('table', {}, h('thead', {}, h('tr', {}, head.map((x) => h('th', {}, x)))),
    h('tbody', {}, rows.map((r) => h('tr', {}, r.map((c) => h('td', {}, c))))));
}
function sourceInput() { return h('input', { size: '38', placeholder: 'source uuid', value: getSource() }); }

async function overview(el: HTMLElement) {
  const out = h('div', {}, 'Loading...');
  el.append(h('h2', {}, 'Overview'), out);
  const kp = (l: string, v: string, cls = '') => h('div', { class: 'card kpi' }, h('span', { class: 'mut' }, l), h('b', { class: cls }, v));
  const kpis: Node[] = [];
  try {
    const r = await fetch(`${B.gateway}/healthz`);
    const j = (await r.json()) as { status?: string };
    kpis.push(kp('Gateway health', String(j.status ?? r.status), r.ok ? 'ok' : 'err'));
  } catch { kpis.push(kp('Gateway health', 'unreachable', 'err')); }
  let counters: Node = h('span', { class: 'mut' }, 'metrics not available');
  try {
    const r = await fetch(`${B.gateway}/metrics`);
    if (r.ok) {
      const s = parseProm(await r.text()).filter((x) => x.name.startsWith('astryon_') && x.name.endsWith('_total'));
      counters = s.length ? table(['metric', 'labels', 'value'], s.map((x) => [x.name, Object.entries(x.labels).map(([k, v]) => `${k}=${v}`).join(' '), fmtNum(x.value)])) : h('span', { class: 'mut' }, 'no counters yet');
    }
  } catch { /* leave default */ }
  clear(out);
  out.append(h('div', { class: 'kpis' }, kpis), h('div', { class: 'card' }, h('h3', {}, 'Gateway counters (public /metrics)'), counters),
    h('div', { class: 'card mut' }, 'File API and query API expose no public health endpoint in this build; their pages report errors inline.'));
}

function uploadForm(onDone: () => void) {
  const src = sourceInput();
  const st = h('select', {}, SOURCE_TYPES.map((x) => h('option', { value: x }, x)));
  const sens = h('select', {}, h('option', { value: '' }, '(profile default)'), SENSITIVITIES.map((x) => h('option', { value: x }, x)));
  const tags = h('input', { size: '30', placeholder: 'purpose=demo,team=ops' });
  const log = h('div', { class: 'mut' });
  const fileIn = h('input', { type: 'file', accept: '.csv,.ndjson,.jsonl,text/csv,application/x-ndjson', multiple: true });
  const drop = h('div', { class: 'drop' }, 'Drop CSV / NDJSON files here or ', fileIn);
  const run = async (files: File[]) => {
    clear(log);
    let tg: Record<string, string>;
    try { tg = parseTags(tags.value); } catch (e) { log.append(errBox(e)); return; }
    setSource(src.value);
    for (const f of files) {
      const line = h('div', {}, `${f.name}: `); const msg = h('span', {}, 'starting'); line.append(msg); log.append(line);
      try {
        const r = await uploadFile(ctx, B.files, { file: f, source_id: src.value.trim(), source_type: st.value, sensitivity: sens.value || undefined, tags: tg }, { s3Proxy: B.s3Proxy, onProgress: (m) => { msg.textContent = m; } });
        msg.textContent = `queued, file ${r.file_object_id}, job ${r.job_id}`; msg.className = 'ok';
      } catch (e) { msg.replaceWith(errBox(e)); }
    }
    onDone();
  };
  drop.addEventListener('dragover', (e) => { e.preventDefault(); drop.classList.add('over'); });
  drop.addEventListener('dragleave', () => drop.classList.remove('over'));
  drop.addEventListener('drop', (e) => { e.preventDefault(); drop.classList.remove('over'); void run([...(e.dataTransfer?.files ?? [])]); });
  fileIn.addEventListener('change', () => { void run([...(fileIn.files ?? [])]); fileIn.value = ''; });
  return h('div', { class: 'card' }, h('h3', {}, 'Upload'),
    h('div', { class: 'row' }, field('Source ID', src), field('Source type', st), field('Sensitivity', sens), field('Tags (k=v,...)', tags)), drop, log);
}

async function uploads(el: HTMLElement) {
  el.append(h('h2', {}, 'Uploads'));
  const list = h('div', {}); const detail = h('div', {});
  const f = {
    source_type: h('select', {}, h('option', { value: '' }, 'any'), SOURCE_TYPES.map((x) => h('option', { value: x }, x))),
    sensitivity: h('select', {}, h('option', { value: '' }, 'any'), SENSITIVITIES.map((x) => h('option', { value: x }, x))),
    status: h('select', {}, h('option', { value: '' }, 'any'), STATUSES.map((x) => h('option', { value: x }, x))),
    label: h('select', {}, h('option', { value: '' }, 'any'), ['contains_geo', 'contains_credentials', 'contains_pii'].map((x) => h('option', { value: x }, x))),
    tag: h('input', { size: '16', placeholder: 'key=value' }),
  };
  let cursors: string[] = []; // stack of cursors for prev/next
  const load = async (cursor?: string) => {
    clear(list); list.append('Loading...');
    try {
      const q: Record<string, string> = {};
      for (const [k, v] of Object.entries(f)) if (v.value) q[k] = v.value;
      if (cursor) q.cursor = cursor;
      const p = await listUploads(ctx, B.files, q);
      clear(list);
      const rows = p.items.map((u) => [
        h('a', { href: '#', onclick: (e: Event) => { e.preventDefault(); void showDetail(u.file_object_id); } }, u.file_object_id.slice(0, 8)),
        u.source_type, u.sensitivity, u.committed ? 'yes' : 'no', u.job?.status ?? '-', u.job?.error ?? '', labelSummary(u.job?.labels), fmtNum(u.size_bytes),
        Object.entries(u.tags).map(([k, v]) => `${k}=${v}`).join(' '), u.created_at,
      ]);
      list.append(rows.length ? table(['id', 'type', 'sensitivity', 'committed', 'status', 'error', 'labels', 'bytes', 'tags', 'created'], rows) : h('div', { class: 'mut' }, 'No uploads match.'));
      const nav = h('div', { class: 'row' });
      if (cursors.length) nav.append(h('button', { onclick: () => { cursors.pop(); void load(cursors.at(-1)); } }, 'Newer'));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { cursors.push(p.next_cursor!); void load(p.next_cursor); } }, 'Older'));
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
      h('button', { class: 'pri', onclick: apply }, 'Filter')), list), detail);
  await load();
}

function detailCard(u: UploadItem) {
  const j = u.job;
  const quarantined = j?.status === 'QUARANTINED';
  return h('div', { class: 'card' }, h('h3', {}, `Upload ${u.file_object_id}`),
    table(['field', 'value'], [
      ['source', u.source_id], ['source type', u.source_type], ['sensitivity', u.sensitivity], ['committed', String(u.committed)], ['bytes', fmtNum(u.size_bytes)],
      ['tags', Object.entries(u.tags).map(([k, v]) => `${k}=${v}`).join(' ') || '-'],
      ['job', j?.id ?? 'not completed yet'],
      ['status', h('span', { class: quarantined ? 'warn' : j?.status === 'FAILED' ? 'err' : '' }, j?.status ?? '-')],
      ['error code', j?.error ?? '-'],
      ['content labels (counts only)', Object.entries(j?.labels ?? {}).map(([k, v]) => `${k}: ${v.count} (${v.fields.length} fields)`).join('; ') || '-'],
      ['profile', u.profile ? `retention=${u.profile.retention_class ?? '-'} export=${u.profile.export_allowed ?? '-'}` : '-'],
    ]), quarantined ? h('p', { class: 'warn' }, 'Held back by content policy: nothing reached the sink.') : null);
}

function rangeControls() {
  const d = lastHours(24);
  const loc = (iso: string) => { const x = new Date(iso); x.setMinutes(x.getMinutes() - x.getTimezoneOffset()); return x.toISOString().slice(0, 16); };
  return {
    from: h('input', { type: 'datetime-local', value: loc(d.from) }), to: h('input', { type: 'datetime-local', value: loc(d.to) }),
    source: sourceInput(), pv: h('input', { size: '6', placeholder: 'e.g. 1', value: env.VITE_DEFAULT_PROCESSING_VERSION ?? '' }),
  };
}
const sourceNote = h('p', { class: 'mut' }, 'The query API requires a source id unless a processing version is given (no tenant-wide default). If the source has no active version, enter a processing version.');

async function events(el: HTMLElement) {
  el.append(h('h2', {}, 'Events'));
  const c = rangeControls();
  const et = h('input', { size: '16' }); const en = h('input', { size: '16' });
  const out = h('div', {}); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(out); out.append('Loading...');
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    if (!from || !to) { clear(out); out.append(errBox(new Error('invalid time range'))); return; }
    setSource(c.source.value);
    try {
      const p = await queryEvents(ctx, B.query, { from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value ? Number(c.pv.value) : undefined, event_type: et.value.trim(), entity_id: en.value.trim(), cursor });
      clear(out);
      out.append(p.items.length ? table(['time', 'type', 'entity', 'flags', 'event id', 'payload'], p.items.map((e) => [e.event_time, e.event_type, e.entity_id ?? '', (e.quality_flags ?? []).join(','), e.event_id, (e.payload ?? '').slice(0, 120)])) : h('div', { class: 'mut' }, 'No events.'));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, 'Previous'));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, 'Next'));
      out.append(nav);
    } catch (e) { clear(out); out.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, sourceNote, h('div', { class: 'row' },
    field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), field('Event type', et), field('Entity', en),
    h('button', { class: 'pri', onclick: () => { stack = []; void load(); } }, 'Search')), out));
}

async function quality(el: HTMLElement) {
  el.append(h('h2', {}, 'Quality and metrics'));
  const c = rangeControls(); const out = h('div', {});
  const load = async () => {
    clear(out); out.append('Loading...');
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    if (!from || !to) { clear(out); out.append(errBox(new Error('invalid time range'))); return; }
    setSource(c.source.value);
    const r = { from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value || undefined };
    const [m, q] = await Promise.allSettled([metricsSummary(ctx, B.query, r), qualitySummary(ctx, B.query, r)]);
    clear(out);
    const grid = (title: string, rows: Record<string, unknown>[] | undefined) => {
      const cols = [...new Set((rows ?? []).flatMap((x) => Object.keys(x)))];
      return h('div', { class: 'card' }, h('h3', {}, title), rows?.length ? table(cols, rows.map((x) => cols.map((k) => (typeof x[k] === 'object' ? JSON.stringify(x[k]) : fmtNum(x[k]))))) : h('span', { class: 'mut' }, 'no data'));
    };
    if (m.status === 'fulfilled') out.append(grid('Per-source metrics', m.value.sources)); else out.append(errBox(m.reason));
    if (q.status === 'fulfilled') out.append(grid('Per-source quality', q.value.sources), grid('Quality flags', q.value.flags)); else out.append(errBox(q.reason));
  };
  el.append(h('div', { class: 'card' }, sourceNote, h('div', { class: 'row' }, field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), h('button', { class: 'pri', onclick: () => void load() }, 'Load'))), out);
}

async function connectors(el: HTMLElement) {
  el.append(h('h2', {}, 'Connectors'));
  try {
    const p = await listConnectors(ctx, B.connectors);
    el.append(p.items.length ? table(['name', 'kind', 'enabled', 'source', 'secret ref', 'created'], p.items.map((x) => [x.name, x.kind, String(x.enabled), x.source_id, x.secret_ref ?? '', x.created_at])) : h('div', { class: 'mut' }, 'No connectors.'));
  } catch (e) {
    if (e instanceof ApiError && e.unavailable) el.append(na('Connector manager is not running or not reachable'));
    else el.append(errBox(e));
  }
}

function render() {
  const root = document.getElementById('app')!;
  clear(root);
  const page = location.hash.replace(/^#\/?/, '') || 'overview';
  const tok = h('input', { type: 'password', size: '28', placeholder: 'bearer token (dev static or AAS)', autocomplete: 'off' });
  const status = h('span', { class: 'mut' }, getToken() ? 'token set (session only)' : 'no token');
  const nav = h('nav', {}, ROUTES.map(([k, l]) => h('a', { href: `#/${k}`, class: k === page ? 'on' : '' }, l)));
  const settings = h('div', { class: 'row', style: 'margin:0;margin-left:auto' },
    tok, h('button', { onclick: () => { setToken(tok.value); render(); } }, 'Set'),
    h('button', { onclick: () => { setToken(null); render(); } }, 'Clear'),
    loginUrl ? h('a', { href: loginUrl }, 'Sign in') : null, status);
  const main = h('main', {});
  root.append(h('header', {}, h('strong', {}, 'ASTRYON Admin'), nav, settings), main);
  const route = ROUTES.find(([k]) => k === page);
  if (!route) { main.append(na('Unknown page')); return; }
  if (page === 'jobs') main.append(h('h2', {}, 'Jobs'), na('/v1/jobs'));
  else if (page === 'sources') main.append(h('h2', {}, 'Sources'), na('/v1/sources'));
  else void Promise.resolve(route[2](main)).catch((e) => main.append(errBox(e)));
}
window.addEventListener('hashchange', render);
render();
