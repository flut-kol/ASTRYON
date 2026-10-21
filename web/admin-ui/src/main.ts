import { clear, h } from './dom';
import { getSource, getToken, loginUrl, setSource, setToken, takeTokenFromHash } from './auth';
import { ApiError, type Ctx } from './http';
import { parseProm } from './prom';
import { getUpload, labelSummary, listUploads, parseTags, SENSITIVITIES, SOURCE_TYPES, STATUSES, uploadFile, type UploadItem } from './upload';
import { fmtNum, lastHours, localToRfc3339, metricsSummary, qualitySummary, queryEvents } from './query';
import { listConnectors, setConnectorEnabled } from './connectors';
import { createExport, getExport, isTerminal, knownExports, rememberExport, validateSpec, type ExportJob } from './exports';
import { listQuarantine, planReplay } from './quarantine';
import { listReplays, REPLAY_STATUSES } from './replays';

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

const frag = takeTokenFromHash(location.hash);
if (frag) { setToken(frag); history.replaceState(null, '', location.pathname + location.search + '#/'); }

const ROUTES: [string, string, (el: HTMLElement) => void | Promise<void>][] = [
  ['overview', 'Overview', overview], ['uploads', 'Uploads', uploads], ['events', 'Events', events],
  ['quality', 'Quality', quality], ['connectors', 'Connectors', connectors], ['exports', 'Exports', exportsPage], ['quarantine', 'Quarantine', quarantinePage], ['replays', 'Replays', replaysPage], ['jobs', 'Jobs', () => {}], ['sources', 'Sources', () => {}],
];

function na(what: string) { return h('div', { class: 'na' }, `${what}: API not available in this build.`); }
function errBox(e: unknown): HTMLElement {
  if (e instanceof ApiError) {
    if (e.status === 401 || e.status === 403) return h('div', { class: 'err', role: 'alert' }, `${e.code}: ${e.message} (${BFF ? 'sign in again' : 'check the token in the box above'})`);
    return h('div', { class: 'err', role: 'alert' }, `${e.code}: ${e.message}`, e.traceId ? ` [trace ${e.traceId}]` : '');
  }
  return h('div', { class: 'err', role: 'alert' }, e instanceof Error ? e.message : String(e));
}
// Async result areas announce their content politely (loading, results, errors) to screen readers.
const live = (cls = '') => h('div', { role: 'status', 'aria-live': 'polite', ...(cls ? { class: cls } : {}) });
const field = (label: string, input: HTMLElement) => h('label', {}, label, input);
function table(head: string[], rows: (string | number | Node)[][], caption = '') {
  return h('table', {}, caption ? h('caption', { class: 'sr-only' }, caption) : null,
    h('thead', {}, h('tr', {}, head.map((x) => h('th', { scope: 'col' }, x || h('span', { class: 'sr-only' }, 'actions'))))),
    h('tbody', {}, rows.map((r) => h('tr', {}, r.map((c) => h('td', {}, c))))));
}
function sourceInput() { return h('input', { size: '38', placeholder: 'source uuid', value: getSource() }); }

async function overview(el: HTMLElement) {
  const out = live(); out.append('Loading...');
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
      counters = s.length ? table(['metric', 'labels', 'value'], s.map((x) => [x.name, Object.entries(x.labels).map(([k, v]) => `${k}=${v}`).join(' '), fmtNum(x.value)]), 'Gateway counters') : h('span', { class: 'mut' }, 'no counters yet');
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
  const log = live('mut');
  const fileIn = h('input', { type: 'file', 'aria-label': 'Choose CSV or NDJSON files to upload', accept: '.csv,.ndjson,.jsonl,text/csv,application/x-ndjson', multiple: true });
  const drop = h('div', { class: 'drop', role: 'group', 'aria-label': 'File drop zone (or use the file chooser)' }, 'Drop CSV / NDJSON files here or ', fileIn);
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
  const list = live(); const detail = live();
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
        h('button', { class: 'lnk', 'aria-label': `Show upload ${u.file_object_id}`, onclick: () => void showDetail(u.file_object_id) }, u.file_object_id.slice(0, 8)),
        u.source_type, u.sensitivity, u.committed ? 'yes' : 'no', u.job?.status ?? '-', u.job?.error ?? '', labelSummary(u.job?.labels), fmtNum(u.size_bytes),
        Object.entries(u.tags).map(([k, v]) => `${k}=${v}`).join(' '), u.created_at,
      ]);
      list.append(rows.length ? table(['id', 'type', 'sensitivity', 'committed', 'status', 'error', 'labels', 'bytes', 'tags', 'created'], rows, 'Uploads') : h('div', { class: 'mut' }, 'No uploads match.'));
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
    ], 'Upload details'), quarantined ? h('p', { class: 'warn' }, 'Held back by content policy: nothing reached the sink.') : null);
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
  const out = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(out); out.append('Loading...');
    const from = localToRfc3339(c.from.value), to = localToRfc3339(c.to.value);
    if (!from || !to) { clear(out); out.append(errBox(new Error('invalid time range'))); return; }
    setSource(c.source.value);
    try {
      const p = await queryEvents(ctx, B.query, { from, to, source_id: c.source.value.trim() || undefined, processing_version: c.pv.value ? Number(c.pv.value) : undefined, event_type: et.value.trim(), entity_id: en.value.trim(), cursor });
      clear(out);
      out.append(p.items.length ? table(['time', 'type', 'entity', 'flags', 'event id', 'payload'], p.items.map((e) => [e.event_time, e.event_type, e.entity_id ?? '', (e.quality_flags ?? []).join(','), e.event_id, (e.payload ?? '').slice(0, 120)]), 'Events') : h('div', { class: 'mut' }, 'No events.'));
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
  const c = rangeControls(); const out = live();
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
      return h('div', { class: 'card' }, h('h3', {}, title), rows?.length ? table(cols, rows.map((x) => cols.map((k) => (typeof x[k] === 'object' ? JSON.stringify(x[k]) : fmtNum(x[k])))), title) : h('span', { class: 'mut' }, 'no data'));
    };
    if (m.status === 'fulfilled') out.append(grid('Per-source metrics', m.value.sources)); else out.append(errBox(m.reason));
    if (q.status === 'fulfilled') out.append(grid('Per-source quality', q.value.sources), grid('Quality flags', q.value.flags)); else out.append(errBox(q.reason));
  };
  el.append(h('div', { class: 'card' }, sourceNote, h('div', { class: 'row' }, field('From', c.from), field('To', c.to), field('Source ID', c.source), field('Proc. version', c.pv), h('button', { class: 'pri', onclick: () => void load() }, 'Load'))), out);
}

async function connectors(el: HTMLElement) {
  el.append(h('h2', {}, 'Connectors'));
  const out = live(); el.append(out);
  const load = async () => {
    clear(out);
    try {
      const p = await listConnectors(ctx, B.connectors);
      const toggle = (x: { id: string; enabled: boolean; name?: string }) => h('button', {
        'aria-label': `${x.enabled ? 'Pause' : 'Resume'} connector ${x.name ?? x.id}`,
        onclick: async () => {
          try { await setConnectorEnabled(ctx, B.connectors, x.id, !x.enabled); await load(); } catch (e) { out.prepend(errBox(e)); }
        },
      }, x.enabled ? 'Pause' : 'Resume');
      out.append(p.items.length ? table(['name', 'kind', 'enabled', 'source', 'secret ref', 'created', ''], p.items.map((x) => [x.name, x.kind, h('span', { class: x.enabled ? 'ok' : 'warn' }, String(x.enabled)), x.source_id, x.secret_ref ?? '', x.created_at, toggle(x)]), 'Connectors') : h('div', { class: 'mut' }, 'No connectors.'));
    } catch (e) {
      if (e instanceof ApiError && e.unavailable) out.append(na('Connector manager is not running or not reachable'));
      else out.append(errBox(e));
    }
  };
  await load();
}

async function exportsPage(el: HTMLElement) {
  el.append(h('h2', {}, 'Exports'));
  const c = rangeControls();
  const fmt = h('select', {}, h('option', { value: 'ndjson' }, 'ndjson'), h('option', { value: 'csv' }, 'csv'));
  const et = h('input', { size: '16' }); const en = h('input', { size: '16' }); const mr = h('input', { size: '9', placeholder: '1000000' });
  const msg = live(); const list = live();
  const jobs = new Map<string, ExportJob | Error>();
  let timer: ReturnType<typeof setTimeout> | undefined;
  const draw = () => {
    clear(list);
    const ids = knownExports();
    if (!ids.length) { list.append(h('div', { class: 'mut' }, 'No exports started from this browser session. The API has no list endpoint; jobs are tracked by id.')); return; }
    list.append(table(['id', 'status', 'rows', 'bytes', 'format', 'error', 'download'], ids.map((id) => {
      const j = jobs.get(id);
      if (!j) return [id, 'loading', '', '', '', '', ''];
      if (j instanceof Error) return [id, h('span', { class: 'err' }, j.message), '', '', '', '', ''];
      return [id, h('span', { class: j.status === 'FAILED' ? 'err' : j.status === 'SUCCEEDED' ? 'ok' : '' }, j.status), fmtNum(j.rows), fmtNum(j.bytes), j.format, j.error_code ?? '',
        j.download_url ? h('a', { href: j.download_url, rel: 'noopener' }, `download (until ${j.download_expires_at ?? '?'})`) : ''];
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
    if (bad) { msg.append(h('div', { class: 'err' }, bad)); return; }
    setSource(c.source.value);
    try {
      const r = await createExport(ctx, B.exports, spec);
      rememberExport(r.id); msg.append(h('div', { class: 'ok' }, `queued ${r.id}`));
      clearTimeout(timer); void refresh();
    } catch (e) { msg.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, 'Range up to 31 days; processing version is required. The tenant comes from your token. Download links are short-lived and fetched again on refresh.'),
    h('div', { class: 'row' }, field('From', c.from), field('To', c.to), field('Source ID (optional)', c.source), field('Proc. version', c.pv), field('Format', fmt), field('Event type', et), field('Entity', en), field('Max rows', mr),
      h('button', { class: 'pri', onclick: () => void start() }, 'Start export')), msg),
    h('div', { class: 'card' }, h('div', { class: 'row' }, h('button', { onclick: () => { clearTimeout(timer); void refresh(); } }, 'Refresh')), list));
  draw(); await refresh();
}

async function replaysPage(el: HTMLElement) {
  el.append(h('h2', {}, 'Replays'));
  const list = live(); let stack: string[] = [];
  const src = sourceInput();
  const st = h('select', {}, h('option', { value: '' }, 'any'), REPLAY_STATUSES.map((x) => h('option', { value: x }, x)));
  const load = async (cursor?: string) => {
    clear(list); list.append('Loading...');
    try {
      const p = await listReplays(ctx, B.replays, { source_id: src.value.trim() || undefined, status: st.value || undefined, cursor });
      clear(list);
      list.append(p.items.length ? table(['created', 'source', 'target version', 'status', 'range', 'job', 'completed'],
        p.items.map((r) => [r.created_at, r.source_id, r.processing_version, h('span', { class: r.status === 'FAILED' ? 'err' : r.status === 'COMPLETED' ? 'ok' : '' }, r.status),
          r.time_from ? `${r.time_from} .. ${r.time_to ?? ''}` : '', r.job_id ?? '', r.completed_at ?? '']), 'Replays') : h('div', { class: 'mut' }, 'No replays.'));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, 'Newer'));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, 'Older'));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na('Replay controller is not running, not reachable or has no PostgreSQL configured') : errBox(e));
    }
  };
  el.append(h('div', { class: 'card' }, h('p', { class: 'mut' }, 'Read-only list. Starting and activating replays is not offered here.'),
    h('div', { class: 'row' }, field('Source ID', src), field('Status', st), h('button', { class: 'pri', onclick: () => { stack = []; setSource(src.value); void load(); } }, 'Filter')), list));
  await load();
}

async function quarantinePage(el: HTMLElement) {
  el.append(h('h2', {}, 'Quarantine'));
  const list = live(); let stack: string[] = [];
  const load = async (cursor?: string) => {
    clear(list); list.append('Loading...');
    try {
      const p = await listQuarantine(ctx, B.quarantine, cursor);
      clear(list);
      list.append(p.items.length ? table(['quarantined at', 'stage', 'reason code', 'reason', 'source', 'event', 'batch/job', 'pipeline v'],
        p.items.map((r) => [r.quarantined_at, r.stage, r.reason_code, r.reason ?? '', r.source_id ?? '', r.event_id ?? '', r.batch_id ?? r.job_id ?? '', r.pipeline_version ?? '']), 'Quarantine') : h('div', { class: 'mut' }, 'Quarantine is empty.'));
      const nav = h('div', { class: 'row' });
      if (stack.length) nav.append(h('button', { onclick: () => { stack.pop(); void load(stack.at(-1)); } }, 'Newer'));
      if (p.next_cursor) nav.append(h('button', { onclick: () => { stack.push(p.next_cursor); void load(p.next_cursor); } }, 'Older'));
      list.append(nav);
    } catch (e) {
      clear(list);
      list.append(e instanceof ApiError && e.unavailable ? na('Quarantine service is not running or not reachable') : errBox(e));
    }
  };
  const d = lastHours(24 * 7);
  const loc = (iso: string) => { const x = new Date(iso); x.setMinutes(x.getMinutes() - x.getTimezoneOffset()); return x.toISOString().slice(0, 16); };
  const src = sourceInput(); const from = h('input', { type: 'datetime-local', value: loc(d.from) }); const to = h('input', { type: 'datetime-local', value: loc(d.to) });
  const pv = h('input', { size: '6', placeholder: '1' }); const plan = live();
  const dry = async () => {
    clear(plan);
    const a = localToRfc3339(from.value), b = localToRfc3339(to.value), v = Number(pv.value);
    if (!a || !b || !src.value.trim() || !Number.isInteger(v) || v < 1) { plan.append(h('div', { class: 'err' }, 'source id, time range and pipeline version are required')); return; }
    setSource(src.value);
    try {
      const r = await planReplay(ctx, B.quarantine, { source_id: src.value.trim(), occurred_from: a, occurred_to: b, pipeline_version: v });
      plan.append(h('div', { class: r.executable ? 'ok' : 'warn' }, `${r.candidate_count_is_lower_bound ? '>= ' : ''}${fmtNum(r.candidate_count)} candidate record(s); ${r.executable ? 'within the replay limit' : 'not executable (too large)'}. Dry run only: nothing was published.`));
    } catch (e) { plan.append(errBox(e)); }
  };
  el.append(h('div', { class: 'card' }, h('h3', {}, 'Replay dry run'), h('div', { class: 'row' }, field('Source ID', src), field('Occurred from', from), field('Occurred to', to), field('Pipeline version', pv),
    h('button', { onclick: () => void dry() }, 'Count candidates')), plan), h('div', { class: 'card' }, list));
  await load();
}

let firstRender = true;
// Shared page shell: skip link, banner, labelled navigation with aria-current, focusable main landmark.
function shell(root: HTMLElement, page: string, settings: HTMLElement): HTMLElement {
  const nav = h('nav', { 'aria-label': 'Main' }, ROUTES.map(([k, l]) => h('a', { href: `#/${k}`, class: k === page ? 'on' : '', ...(k === page ? { 'aria-current': 'page' } : {}) }, l)));
  const main = h('main', { id: 'main', tabindex: '-1' });
  root.append(h('a', { class: 'skip', href: '#main', onclick: (e: Event) => { e.preventDefault(); main.focus(); } }, 'Skip to main content'),
    h('header', {}, h('h1', {}, 'ASTRYON Admin'), nav, settings), main);
  const label = ROUTES.find(([k]) => k === page)?.[1] ?? 'Not found';
  document.title = `${label} - ASTRYON Admin`;
  // Route changes move focus to the new content (not on first load, so the page title is read first).
  if (!firstRender) main.focus({ preventScroll: true });
  firstRender = false;
  return main;
}
function renderPage(main: HTMLElement, page: string) {
  const route = ROUTES.find(([k]) => k === page);
  if (!route) { main.append(na('Unknown page')); return; }
  if (page === 'jobs') main.append(h('h2', {}, 'Jobs'), na('/v1/jobs'));
  else if (page === 'sources') main.append(h('h2', {}, 'Sources'), na('/v1/sources'));
  else void Promise.resolve(route[2](main)).catch((e) => main.append(errBox(e)));
}
function render() {
  const root = document.getElementById('app')!;
  clear(root);
  const page = location.hash.replace(/^#\/?/, '') || 'overview';
  if (BFF) { renderBff(root, page); return; }
  const tok = h('input', { type: 'password', size: '28', placeholder: 'bearer token (dev static or AAS)', 'aria-label': 'Bearer token', autocomplete: 'off' });
  const status = h('span', { class: 'mut', role: 'status' }, getToken() ? 'token set (session only)' : 'no token');
  const settings = h('div', { class: 'row', style: 'margin:0;margin-left:auto' },
    tok, h('button', { onclick: () => { setToken(tok.value); render(); } }, 'Set'),
    h('button', { onclick: () => { setToken(null); render(); } }, 'Clear'),
    loginUrl ? h('a', { href: loginUrl }, 'Sign in') : null, status);
  renderPage(shell(root, page, settings), page);
}
function renderBff(root: HTMLElement, page: string) {
  const who = h('span', { class: 'mut', role: 'status' }, 'checking session...');
  const settings = h('div', { class: 'row', style: 'margin:0;margin-left:auto' }, who);
  const main = shell(root, page, settings);
  void fetch('/auth/me', { credentials: 'same-origin' }).then((r) => r.json() as Promise<{ authenticated?: boolean; subject?: string }>).then((m) => {
    clear(who);
    if (!m.authenticated) { who.append(h('a', { href: '/auth/login' }, 'Sign in')); return; }
    who.append(`signed in${m.subject ? ` as ${m.subject}` : ''} `, h('button', {
      onclick: async () => {
        const r = await fetch('/auth/logout', { method: 'POST', credentials: 'same-origin', headers: { 'X-Requested-With': 'astryon-admin' } });
        const j = (await r.json().catch(() => ({}))) as { logout_url?: string };
        location.href = j.logout_url ?? '/';
      },
    }, 'Sign out'));
  }).catch(() => { who.textContent = 'session service unreachable'; });
  renderPage(main, page);
}
window.addEventListener('hashchange', render);
render();
