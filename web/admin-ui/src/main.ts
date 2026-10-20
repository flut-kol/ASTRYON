import { ApiClient } from './api';
import { getToken, loginUrl, setToken, takeTokenFromHash } from './auth';
import { counters, errorView, healthView, jobRow, sourceMetricRows, sourceRow, topErrorRows } from './viewmodel';

const api = new ApiClient({ baseUrl: import.meta.env.VITE_API_BASE_URL ?? '', getToken });
const app = document.getElementById('app')!;
const esc = (s: unknown) => String(s).replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]!));
const pages = ['counters', 'jobs', 'sources', 'health'] as const;

const h = takeTokenFromHash(location.hash);
if (h) { setToken(h); history.replaceState(null, '', location.pathname); }

function errHtml(e: unknown) {
  const v = errorView(e);
  return `<div class="error">${esc(v.message)}${v.traceId ? `<br>trace_id: <code>${esc(v.traceId)}</code>` : ''}</div>`;
}
const table = (head: string[], rows: string[][]) =>
  `<table><tr>${head.map(x => `<th>${x}</th>`).join('')}</tr>${rows.map(r => `<tr>${r.join('')}</tr>`).join('') || `<tr><td colspan="${head.length}">No data</td></tr>`}</table>`;
const td = (s: unknown, cls = '') => `<td class="${cls}">${esc(s)}</td>`;

async function render(page: string) {
  let body = '';
  try {
    if (page === 'counters') {
      const [m, q] = await Promise.all([api.metrics(), api.quality()]);
      body = `<div class="cards">${counters(m, q).map(c => `<div class="card">${esc(c.label)}<b>${esc(c.value)}</b></div>`).join('')}</div>
        <h3>By source</h3>${table(['Source', 'Events', 'Errors', 'Max lag (ms)'], sourceMetricRows(m).map(r => [td(r.sourceId), td(r.events), td(r.errors), td(r.lagMs)]))}
        <h3>Top errors</h3>${table(['Code', 'Stage', 'Count'], topErrorRows(q).map(r => [td(r.code), td(r.stage), td(r.count)]))}`;
    } else if (page === 'jobs') {
      const p = await api.jobs();
      body = table(['ID', 'Kind', 'Status', 'Attempts', 'Processed', 'Failed', 'Last error', 'Created'], p.items.map(jobRow).map(j => [td(j.id), td(j.kind), td(j.status, j.cls), td(j.attempts), td(j.processed), td(j.failed), td(j.lastError), td(j.created)]));
    } else if (page === 'sources') {
      const p = await api.sources();
      body = table(['ID', 'Name', 'Kind', 'Status', 'Updated'], p.items.map(sourceRow).map(s => [td(s.id), td(s.name), td(s.kind), td(s.status, s.enabled ? 'ok' : 'warn'), td(s.updated)]));
    } else {
      const v = healthView(await api.health());
      body = `<p>Status: <b class="${v.cls}">${esc(v.status)}</b></p>` + table(['Check', 'State'], v.checks.map(c => [td(c.name), td(c.state, c.cls)]));
    }
  } catch (e) { body = errHtml(e); }
  return body;
}

async function show() {
  const page = (pages as readonly string[]).includes(location.hash.slice(1)) ? location.hash.slice(1) : 'counters';
  const authed = !!getToken();
  app.innerHTML = `<header><strong>ASTRYON Admin</strong>${pages.map(p => `<a href="#${p}" class="${p === page ? 'active' : ''}">${p}</a>`).join('')}
    <span style="flex:1"></span>${authed ? '<button id="out">Sign out</button>'
      : loginUrl ? `<a href="${esc(loginUrl)}">Sign in</a>`
      : '<input id="tok" placeholder="paste dev token" size="30"><button id="set">Use</button>'}</header>
    <main><h2>${page}</h2><div id="view">Loading…</div></main>`;
  document.getElementById('out')?.addEventListener('click', () => { setToken(null); show(); });
  document.getElementById('set')?.addEventListener('click', () => { setToken((document.getElementById('tok') as HTMLInputElement).value); show(); });
  const html = await render(page);
  document.getElementById('view')!.innerHTML = html;
}
window.addEventListener('hashchange', show);
show();
