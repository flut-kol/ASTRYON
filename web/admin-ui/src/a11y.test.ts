// @vitest-environment jsdom
// Accessibility smoke over every rendered view with axe-core (jsdom has no layout, so color-contrast is checked
// separately in docs/README via computed ratios). Fails on serious/critical violations.
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import axe from 'axe-core';
import { mockApi } from './mockApi';

const ROUTES = ['overview', 'uploads', 'events', 'quality', 'connectors', 'exports', 'quarantine', 'replays', 'jobs', 'sources'];

function stubFetch(fail?: number) {
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const u = new URL(String(input), 'http://localhost');
    const r = mockApi(init?.method ?? 'GET', u.pathname, { fail });
    const body = r.text ?? JSON.stringify(r.json ?? {});
    return new Response(body, { status: r.status, headers: { 'Content-Type': r.text ? 'text/plain' : 'application/json' } });
  }));
}
async function settle(root: HTMLElement) {
  await vi.waitFor(() => { expect(root.textContent).not.toContain('Loading...'); }, { timeout: 2000 });
}
async function mount(route: string) {
  vi.resetModules();
  document.documentElement.lang = 'en';
  document.body.innerHTML = '<div id="app"></div>';
  location.hash = `#/${route}`;
  await import('./main');
  await settle(document.getElementById('app')!);
}
async function violations() {
  const r = await axe.run(document.documentElement, { rules: { 'color-contrast': { enabled: false } }, resultTypes: ['violations'] });
  return r.violations.filter((v) => v.impact === 'serious' || v.impact === 'critical')
    .map((v) => `${v.id}: ${v.help} -> ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(' | ')}`);
}

beforeEach(() => { sessionStorage.clear(); stubFetch(); });
afterEach(() => { vi.unstubAllGlobals(); vi.unstubAllEnvs(); });

describe('axe: token mode', () => {
  for (const route of ROUTES) {
    it(`#/${route} has no serious or critical violations`, async () => {
      await mount(route);
      expect(await violations()).toEqual([]);
    });
  }
  it('error state (503) is announced via role=alert and is accessible', async () => {
    stubFetch(503);
    await mount('quarantine');
    expect(document.body.textContent).toContain('not running or not reachable');
    await mount('events');
    document.querySelector<HTMLButtonElement>('button.pri')!.click();
    await settle(document.getElementById('app')!);
    expect(document.querySelector('[role=alert]')).not.toBeNull();
    expect(await violations()).toEqual([]);
  });
});

describe('axe: BFF mode', () => {
  it('signed-out and signed-in shells are accessible', async () => {
    vi.stubEnv('VITE_AUTH_MODE', 'bff');
    for (const authed of [false, true]) {
      vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
        const u = new URL(String(input), 'http://localhost');
        if (u.pathname === '/auth/me') return new Response(JSON.stringify(authed ? { authenticated: true, subject: 'ops@example' } : { authenticated: false }), { status: 200 });
        const r = mockApi(init?.method ?? 'GET', u.pathname);
        return new Response(r.text ?? JSON.stringify(r.json ?? {}), { status: r.status });
      }));
      await mount('overview');
      await vi.waitFor(() => { expect(document.querySelector('header [role=status]')?.textContent).not.toContain('checking'); });
      expect(await violations()).toEqual([]);
    }
  });
});

describe('axe: Ukrainian UI', () => {
  it('main routes have no serious or critical violations and lang=uk', async () => {
    localStorage.setItem('astryon_admin_lang', 'ua');
    try {
      for (const route of ['overview', 'uploads', 'events', 'quarantine']) {
        await mount(route);
        expect(document.documentElement.lang).toBe('uk');
        expect(await violations(), route).toEqual([]);
      }
    } finally { localStorage.removeItem('astryon_admin_lang'); }
  });
});

describe('page structure', () => {
  it('has skip link, one h1, labelled nav, focusable main, and sets the document title', async () => {
    await mount('uploads');
    expect(document.querySelector('a.skip')?.getAttribute('href')).toBe('#main');
    expect(document.querySelectorAll('h1').length).toBe(1);
    expect(document.querySelector('nav')?.getAttribute('aria-label')).toBe('Main');
    expect(document.querySelector('nav a[aria-current=page]')?.textContent).toBe('Uploads');
    expect(document.querySelector('main#main')?.getAttribute('tabindex')).toBe('-1');
    expect(document.title).toBe('Uploads - ASTRYON Admin');
  });
  it('every input and select has an accessible name', async () => {
    for (const route of ['uploads', 'events', 'exports', 'quarantine', 'replays']) {
      await mount(route);
      for (const el of document.querySelectorAll('input, select')) {
        const named = el.getAttribute('aria-label') || el.closest('label')?.textContent?.trim();
        expect(named, `${route}: ${el.outerHTML.slice(0, 80)}`).toBeTruthy();
      }
    }
  });
});

describe('detector sanity', () => {
  it('axe flags an unlabeled input, an image without alt and an unnamed button', async () => {
    document.documentElement.lang = 'en';
    document.title = 't';
    document.body.innerHTML = '<main><h1>x</h1><input><img src="x.png"><button></button></main>';
    const found = await violations();
    expect(found.some((v) => v.startsWith('label'))).toBe(true);
    expect(found.some((v) => v.startsWith('image-alt'))).toBe(true);
    expect(found.some((v) => v.startsWith('button-name'))).toBe(true);
  });
});
