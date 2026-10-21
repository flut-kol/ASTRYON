// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { mockApi } from './mockApi';

async function mount(route: string) {
  vi.resetModules();
  document.body.innerHTML = '<div id="app"></div>';
  location.hash = `#/${route}`;
  await import('./main');
  await vi.waitFor(() => { expect(document.getElementById('app')!.textContent).not.toContain('Loading...'); expect(document.getElementById('app')!.textContent).not.toContain('Завантаження...'); }, { timeout: 2000 });
}
beforeEach(() => {
  localStorage.clear(); sessionStorage.clear(); document.documentElement.removeAttribute('data-theme');
  vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const r = mockApi(init?.method ?? 'GET', new URL(String(input), 'http://localhost').pathname);
    return new Response(r.text ?? JSON.stringify(r.json ?? {}), { status: r.status });
  }));
});

describe('language', () => {
  it('defaults to English, switches to Ukrainian, persists and sets <html lang>', async () => {
    await mount('uploads');
    expect(document.documentElement.lang).toBe('en');
    expect(document.querySelector('main h2')?.textContent).toBe('Uploads');
    document.getElementById('pref-lang-ua')!.click();
    expect(document.documentElement.lang).toBe('uk');
    expect(document.querySelector('nav a[aria-current=page]')?.textContent).toBe('Завантаження');
    expect(document.title).toBe('Завантаження - ASTRYON Admin');
    expect(document.getElementById('pref-lang-ua')!.getAttribute('aria-pressed')).toBe('true');
    expect(localStorage.getItem('astryon_admin_lang')).toBe('ua');
    await mount('uploads'); // reload: preference is restored
    expect(document.documentElement.lang).toBe('uk');
    document.getElementById('pref-lang-en')!.click();
    expect(document.querySelector('main h2')?.textContent).toBe('Uploads');
  });
  it('translates library validation messages and keeps unknown strings', async () => {
    const { t, tMsg, setLang } = await import('./i18n');
    setLang('ua');
    expect(tMsg('range must not exceed 31 days')).toBe('діапазон не може перевищувати 31 день');
    expect(tMsg('tag "x" must be key=value')).toContain('«x»');
    expect(t('no such key')).toBe('no such key');
    expect(t('queued {id}', { id: 'e1' })).toBe('у черзі e1');
    setLang('en');
    expect(t('queued {id}', { id: 'e1' })).toBe('queued e1');
  });
});

describe('theme', () => {
  it('forces light/dark through data-theme and returns to system', async () => {
    await mount('overview');
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
    document.getElementById('pref-theme-dark')!.click();
    expect(document.documentElement.dataset.theme).toBe('dark');
    expect(localStorage.getItem('astryon_admin_theme')).toBe('dark');
    document.getElementById('pref-theme-light')!.click();
    expect(document.documentElement.dataset.theme).toBe('light');
    document.getElementById('pref-theme-system')!.click();
    expect(document.documentElement.hasAttribute('data-theme')).toBe(false);
  });
});
