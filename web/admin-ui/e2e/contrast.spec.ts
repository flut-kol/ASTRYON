import { createRequire } from 'node:module';
import { expect, test } from '@playwright/test';
import { mockApi } from '../src/mockApi';

// Real-browser axe run including color-contrast (jsdom cannot compute it), in both color schemes, on every page.
const axeSource = createRequire(import.meta.url).resolve('axe-core/axe.min.js');
const PAGES = ['overview', 'uploads', 'events', 'quality', 'connectors', 'exports', 'quarantine', 'replays'];

for (const scheme of ['light', 'dark'] as const) {
  for (const authed of [true, false]) {
  test(`axe incl. color-contrast, ${scheme} scheme, ${authed ? 'signed in' : 'signed out'}, all pages`, async ({ page }) => {
    await page.emulateMedia({ colorScheme: scheme });
    await page.route('**/auth/me', (r) => r.fulfill({ json: authed ? { authenticated: true, subject: 'ops@example' } : { authenticated: false } }));
    await page.route('**/api/**', (r) => {
      const m = mockApi(r.request().method(), new URL(r.request().url()).pathname);
      return r.fulfill({ status: m.status, contentType: m.text ? 'text/plain' : 'application/json', body: m.text ?? JSON.stringify(m.json ?? {}) });
    });
    const bad: string[] = [];
    for (const p of PAGES) {
      await page.goto(`/#/${p}`);
      await page.getByRole('main').getByRole('heading').first().waitFor();
      await page.waitForFunction(() => !document.querySelector('main')?.textContent?.includes('Loading...'));
      await page.addScriptTag({ path: axeSource });
      const res = await page.evaluate(async () => {
        // @ts-expect-error axe is injected
        const r = await window.axe.run(document, { resultTypes: ['violations'] });
        return r.violations.filter((v: { impact: string }) => v.impact === 'serious' || v.impact === 'critical')
          .map((v: { id: string; nodes: { target: string[] }[] }) => `${v.id}: ${v.nodes.slice(0, 3).map((n) => n.target.join(' ')).join(' | ')}`);
      });
      bad.push(...res.map((x) => `${scheme} #/${p} ${x}`));
    }
    expect(bad).toEqual([]);
  });
  }
}
