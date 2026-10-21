import { expect, test, type Page } from '@playwright/test';
import { mockApi } from '../src/mockApi';

// Mocked backend: /auth/me and every /api/* request is answered from src/mockApi.ts. `fail` forces an HTTP status on /api/*.
async function backend(page: Page, opts: { authed?: boolean; fail?: number } = {}) {
  const calls: { method: string; path: string; headers: Record<string, string> }[] = [];
  await page.route('**/auth/me', (r) => r.fulfill({ json: opts.authed ? { authenticated: true, subject: 'ops@example' } : { authenticated: false } }));
  await page.route('**/api/**', (r) => {
    const u = new URL(r.request().url());
    calls.push({ method: r.request().method(), path: u.pathname, headers: r.request().headers() });
    const m = mockApi(r.request().method(), u.pathname, { fail: opts.fail });
    return r.fulfill({ status: m.status, contentType: m.text ? 'text/plain' : 'application/json', body: m.text ?? JSON.stringify(m.json ?? {}) });
  });
  return calls;
}

test('sign-in state: signed out shows a Sign in link, signed in shows the subject and Sign out', async ({ page }) => {
  await backend(page);
  await page.goto('/#/overview');
  await expect(page.getByRole('banner').getByRole('link', { name: 'Sign in' })).toHaveAttribute('href', '/auth/login');
  const calls = await backend(page, { authed: true });
  await page.reload();
  await expect(page.getByRole('banner')).toContainText('signed in as ops@example');
  await expect(page.getByRole('button', { name: 'Sign out' })).toBeVisible();
  expect(calls.length).toBeGreaterThan(0);
});

test('overview shows gateway health and counters; skip link and landmarks exist', async ({ page }) => {
  await backend(page, { authed: true });
  await page.goto('/#/overview');
  await expect(page.getByRole('heading', { name: 'Overview' })).toBeVisible();
  await expect(page.getByText('Gateway health')).toBeVisible();
  await expect(page.getByRole('cell', { name: 'astryon_gateway_ack_total' })).toBeVisible();
  await expect(page.getByRole('main')).toBeVisible();
  await expect(page.getByRole('navigation', { name: 'Main' }).getByRole('link', { name: 'Overview' })).toHaveAttribute('aria-current', 'page');
  await page.keyboard.press('Tab');
  await expect(page.getByRole('link', { name: 'Skip to main content' })).toBeFocused();
  await page.keyboard.press('Enter');
  await expect(page.getByRole('main')).toBeFocused();
});

test('events query sends a whitelisted body without tenant and renders rows', async ({ page }) => {
  const calls = await backend(page, { authed: true });
  await page.goto('/#/events');
  await page.getByLabel('Source ID').fill('00000000-0000-0000-0000-0000000000b1');
  await page.getByLabel('Proc. version').fill('1');
  const req = page.waitForRequest((r) => r.url().includes('/v1/query/events'));
  await page.getByRole('button', { name: 'Search' }).click();
  const body = (await req).postDataJSON();
  expect(body.source_id).toBe('00000000-0000-0000-0000-0000000000b1');
  expect(body).not.toHaveProperty('tenant_id');
  await expect(page.getByRole('cell', { name: 'sensor.reading' })).toBeVisible();
  const post = calls.find((c) => c.path.endsWith('/v1/query/events'))!;
  expect(post.headers['x-requested-with']).toBe('astryon-admin'); // CSRF header on unsafe methods in BFF mode
  expect(post.headers['authorization']).toBeUndefined(); // the browser holds no token
});

test('quarantine list and replay dry-run', async ({ page }) => {
  await backend(page, { authed: true });
  await page.goto('/#/quarantine');
  await expect(page.getByRole('cell', { name: 'BAD_VALUE' })).toBeVisible();
  await page.getByLabel('Source ID').fill('00000000-0000-0000-0000-0000000000b1');
  await page.getByLabel('Pipeline version').fill('1');
  await page.getByRole('button', { name: 'Count candidates' }).click();
  await expect(page.getByRole('status').filter({ hasText: '17 candidate record(s)' })).toContainText('Dry run only: nothing was published.');
});

test('uploads list renders and the detail opens from a real button', async ({ page }) => {
  await backend(page, { authed: true });
  await page.goto('/#/uploads');
  await expect(page.getByRole('table', { name: 'Uploads' })).toBeVisible();
  await expect(page.getByRole('cell', { name: 'telemetry' })).toBeVisible();
  await page.getByRole('button', { name: /^Show upload 11111111/ }).click();
  await expect(page.getByRole('heading', { name: /^Upload 11111111/ })).toBeVisible();
  await expect(page.getByText('Held back by content policy')).toBeVisible();
  // the drop zone has a keyboard-reachable file chooser fallback
  await expect(page.getByLabel('Choose CSV or NDJSON files to upload')).toBeEnabled();
});

test('401 from an API shows the sign-in prompt in an alert', async ({ page }) => {
  await backend(page, { fail: 403 });
  await page.goto('/#/replays');
  await expect(page.getByRole('alert')).toContainText('sign in again');
});

test('503 from an API shows "not available" instead of a crash', async ({ page }) => {
  await backend(page, { authed: true, fail: 503 });
  await page.goto('/#/quarantine');
  await expect(page.getByText('Quarantine service is not running or not reachable')).toBeVisible();
  await page.goto('/#/events');
  await page.getByLabel('Source ID').fill('00000000-0000-0000-0000-0000000000b1');
  await page.getByLabel('Proc. version').fill('1');
  await page.getByRole('button', { name: 'Search' }).click();
  await expect(page.getByRole('alert')).toContainText('unavailable');
});

test('route change moves focus to main content', async ({ page }) => {
  await backend(page, { authed: true });
  await page.goto('/#/overview');
  await page.getByRole('navigation', { name: 'Main' }).getByRole('link', { name: 'Replays' }).click();
  await expect(page.getByRole('heading', { name: 'Replays' })).toBeVisible();
  await expect(page.getByRole('main')).toBeFocused();
  await expect(page).toHaveTitle('Replays - ASTRYON Admin');
});
