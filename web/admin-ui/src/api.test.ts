import { describe, expect, it, vi } from 'vitest';
import { ApiClient, ApiError } from './api';

const mk = (res: Response | Error, token: string | null = 'tok') => {
  const f = vi.fn(async () => { if (res instanceof Error) throw res; return res; });
  return { f, c: new ApiClient({ getToken: () => token, fetchFn: f as unknown as typeof fetch }) };
};
const json = (b: unknown, status = 200, h: Record<string, string> = {}) =>
  new Response(JSON.stringify(b), { status, headers: { 'content-type': 'application/json', ...h } });

describe('ApiClient', () => {
  it('sends bearer token and never a tenant param/header', async () => {
    const { f, c } = mk(json({ items: [] }));
    await c.jobs('abc');
    const [url, init] = f.mock.calls[0] as unknown as [string, RequestInit];
    expect(url).toBe('/v1/jobs?cursor=abc');
    expect((init.headers as Record<string, string>).Authorization).toBe('Bearer tok');
    expect(JSON.stringify(init.headers).toLowerCase()).not.toContain('tenant');
  });
  it('passes range/status params and skips empty ones', async () => {
    const { f, c } = mk(json({}));
    f.mockImplementation(async () => json({}));
    await c.metrics({ from: '2026-10-19T00:00:00Z' });
    await c.jobs(undefined, 'FAILED');
    expect((f.mock.calls[0] as unknown as [string])[0]).toBe('/v1/metrics/summary?from=2026-10-19T00%3A00%3A00Z');
    expect((f.mock.calls[1] as unknown as [string])[0]).toBe('/v1/jobs?status=FAILED');
  });
  it('healthz returns the Health body on 503', async () => {
    const { c } = mk(json({ status: 'degraded', checks: { postgres: 'down' } }, 503));
    await expect(c.health()).resolves.toMatchObject({ status: 'degraded' });
  });
  it('omits Authorization without token', async () => {
    const { f, c } = mk(json({ status: 'ok' }), null);
    await c.health();
    expect((f.mock.calls[0] as unknown as [string, RequestInit])[1].headers).not.toHaveProperty('Authorization');
  });
  it('parses structured error with trace_id', async () => {
    const { c } = mk(json({ code: 'forbidden', message: 'nope', details: {}, trace_id: 't1' }, 403));
    await expect(c.sources()).rejects.toMatchObject({ status: 403, code: 'forbidden', traceId: 't1' });
  });
  it('falls back to X-Request-ID on non-JSON error', async () => {
    const { c } = mk(new Response('boom', { status: 502, headers: { 'X-Request-ID': 'r9' } }));
    const e = await c.metrics().catch(x => x);
    expect(e).toBeInstanceOf(ApiError);
    expect(e.traceId).toBe('r9'); expect(e.code).toBe('http_502');
  });
  it('wraps network errors', async () => {
    const { c } = mk(new Error('down'));
    await expect(c.quality()).rejects.toMatchObject({ status: 0, code: 'network_error' });
  });
});
