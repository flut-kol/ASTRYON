import { describe, expect, it, vi } from 'vitest';
import { ApiError, call, parseError } from './http';
import { parseProm } from './prom';
import { labelSummary, parseTags, planParts, rewritePartUrl, sha256Hex, uploadFile } from './upload';
import { localToRfc3339, queryEvents } from './query';
import { takeTokenFromHash } from './auth';

const json = (b: unknown, status = 200, headers: Record<string, string> = {}) => new Response(JSON.stringify(b), { status, headers });

describe('errors', () => {
  it('parses both shapes', () => {
    expect(parseError(400, { error: { code: 'invalid_range', message: 'm', trace_id: 't' } }, 'x')).toMatchObject({ code: 'invalid_range', traceId: 't' });
    expect(parseError(404, { code: 'NOT_FOUND', message: 'nf' }, 'x')).toMatchObject({ code: 'NOT_FOUND', message: 'nf' });
    expect(parseError(500, null, 'fb', 'rid')).toMatchObject({ code: 'http_500', message: 'fb', traceId: 'rid' });
  });
  it('404 and network errors are "unavailable"', async () => {
    const f = vi.fn().mockResolvedValue(json({}, 404));
    await expect(call({ getToken: () => null, fetchFn: f }, '/x', '/v1/jobs')).rejects.toMatchObject({ unavailable: true });
    const g = vi.fn().mockRejectedValue(new Error('down'));
    await expect(call({ getToken: () => null, fetchFn: g }, '/x', '/v1/x')).rejects.toBeInstanceOf(ApiError);
  });
  it('sends bearer, never tenant', async () => {
    const f = vi.fn().mockResolvedValue(json({ items: [] }));
    await call({ getToken: () => 'tok', fetchFn: f }, '/b/', '/v1/a', { query: { a: '1', b: undefined } });
    expect(f.mock.calls[0][0]).toBe('/b/v1/a?a=1');
    expect(f.mock.calls[0][1].headers.Authorization).toBe('Bearer tok');
  });
});

describe('helpers', () => {
  it('prometheus', () => {
    const s = parseProm('# HELP x\nastryon_a_total{code="202",x="y"} 5\nbad line\nm 1.5');
    expect(s).toEqual([{ name: 'astryon_a_total', labels: { code: '202', x: 'y' }, value: 5 }, { name: 'm', labels: {}, value: 1.5 }]);
  });
  it('tags', () => {
    expect(parseTags('purpose=demo, a=b=c')).toEqual({ purpose: 'demo', a: 'b=c' });
    expect(parseTags('')).toEqual({});
    expect(() => parseTags('bad')).toThrow();
  });
  it('parts, urls, labels, hash, time, hash fragment', async () => {
    expect(planParts(1)).toBe(1); expect(planParts(8 * 1024 * 1024 + 1)).toBe(2);
    expect(rewritePartUrl('http://127.0.0.1:19200/files/o?X-Amz-Signature=a', '/api/s3/')).toBe('/api/s3/files/o?X-Amz-Signature=a');
    expect(rewritePartUrl('http://h/p', undefined)).toBe('http://h/p');
    expect(labelSummary({ contains_pii: { count: 3, fields: ['e'] } })).toBe('pii:3');
    expect(await sha256Hex(new TextEncoder().encode('abc').buffer as ArrayBuffer)).toBe('ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad');
    expect(localToRfc3339('')).toBeUndefined();
    expect(takeTokenFromHash('#token=a%2Bb')).toBe('a+b');
  });
});

describe('query', () => {
  it('posts only whitelisted fields', async () => {
    const f = vi.fn().mockResolvedValue(json({ items: [], next_cursor: '' }));
    await queryEvents({ getToken: () => 't', fetchFn: f }, '/q', { from: 'a', to: 'b', source_id: 's', event_type: '', cursor: 'c' });
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({ from: 'a', to: 'b', limit: 100, source_id: 's', cursor: 'c' });
  });
});

describe('upload flow', () => {
  it('create -> PUT parts -> complete', async () => {
    const calls: string[] = [];
    const f = vi.fn(async (url: string, init: RequestInit) => {
      calls.push(`${init.method} ${url}`);
      if (url === '/f/v1/files/uploads') return json({ file_object_id: 'fo', upload_id: 'u', part_urls: ['http://s3/files/k?sig=1'], expires_at: 'x' }, 201);
      if (init.method === 'PUT') return new Response('', { status: 200, headers: { ETag: '"e1"' } });
      return json({ file_object_id: 'fo', status: 'committed', job_id: 'j' });
    });
    const file = new File(['a,b\n1,2\n'], 'x.csv', { type: 'text/csv' });
    const r = await uploadFile({ getToken: () => 't', fetchFn: f as unknown as typeof fetch }, '/f', { file, source_id: 's', source_type: 'telemetry', tags: { purpose: 'd' } }, { s3Proxy: '/api/s3' });
    expect(r.job_id).toBe('j');
    expect(calls).toEqual(['POST /f/v1/files/uploads', 'PUT /api/s3/files/k?sig=1', 'POST /f/v1/files/uploads/fo/complete']);
    const body = JSON.parse((f.mock.calls[0][1] as RequestInit).body as string);
    expect(body).toMatchObject({ source_type: 'telemetry', tags: { purpose: 'd' }, size_bytes: 8, parts: 1 });
    expect(body.tenant_id).toBeUndefined();
    expect(JSON.parse((f.mock.calls[2][1] as RequestInit).body as string).parts).toEqual([{ number: 1, etag: '"e1"' }]);
  });
});
