import { describe, expect, it, vi } from 'vitest';
import { ApiError, call, parseError } from './http';
import { parseProm } from './prom';
import { labelSummary, parseTags, planParts, rewritePartUrl, sha256Hex, uploadFile } from './upload';
import { localToRfc3339, queryEvents } from './query';
import { takeTokenFromHash } from './auth';
import { getVersion, listVersions, preview, saveVersion, validateOp, type OpSpec } from './fileEdit';

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

describe('exports and connectors', () => {
  it('validates the spec and sends only whitelisted fields', async () => {
    const { buildSpec, createExport, validateSpec } = await import('./exports');
    const base = { from: '2026-10-01T00:00:00Z', to: '2026-10-02T00:00:00Z', processing_version: 1 };
    expect(validateSpec(base)).toBeUndefined();
    expect(validateSpec({ ...base, to: '2026-12-01T00:00:00Z' })).toMatch(/31 days/);
    expect(validateSpec({ ...base, to: base.from })).toMatch(/after/);
    expect(validateSpec({ ...base, processing_version: -1 })).toMatch(/version/);
    expect(validateSpec({ ...base, max_rows: 0 })).toMatch(/max rows/);
    expect(buildSpec({ ...base, source_id: '', event_type: 'x', format: 'csv' })).toEqual({ ...base, event_type: 'x', format: 'csv' });
    const f = vi.fn().mockResolvedValue(json({ id: 'e1', status: 'QUEUED' }, 202));
    await createExport({ getToken: () => 't', fetchFn: f }, '/x', base);
    expect(f.mock.calls[0][0]).toBe('/x/v1/exports');
    expect(JSON.parse(f.mock.calls[0][1].body).tenant_id).toBeUndefined();
  });
  it('pause/resume hit the right endpoints', async () => {
    const { setConnectorEnabled } = await import('./connectors');
    const f = vi.fn(async (_u: string, _i: RequestInit) => json({ id: 'a', enabled: false }));
    await setConnectorEnabled({ getToken: () => 't', fetchFn: f as unknown as typeof fetch }, '/c', 'a/b', false);
    await setConnectorEnabled({ getToken: () => 't', fetchFn: f as unknown as typeof fetch }, '/c', 'a', true);
    expect(f.mock.calls.map((c) => `${c[1].method} ${c[0]}`)).toEqual(['POST /c/v1/connectors/a%2Fb/pause', 'POST /c/v1/connectors/a/resume']);
  });
});

describe('bff mode', () => {
  it('uses the session cookie, no bearer, CSRF header on POST', async () => {
    const f = vi.fn(async (_u: string, _i: RequestInit) => json({ ok: true }));
    const c = { getToken: () => 'leak', fetchFn: f as unknown as typeof fetch, bff: true };
    await call(c, '/api/query', '/v1/x');
    await call(c, '/api/query', '/v1/x', { method: 'POST', body: {} });
    const [g, p] = f.mock.calls.map((x) => x[1] as RequestInit & { headers: Record<string, string> });
    expect(g.headers.Authorization).toBeUndefined();
    expect(g.headers['X-Requested-With']).toBeUndefined();
    expect(p.headers['X-Requested-With']).toBe('astryon-admin');
    expect(p.credentials).toBe('same-origin');
  });
});

describe('file edit', () => {
  it('preview posts ops and format options to the right path', async () => {
    const f = vi.fn().mockResolvedValue(json({ header: ['a'], rows: [['1']], truncated: false, warnings: [] }));
    const ops: OpSpec[] = [{ type: 'rename_column', column: 'a', new_name: 'b' }];
    await preview({ getToken: () => 't', fetchFn: f }, '/files', 'f1', ops, { output_delimiter: ',' }, 10);
    expect(f.mock.calls[0][0]).toBe('/files/v1/files/uploads/f1/preview');
    expect(f.mock.calls[0][1].method).toBe('POST');
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({ ops, output_delimiter: ',', limit: 10 });
  });

  it('saveVersion posts to the versions path', async () => {
    const f = vi.fn().mockResolvedValue(json({ version_id: 'v1', file_object_id: 'f1', version_number: 1, header: ['a'], output_delimiter: ',', header_row: true, size_bytes: 1, row_count: 1, created_at: 't' }));
    await saveVersion({ getToken: () => 't', fetchFn: f }, '/files', 'f1', [], { output_delimiter: ',' }, 'out.csv');
    expect(f.mock.calls[0][0]).toBe('/files/v1/files/uploads/f1/versions');
    expect(JSON.parse(f.mock.calls[0][1].body)).toEqual({ ops: [], output_delimiter: ',', filename: 'out.csv' });
  });

  it('listVersions and getVersion build the right URLs', async () => {
    const f = vi.fn().mockResolvedValue(json({ items: [] }));
    await listVersions({ getToken: () => 't', fetchFn: f }, '/files', 'f1');
    expect(f.mock.calls[0][0]).toBe('/files/v1/files/uploads/f1/versions');
    const g = vi.fn().mockResolvedValue(json({ version_id: 'v1' }));
    await getVersion({ getToken: () => 't', fetchFn: g }, '/files', 'f1', 'v 1');
    expect(g.mock.calls[0][0]).toBe('/files/v1/files/uploads/f1/versions/v%201');
  });

  it('validateOp rejects ops missing required fields and accepts valid ones', () => {
    expect(validateOp({ type: 'rename_column', column: 'a' })).toBeDefined();
    expect(validateOp({ type: 'rename_column', column: 'a', new_name: 'b' })).toBeUndefined();
    expect(validateOp({ type: 'split_column', column: 'a', delimiter: ' ', new_columns: ['x'] })).toBeDefined();
    expect(validateOp({ type: 'split_column', column: 'a', delimiter: ' ', new_columns: ['x', 'y'] })).toBeUndefined();
    expect(validateOp({ type: 'merge_columns', columns: ['a'], new_column: 'c' })).toBeDefined();
    expect(validateOp({ type: 'merge_columns', columns: ['a', 'b'], new_column: 'c' })).toBeUndefined();
    expect(validateOp({ type: 'find_replace', find: 'x' })).toBeDefined();
    expect(validateOp({ type: 'find_replace', all_columns: true, find: 'x' })).toBeUndefined();
    expect(validateOp({ type: 'delete_column' })).toBeDefined();
    expect(validateOp({ type: 'delete_column', column: 'a' })).toBeUndefined();
    expect(validateOp({ type: 'reorder_columns', order: [] })).toBeDefined();
    expect(validateOp({ type: 'reorder_columns', order: ['a'] })).toBeUndefined();
    expect(validateOp({ type: 'trim_whitespace' })).toBeDefined();
    expect(validateOp({ type: 'trim_whitespace', all_columns: true })).toBeUndefined();
    expect(validateOp({ type: 'drop_empty_rows' })).toBeUndefined();
  });
});
