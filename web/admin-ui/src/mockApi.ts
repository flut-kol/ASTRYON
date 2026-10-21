// Fixture backend shared by the jsdom accessibility tests and the Playwright smoke suite. Test-only: never imported by main.ts.
export interface MockReply { status: number; json?: unknown; text?: string }
const T = '2026-10-21T10:00:00Z';
const SRC = '00000000-0000-0000-0000-0000000000b1';
const UP = '11111111-2222-4333-8444-555555555555';

export function mockApi(method: string, pathname: string, opts: { fail?: number } = {}): MockReply {
  if (opts.fail) return { status: opts.fail, json: { error: { code: opts.fail === 503 ? 'unavailable' : 'forbidden', message: 'mock failure', trace_id: 'trace-mock-1' } } };
  const p = pathname.replace(/^\/api\/[a-z0-9]+/, '');
  const base = pathname.split('/')[2];
  if (base === 'gateway' && p === '/healthz') return { status: 200, json: { status: 'ok' } };
  if (base === 'gateway' && p === '/metrics') return { status: 200, text: 'astryon_gateway_ack_total{outcome="ok"} 42\nastryon_gateway_ack_errors_total 1\n' };
  if (base === 'files' && p === '/v1/files/uploads' && method === 'GET') {
    return { status: 200, json: { items: [{ file_object_id: UP, source_id: SRC, source_type: 'telemetry', committed: true, size_bytes: 2048, sensitivity: 'internal', tags: { team: 'ops' }, created_at: T, job: { id: 'job-1', status: 'SUCCEEDED', labels: {} } }] } };
  }
  if (base === 'files' && p === `/v1/files/uploads/${UP}`) {
    return { status: 200, json: { file_object_id: UP, source_id: SRC, source_type: 'telemetry', committed: true, size_bytes: 2048, sensitivity: 'internal', tags: {}, created_at: T, job: { id: 'job-1', status: 'QUARANTINED', labels: { contains_pii: { count: 3, fields: ['email'] } } } } };
  }
  if (base === 'query' && p === '/v1/query/events') {
    return { status: 200, json: { items: [{ event_id: 'e1', source_id: SRC, event_type: 'sensor.reading', event_time: T, entity_id: 'dev-1', quality_flags: ['late'], payload: '{"v":1}' }], next_cursor: '' } };
  }
  if (base === 'query' && p === '/v1/metrics/summary') return { status: 200, json: { sources: [{ source_id: SRC, events: 1200, errors: 3 }] } };
  if (base === 'query' && p === '/v1/quality/summary') return { status: 200, json: { sources: [{ source_id: SRC, score: 0.99 }], flags: [{ flag: 'late', events: 5 }] } };
  if (base === 'connectors' && p === '/v1/connectors') return { status: 200, json: { items: [{ id: 'c1', source_id: SRC, name: 'orders-api', kind: 'rest_pull', enabled: true, config: {}, created_at: T }], next_cursor: '' } };
  if (base === 'exports' && p.startsWith('/v1/exports/')) return { status: 200, json: { id: p.split('/').pop(), status: 'RUNNING', rows: 0, bytes: 0, format: 'ndjson', created_at: T } };
  if (base === 'quarantine' && p === '/v1/quarantine') {
    return { status: 200, json: { items: [{ id: 'q1', source_id: SRC, event_id: 'e9', reason_code: 'BAD_VALUE', reason: 'masked failure', stage: 'normalize', quarantined_at: T, pipeline_version: 1 }], next_cursor: '' } };
  }
  if (base === 'quarantine' && p === '/v1/quarantine/replays/dry-run' && method === 'POST') {
    return { status: 200, json: { dry_run: true, candidate_count: 17, candidate_count_is_lower_bound: false, executable: true } };
  }
  if (base === 'replays' && p === '/v1/replays') {
    return { status: 200, json: { items: [{ id: 'r1', source_id: SRC, processing_version: 2, status: 'COMPLETED', time_from: T, time_to: T, job_id: 'j1', created_at: T, updated_at: T, completed_at: T }], next_cursor: '' } };
  }
  return { status: 404, json: { error: { code: 'not_found', message: 'no mock for ' + method + ' ' + pathname } } };
}
