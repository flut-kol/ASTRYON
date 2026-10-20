import { describe, expect, it } from 'vitest';
import { ApiError } from './api';
import { counters, errorView, healthView, jobRow, sourceMetricRows, sourceRow, topErrorRows } from './viewmodel';
import { takeTokenFromHash } from './auth';
import type { Job, MetricsSummary, QualitySummary } from './types';

const sm = { source_id: 's1', events: 1204331, bytes: 10, batches: 2, errors: 4, max_ingest_lag_ms: 850 };
const m: MetricsSummary = { from: 'a', to: 'b', totals: { ...sm, source_id: '' }, by_source: [sm], series: [] };
const q: QualitySummary = { from: 'a', to: 'b', events_total: 10000, events_valid: 9900, events_rejected: 100, events_with_flags: 5, flags: [], top_errors: [{ error_code: 'E1', stage: 'validate', count: 1500 }] };
const job: Job = { id: 'j', kind: 'process_file', status: 'FAILED', source_id: null, batch_id: null, attempt_count: 3, max_attempts: 3, events_processed: 1000, events_failed: 5, last_error: null, created_at: 'x', started_at: null, finished_at: null };

describe('viewmodel', () => {
  it('formats counters', () => {
    const c = counters(m, q);
    expect(c[0]).toEqual({ label: 'Events', value: '1,204,331' });
    expect(c.at(-1)).toEqual({ label: 'Rejected rate', value: '1.00%' });
  });
  it('counters without quality; zero total safe', () => {
    expect(counters(m)).toHaveLength(5);
    expect(counters(m, { ...q, events_total: 0, events_rejected: 0 }).at(-1)!.value).toBe('0.00%');
  });
  it('by-source and top-error rows', () => {
    expect(sourceMetricRows(m)[0]).toMatchObject({ sourceId: 's1', lagMs: '850' });
    expect(topErrorRows(q)[0]).toEqual({ code: 'E1', stage: 'validate', count: '1,500' });
  });
  it('job rows', () => {
    expect(jobRow(job)).toMatchObject({ cls: 'bad', attempts: '3/3', processed: '1,000', lastError: '' });
    expect(jobRow({ ...job, status: 'SUCCEEDED' }).cls).toBe('ok');
    expect(jobRow({ ...job, status: 'RETRYING' }).cls).toBe('warn');
  });
  it('source rows', () => {
    expect(sourceRow({ id: 's', name: 'n', kind: 'kafka', is_enabled: false, created_at: 'c', updated_at: 'u' })).toMatchObject({ enabled: false, status: 'disabled', kind: 'kafka' });
  });
  it('health', () => {
    const v = healthView({ status: 'degraded', checks: { clickhouse: 'ok', postgres: 'down' } });
    expect(v.cls).toBe('warn');
    expect(v.checks).toEqual([{ name: 'clickhouse', state: 'ok', cls: 'ok' }, { name: 'postgres', state: 'down', cls: 'bad' }]);
  });
  it('error view carries trace_id', () => {
    expect(errorView(new ApiError(401, 'AUTH_REQUIRED', 'bad', 'tr1'))).toEqual({ message: 'AUTH_REQUIRED: bad (token missing or expired)', traceId: 'tr1' });
    expect(errorView(new Error('x'))).toEqual({ message: 'x' });
  });
  it('token from hash', () => {
    expect(takeTokenFromHash('#token=a%2Bb')).toBe('a+b');
    expect(takeTokenFromHash('#jobs')).toBeNull();
  });
});
