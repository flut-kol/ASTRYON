import { ApiError } from './api';
import type { Health, Job, MetricsSummary, QualitySummary, Source } from './types';

export interface Counter { label: string; value: string }
const fmt = (n: number) => new Intl.NumberFormat('en-US').format(n);
const pct = (a: number, b: number) => (b === 0 ? '0.00%' : `${((a / b) * 100).toFixed(2)}%`);

export function counters(m: MetricsSummary, q?: QualitySummary): Counter[] {
  const t = m.totals;
  const out: Counter[] = [
    { label: 'Events', value: fmt(t.events) },
    { label: 'Errors', value: fmt(t.errors) },
    { label: 'Batches', value: fmt(t.batches) },
    { label: 'Bytes', value: fmt(t.bytes) },
    { label: 'Max ingest lag (ms)', value: fmt(t.max_ingest_lag_ms) },
  ];
  if (q) {
    out.push(
      { label: 'Valid', value: fmt(q.events_valid) },
      { label: 'Rejected', value: fmt(q.events_rejected) },
      { label: 'With flags', value: fmt(q.events_with_flags) },
      { label: 'Rejected rate', value: pct(q.events_rejected, q.events_total) },
    );
  }
  return out;
}

export function sourceMetricRows(m: MetricsSummary) {
  return m.by_source.map(s => ({ sourceId: s.source_id, events: fmt(s.events), errors: fmt(s.errors), lagMs: fmt(s.max_ingest_lag_ms) }));
}
export function topErrorRows(q: QualitySummary) {
  return q.top_errors.map(e => ({ code: e.error_code, stage: e.stage, count: fmt(e.count) }));
}

export function jobRow(j: Job) {
  const cls = j.status === 'FAILED' ? 'bad' : j.status === 'RUNNING' || j.status === 'RETRYING' ? 'warn' : j.status === 'SUCCEEDED' ? 'ok' : '';
  return {
    id: j.id, kind: j.kind, status: j.status, cls,
    attempts: `${j.attempt_count}/${j.max_attempts}`,
    processed: fmt(j.events_processed), failed: fmt(j.events_failed),
    lastError: j.last_error ?? '', created: j.created_at,
  };
}

export function sourceRow(s: Source) {
  return { id: s.id, name: s.name, kind: s.kind, enabled: s.is_enabled, status: s.is_enabled ? 'enabled' : 'disabled', updated: s.updated_at };
}

export function healthView(h: Health) {
  return {
    status: h.status, cls: h.status === 'ok' ? 'ok' : 'warn',
    checks: Object.entries(h.checks ?? {}).map(([name, state]) => ({ name, state: state as string, cls: state === 'ok' ? 'ok' : 'bad' })),
  };
}

export function errorView(e: unknown): { message: string; traceId?: string } {
  if (e instanceof ApiError) {
    const hint = e.status === 401 ? ' (token missing or expired)' : e.status === 403 ? ' (forbidden: missing scope)' : e.status === 429 ? ' (rate limited)' : '';
    return { message: `${e.code}: ${e.message}${hint}`, traceId: e.traceId };
  }
  return { message: e instanceof Error ? e.message : String(e) };
}
