import { call, type Ctx } from './http';
export interface EventRow {
  event_id: string; source_id: string; batch_id?: string; processing_version?: number; event_type: string;
  event_time: string; entity_id?: string; attributes?: Record<string, string>; payload?: string; quality_flags?: string[];
}
export interface EventsPage { items: EventRow[]; next_cursor: string }
export interface EventsFilter { from: string; to: string; source_id?: string; processing_version?: number; event_type?: string; entity_id?: string; limit?: number; cursor?: string }
type Rows = Record<string, unknown>[];

/** Fixed, whitelisted request only: the UI never builds SQL or free-form query text. */
export function queryEvents(c: Ctx, base: string, f: EventsFilter) {
  const body: Record<string, unknown> = { from: f.from, to: f.to, limit: f.limit ?? 100 };
  for (const k of ['source_id', 'processing_version', 'event_type', 'entity_id', 'cursor'] as const) {
    const v = f[k]; if (v !== undefined && v !== '') body[k] = v;
  }
  return call<EventsPage>(c, base, '/v1/query/events', { method: 'POST', body });
}
export interface Range { from: string; to: string; source_id?: string; processing_version?: string }
export const metricsSummary = (c: Ctx, base: string, r: Range) => call<{ sources?: Rows }>(c, base, '/v1/metrics/summary', { query: { ...r } });
export const qualitySummary = (c: Ctx, base: string, r: Range) => call<{ sources?: Rows; flags?: Rows }>(c, base, '/v1/quality/summary', { query: { ...r } });

export function lastHours(h: number, now = new Date()): { from: string; to: string } {
  return { from: new Date(now.getTime() - h * 3600_000).toISOString(), to: now.toISOString() };
}
/** datetime-local value ("YYYY-MM-DDTHH:mm", local time) -> RFC 3339 UTC. */
export function localToRfc3339(v: string): string | undefined {
  if (!v) return undefined;
  const d = new Date(v);
  return Number.isNaN(d.getTime()) ? undefined : d.toISOString();
}
export const fmtNum = (v: unknown) => { const n = Number(v); return Number.isFinite(n) ? n.toLocaleString('en-US') : String(v ?? ''); };
