import { call, type Ctx } from './http';
export interface ExportSpec { from: string; to: string; processing_version: number; format?: 'csv' | 'ndjson'; source_id?: string; event_type?: string; entity_id?: string; max_rows?: number }
export interface ExportJob {
  id: string; status: 'QUEUED' | 'RUNNING' | 'SUCCEEDED' | 'FAILED'; rows: number; bytes: number; format: string; error_code?: string;
  created_at: string; started_at?: string; finished_at?: string; download_url?: string; download_expires_at?: string;
}
export const MAX_RANGE_MS = 31 * 24 * 3600_000;

/** Client-side mirror of the server rules, only to fail fast; the server stays authoritative. */
export function validateSpec(s: ExportSpec): string | undefined {
  const a = Date.parse(s.from), b = Date.parse(s.to);
  if (Number.isNaN(a) || Number.isNaN(b)) return 'invalid time range';
  if (b <= a) return 'to must be after from';
  if (b - a > MAX_RANGE_MS) return 'range must not exceed 31 days';
  if (!Number.isInteger(s.processing_version) || s.processing_version < 0) return 'processing version must be a non-negative integer';
  if (s.max_rows !== undefined && (!Number.isInteger(s.max_rows) || s.max_rows < 1 || s.max_rows > 10_000_000)) return 'max rows must be 1..10000000';
  return undefined;
}
/** Only whitelisted fields; the tenant is never sent (it comes from the token). */
export function buildSpec(s: ExportSpec): Record<string, unknown> {
  const body: Record<string, unknown> = { from: s.from, to: s.to, processing_version: s.processing_version };
  for (const k of ['format', 'source_id', 'event_type', 'entity_id', 'max_rows'] as const) {
    const v = s[k]; if (v !== undefined && v !== '') body[k] = v;
  }
  return body;
}
export const createExport = (c: Ctx, base: string, s: ExportSpec) => call<{ id: string; status: string }>(c, base, '/v1/exports', { method: 'POST', body: buildSpec(s) });
export const getExport = (c: Ctx, base: string, id: string) => call<ExportJob>(c, base, `/v1/exports/${encodeURIComponent(id)}`);
export const isTerminal = (j: ExportJob) => j.status === 'SUCCEEDED' || j.status === 'FAILED';

// The API has no list endpoint; ids created from this browser are remembered (non-secret) in sessionStorage.
const KEY = 'astryon_admin_exports';
export function knownExports(): string[] { try { return JSON.parse(sessionStorage.getItem(KEY) ?? '[]') as string[]; } catch { return []; } }
export function rememberExport(id: string) { try { sessionStorage.setItem(KEY, JSON.stringify([id, ...knownExports().filter((x) => x !== id)].slice(0, 20))); } catch { /* ignore */ } }
