import { call, type Ctx } from './http';

export interface TextHit {
  event_id: string; source_id: string; event_time: string; event_type?: string;
  entity_id?: string; processing_version?: number; snippet?: string;
}
export interface TextSearchRequest {
  text: string; from: string; to: string; source_id?: string;
  processing_version?: number; limit?: number; cursor?: string;
}
export interface TextSearchPage { items: TextHit[]; next_cursor: string }

/** Server-side token-index search. The browser never sends SQL or tenant_id. */
export function searchText(c: Ctx, base: string, f: TextSearchRequest) {
  const body: Record<string, unknown> = { text: f.text, from: f.from, to: f.to, limit: f.limit ?? 50 };
  for (const k of ['source_id', 'processing_version', 'cursor'] as const) {
    const v = f[k]; if (v !== undefined && v !== '') body[k] = v;
  }
  return call<TextSearchPage>(c, base, '/v1/query/text', { method: 'POST', body });
}
