import { call, type Ctx } from './http';
export interface Source { id: string; name: string; kind: string; enabled: boolean; connector_count: number; created_at: string; updated_at: string }
/** Read-only; served by connector-manager (scope astryon:sources:read). Never carries config or credentials. */
export const listSources = (c: Ctx, base: string, cursor?: string) =>
  call<{ items: Source[]; next_cursor: string }>(c, base, '/v1/sources', { query: { limit: '50', cursor } });
