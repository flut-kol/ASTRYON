import { call, type Ctx } from './http';
export interface Connector { id: string; source_id: string; name: string; kind: string; enabled: boolean; secret_ref?: string; config: Record<string, unknown>; created_at: string }
export const listConnectors = (c: Ctx, base: string, cursor?: string) =>
  call<{ items: Connector[]; next_cursor: string }>(c, base, '/v1/connectors', { query: { cursor } });
