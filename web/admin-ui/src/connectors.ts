import { call, type Ctx } from './http';
export interface Connector { id: string; source_id: string; name: string; kind: string; enabled: boolean; secret_ref?: string; config: Record<string, unknown>; created_at: string }
export const listConnectors = (c: Ctx, base: string, cursor?: string) =>
  call<{ items: Connector[]; next_cursor: string }>(c, base, '/v1/connectors', { query: { cursor } });
/** Idempotent; pulls stop at the next lease after pause. */
export const setConnectorEnabled = (c: Ctx, base: string, id: string, enabled: boolean) =>
  call<Connector>(c, base, `/v1/connectors/${encodeURIComponent(id)}/${enabled ? 'resume' : 'pause'}`, { method: 'POST' });
export const CONNECTOR_KINDS = ['REST_PULL', 'POSTGRES_CDC', 'MYSQL_CDC'] as const;
export interface NewConnector { source_id: string; name: string; kind: string; secret_ref: string; config?: Record<string, unknown> }
/** Client-side mirror of the server rules, only to fail fast; the server stays authoritative. The tenant is never sent. */
export function validateConnector(n: NewConnector): string | undefined {
  if (!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(n.source_id)) return 'source id must be a UUID';
  if (!n.name.trim() || n.name.length > 128) return 'name is required (up to 128 characters)';
  if (!(CONNECTOR_KINDS as readonly string[]).includes(n.kind)) return 'unknown connector kind';
  if (!/^[a-z0-9+.-]+:\/\//.test(n.secret_ref) || n.secret_ref.length > 512) return 'secret ref must look like env://NAME (a reference, never the secret itself)';
  return undefined;
}
export function parseConfig(s: string): Record<string, unknown> {
  if (!s.trim()) return {};
  const v = JSON.parse(s) as unknown;
  if (!v || typeof v !== 'object' || Array.isArray(v)) throw new Error('config must be a JSON object');
  if (Object.keys(v).length > 32) throw new Error('config has too many keys (max 32)');
  return v as Record<string, unknown>;
}
export const createConnector = (c: Ctx, base: string, n: NewConnector) =>
  call<Connector>(c, base, '/v1/connectors', { method: 'POST', body: { source_id: n.source_id, name: n.name.trim(), kind: n.kind, secret_ref: n.secret_ref, ...(n.config && Object.keys(n.config).length ? { config: n.config } : {}) } });
