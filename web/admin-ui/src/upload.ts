import { call, ApiError, parseError, type Ctx } from './http';

export const SOURCE_TYPES = ['unknown', 'telemetry', 'geo', 'report', 'export', 'config', 'log', 'credentials_bundle', 'personal_data', 'financial', 'media_meta', 'reference'] as const;
export const SENSITIVITIES = ['public', 'internal', 'restricted'] as const;
export const STATUSES = ['NEW', 'QUEUED', 'RUNNING', 'RETRYING', 'SUCCEEDED', 'FAILED', 'CANCELLED', 'QUARANTINED'] as const;
export const PART_SIZE = 8 * 1024 * 1024; // S3 minimum for non-last parts is 5 MiB
export const MAX_BROWSER_BYTES = 256 * 1024 * 1024; // whole file is hashed in memory; larger files need a CLI/uploader

export interface Labels { [name: string]: { count: number; fields: string[] } }
export interface JobInfo { id: string; status: string; error?: string; labels: Labels }
export interface UploadItem {
  file_object_id: string; source_id: string; source_type: string; committed: boolean; size_bytes: number;
  sensitivity: string; tags: Record<string, string>; created_at: string; job?: JobInfo;
  profile?: { retention_class?: string; required_read_scope?: string; export_allowed?: boolean };
}
export interface UploadPage { items: UploadItem[]; next_cursor?: string }
export interface UploadSession { file_object_id: string; upload_id: string; part_urls: string[]; expires_at: string }

export function parseTags(s: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const p of s.split(',').map((x) => x.trim()).filter(Boolean)) {
    const i = p.indexOf('=');
    if (i < 1) throw new Error(`tag "${p}" must be key=value`);
    out[p.slice(0, i).trim()] = p.slice(i + 1).trim();
  }
  return out;
}
export function planParts(size: number, partSize = PART_SIZE): number { return Math.max(1, Math.ceil(size / partSize)); }
export async function sha256Hex(buf: ArrayBuffer): Promise<string> {
  const d = await crypto.subtle.digest('SHA-256', buf);
  return [...new Uint8Array(d)].map((b) => b.toString(16).padStart(2, '0')).join('');
}
/** Pre-signed URLs are absolute S3 URLs; in dev they go through the same-origin /api/s3 proxy (no S3 CORS needed). */
export function rewritePartUrl(u: string, s3Proxy: string | undefined): string {
  if (!s3Proxy) return u;
  const x = new URL(u);
  return s3Proxy.replace(/\/$/, '') + x.pathname + x.search;
}
export function labelSummary(l: Labels | undefined): string {
  const e = Object.entries(l ?? {});
  return e.length ? e.map(([k, v]) => `${k.replace(/^contains_/, '')}:${v.count}`).join(' ') : '-';
}

export interface ListFilter { source_type?: string; sensitivity?: string; tag?: string; status?: string; label?: string; cursor?: string; limit?: string }
export const listUploads = (c: Ctx, base: string, f: ListFilter) => call<UploadPage>(c, base, '/v1/files/uploads', { query: { ...f } });
export const getUpload = (c: Ctx, base: string, id: string) => call<UploadItem>(c, base, `/v1/files/uploads/${encodeURIComponent(id)}`);

export type UploadPreview = { mode: 'content' | 'metadata-only'; content_type?: string; content?: string; reason?: string; [key: string]: unknown };
/** Fetches the bounded, policy-filtered preview. The endpoint may return text for safe files or JSON metadata. */
export async function previewUpload(c: Ctx, base: string, id: string): Promise<UploadPreview> {
  const f = c.fetchFn ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const headers: Record<string, string> = { Accept: 'text/plain, application/json' };
  const tok = c.bff ? null : c.getToken();
  if (tok) headers.Authorization = `Bearer ${tok}`;
  let res: Response;
  try { res = await f(`${base.replace(/\/$/, '')}/v1/files/uploads/${encodeURIComponent(id)}/preview`, { headers, credentials: 'same-origin' }); }
  catch (e) { throw new ApiError(0, 'network_error', e instanceof Error ? e.message : 'network error'); }
  const reqId = res.headers.get('X-Request-ID') ?? undefined;
  if (!res.ok) { let body: unknown; try { body = await res.json(); } catch { /* non-JSON */ } throw parseError(res.status, body, res.statusText || 'request failed', reqId); }
  const ct = res.headers.get('Content-Type') ?? '';
  if (ct.includes('json')) return { ...(await res.json() as Record<string, unknown>), mode: 'metadata-only' };
  return { mode: 'content', content_type: ct, content: await res.text() };
}

export interface UploadParams { file: File; source_id: string; source_type: string; sensitivity?: string; tags: Record<string, string> }
export async function uploadFile(c: Ctx, base: string, p: UploadParams, opts: { s3Proxy?: string; onProgress?: (msg: string) => void } = {}): Promise<{ file_object_id: string; job_id: string }> {
  const say = opts.onProgress ?? (() => {});
  if (p.file.size > MAX_BROWSER_BYTES) throw new Error(`file larger than ${MAX_BROWSER_BYTES / 1024 / 1024} MiB: use a CLI uploader`);
  if (p.file.size === 0) throw new Error('empty file');
  say('hashing');
  const buf = await p.file.arrayBuffer();
  const sha256 = await sha256Hex(buf);
  const parts = planParts(buf.byteLength);
  say('creating session');
  const body: Record<string, unknown> = {
    source_id: p.source_id, filename: p.file.name, content_type: p.file.type || guessType(p.file.name), sha256,
    size_bytes: buf.byteLength, parts, source_type: p.source_type, tags: p.tags,
  };
  if (p.sensitivity) body.sensitivity = p.sensitivity;
  const sess = await call<UploadSession>(c, base, '/v1/files/uploads', { method: 'POST', body });
  if (sess.part_urls.length !== parts) throw new ApiError(0, 'bad_response', 'part_urls count mismatch');
  const f = c.fetchFn ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  const etags: { number: number; etag: string }[] = [];
  for (let i = 0; i < parts; i++) {
    say(`part ${i + 1}/${parts}`);
    const res = await f(rewritePartUrl(sess.part_urls[i], opts.s3Proxy), { method: 'PUT', body: buf.slice(i * PART_SIZE, (i + 1) * PART_SIZE) });
    if (!res.ok) throw new ApiError(res.status, 'part_upload_failed', `part ${i + 1} PUT failed (${res.status})`);
    const etag = res.headers.get('ETag');
    if (!etag) throw new ApiError(0, 'etag_unreadable', 'part ETag not readable (S3 CORS must expose ETag, or use the dev proxy)');
    etags.push({ number: i + 1, etag });
  }
  say('completing');
  const done = await call<{ file_object_id: string; job_id: string }>(c, base, `/v1/files/uploads/${sess.file_object_id}/complete`, { method: 'POST', body: { upload_id: sess.upload_id, parts: etags } });
  return done;
}
function guessType(n: string) { return /\.csv$/i.test(n) ? 'text/csv' : 'application/x-ndjson'; }
