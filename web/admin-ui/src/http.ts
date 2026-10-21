export class ApiError extends Error {
  constructor(public status: number, public code: string, message: string, public traceId?: string) { super(message); }
  /** Endpoint absent in this build / service not reachable: UI shows "not available" instead of an error. */
  get unavailable() { return this.status === 404 || this.status === 405 || this.status === 501 || this.status === 502 || this.status === 503 || this.status === 504 || this.status === 0 || (this.status === 500 && this.code === 'http_500'); }
}
export interface Ctx { getToken: () => string | null; fetchFn?: typeof fetch }

/** Parses both error shapes: {code,message,trace_id} and {error:{code,message,trace_id}}. */
export function parseError(status: number, body: unknown, fallback: string, reqId?: string): ApiError {
  const b = (body && typeof body === 'object' ? body : {}) as Record<string, unknown>;
  const e = (b.error && typeof b.error === 'object' ? b.error : b) as Record<string, unknown>;
  const s = (v: unknown) => (typeof v === 'string' ? v : undefined);
  return new ApiError(status, s(e.code) ?? `http_${status}`, s(e.message) ?? fallback, s(e.trace_id) ?? reqId);
}

export async function call<T>(ctx: Ctx, base: string, path: string, init: { method?: string; query?: Record<string, string | undefined>; body?: unknown } = {}): Promise<T> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(init.query ?? {})) if (v) qs.set(k, v);
  const url = base.replace(/\/$/, '') + path + (qs.toString() ? `?${qs}` : '');
  const headers: Record<string, string> = { Accept: 'application/json' };
  const tok = ctx.getToken();
  if (tok) headers.Authorization = `Bearer ${tok}`; // tenant comes from token claims only; never sent by UI
  if (init.body !== undefined) headers['Content-Type'] = 'application/json';
  const f = ctx.fetchFn ?? ((...a: Parameters<typeof fetch>) => fetch(...a));
  let res: Response;
  try {
    res = await f(url, { method: init.method ?? 'GET', headers, body: init.body === undefined ? undefined : JSON.stringify(init.body) });
  } catch (e) { throw new ApiError(0, 'network_error', e instanceof Error ? e.message : 'network error'); }
  const reqId = res.headers.get('X-Request-ID') ?? undefined;
  if (!res.ok) {
    let body: unknown; try { body = await res.json(); } catch { /* non-JSON */ }
    throw parseError(res.status, body, res.statusText || 'request failed', reqId);
  }
  try { return (await res.json()) as T; } catch { throw new ApiError(res.status, 'bad_response', 'response is not JSON', reqId); }
}
