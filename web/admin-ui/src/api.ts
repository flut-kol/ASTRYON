import type { ApiErrorBody, Health, Job, MetricsSummary, Page, QualitySummary, Source } from './types';

export interface Range { from?: string; to?: string; source_id?: string }

export class ApiError extends Error {
  constructor(public status: number, public code: string, message: string, public traceId?: string, public details?: unknown) {
    super(message);
  }
}

export interface ClientOptions {
  baseUrl?: string;
  getToken: () => string | null;
  fetchFn?: typeof fetch;
}

export class ApiClient {
  private base: string; private fetchFn: typeof fetch;
  constructor(private opts: ClientOptions) {
    this.base = (opts.baseUrl ?? '').replace(/\/$/, '');
    this.fetchFn = opts.fetchFn ?? ((...a) => fetch(...a));
  }

  private async get<T>(path: string, query?: Record<string, string | undefined>, okStatuses: number[] = []): Promise<T> {
    const qs = new URLSearchParams();
    for (const [k, v] of Object.entries(query ?? {})) if (v) qs.set(k, v);
    const url = this.base + path + (qs.size ? `?${qs}` : '');
    const headers: Record<string, string> = { Accept: 'application/json' };
    const tok = this.opts.getToken();
    if (tok) headers.Authorization = `Bearer ${tok}`; // tenant comes from token claims only
    let res: Response;
    try { res = await this.fetchFn(url, { headers }); }
    catch (e) { throw new ApiError(0, 'network_error', e instanceof Error ? e.message : 'network error'); }
    const reqId = res.headers.get('X-Request-ID') ?? undefined;
    if (!res.ok && !okStatuses.includes(res.status)) {
      let body: Partial<ApiErrorBody> = {};
      try { body = await res.json(); } catch { /* non-JSON error */ }
      throw new ApiError(res.status, body.code ?? `http_${res.status}`, body.message ?? res.statusText ?? 'request failed', body.trace_id ?? reqId, body.details);
    }
    return (await res.json()) as T;
  }

  // from/to are RFC 3339; server defaults to the last 24h. tenant is never a parameter.
  metrics(r: Range = {}) { return this.get<MetricsSummary>('/v1/metrics/summary', { ...r }); }
  quality(r: Range = {}) { return this.get<QualitySummary>('/v1/quality/summary', { ...r }); }
  sources(cursor?: string) { return this.get<Page<Source>>('/v1/sources', { cursor }); }
  jobs(cursor?: string, status?: string) { return this.get<Page<Job>>('/v1/jobs', { cursor, status }); }
  // 503 carries a valid Health body (dependency down), so it is returned, not thrown.
  health() { return this.get<Health>('/healthz', undefined, [503]); }
}
