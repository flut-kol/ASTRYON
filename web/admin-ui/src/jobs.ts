import { call, type Ctx } from './http';
export interface Job {
  id: string; kind: string; status: string; source_id?: string | null; file_object_id?: string | null; attempt_count: number; max_attempts: number;
  events_processed: number; events_failed: number; error?: string; labels?: Record<string, number>; created_at: string; started_at?: string | null; finished_at?: string | null;
}
/** Read-only; served by the file API (scope astryon:jobs:read). */
export const listJobs = (c: Ctx, base: string, q: { status?: string; source_id?: string; cursor?: string }) =>
  call<{ items: Job[]; next_cursor: string }>(c, base, '/v1/jobs', { query: { limit: '50', ...q } });

export const retryUpload = (c: Ctx, base: string, fileObjectID: string) =>
  call<{ file_object_id: string; status: string; job_id?: string; mode?: string }>(c, base, `/v1/files/uploads/${encodeURIComponent(fileObjectID)}/retry`, { method: 'POST', body: {} });
