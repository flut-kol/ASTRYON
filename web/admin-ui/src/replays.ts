import { call, type Ctx } from './http';
export interface Replay {
  id: string; source_id: string; processing_version: number; status: string; time_from?: string; time_to?: string;
  job_id?: string; created_at: string; updated_at: string; completed_at?: string;
}
export const REPLAY_STATUSES = ['REQUESTED', 'APPROVED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED'];
/** Read-only: creating and activating replays stays out of the UI. */
export const listReplays = (c: Ctx, base: string, q: { source_id?: string; status?: string; cursor?: string }) =>
  call<{ items: Replay[]; next_cursor: string }>(c, base, '/v1/replays', { query: { limit: '50', ...q } });
