import { call, type Ctx } from './http';
export interface QuarantineRecord {
  id: string; source_id?: string; batch_id?: string; job_id?: string; event_id?: string; processing_version?: number; pipeline_version?: number;
  reason_code: string; reason?: string; stage: string; quarantined_at: string; original?: unknown;
}
export const listQuarantine = (c: Ctx, base: string, cursor?: string) =>
  call<{ items: QuarantineRecord[]; next_cursor: string }>(c, base, '/v1/quarantine', { query: { limit: '50', cursor } });
export interface ReplayPlan { dry_run: boolean; candidate_count: number; candidate_count_is_lower_bound: boolean; executable: boolean; [k: string]: unknown }
/** Read-only count; publishing a replay is deliberately not offered by this UI. */
export const planReplay = (c: Ctx, base: string, r: { source_id: string; occurred_from: string; occurred_to: string; pipeline_version: number }) =>
  call<ReplayPlan>(c, base, '/v1/quarantine/replays/dry-run', { method: 'POST', body: r });
