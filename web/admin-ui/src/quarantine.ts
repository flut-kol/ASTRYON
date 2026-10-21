import { call, type Ctx } from './http';
export interface QuarantineRecord {
  id: string; source_id?: string; batch_id?: string; job_id?: string; event_id?: string; processing_version?: number; pipeline_version?: number;
  reason_code: string; reason?: string; stage: string; quarantined_at: string; original?: unknown;
}
export const listQuarantine = (c: Ctx, base: string, cursor?: string) =>
  call<{ items: QuarantineRecord[]; next_cursor: string }>(c, base, '/v1/quarantine', { query: { limit: '50', cursor } });
export interface ReplayPlan { dry_run: boolean; candidate_count: number; candidate_count_is_lower_bound: boolean; executable: boolean; [k: string]: unknown }
export interface ReplayRequest { source_id: string; occurred_from: string; occurred_to: string; pipeline_version: number }
export const MAX_REPLAY_RECORDS = 10000; // the service refuses larger plans; mirrored here only to fail fast
/** Read-only count (dry run). */
export const planReplay = (c: Ctx, base: string, r: ReplayRequest) =>
  call<ReplayPlan>(c, base, '/v1/quarantine/replays/dry-run', { method: 'POST', body: r });
/** A plan may be published only when its count is exact, non-zero and within the service limit. */
export const canPublish = (p: ReplayPlan) => p.executable && !p.candidate_count_is_lower_bound && p.candidate_count > 0 && p.candidate_count <= MAX_REPLAY_RECORDS;
/** Publishes the bounded replay job. Needs the separate astryon:quarantine-replay:write scope; the service audits before publishing. */
export const publishReplay = (c: Ctx, base: string, r: ReplayRequest) =>
  call<{ job_id: string; status: string; candidate_count: number }>(c, base, '/v1/quarantine/replays', { method: 'POST', body: r });
