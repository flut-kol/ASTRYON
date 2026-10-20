// Mirrors docs/api/openapi.yaml (components.schemas).
export interface ApiErrorBody { code: string; message: string; details?: unknown; trace_id: string }
export interface SourceMetrics { source_id: string; events: number; bytes: number; batches: number; errors: number; max_ingest_lag_ms: number }
export interface MetricsSummary {
  from: string; to: string; totals: SourceMetrics; by_source: SourceMetrics[];
  series: { hour: string; events: number; errors: number }[];
}
export interface QualitySummary {
  from: string; to: string; events_total: number; events_valid: number; events_rejected: number; events_with_flags: number;
  flags: { flag: string; count: number }[];
  top_errors: { error_code: string; stage: string; count: number }[];
}
export interface Source { id: string; name: string; kind: string; is_enabled: boolean; created_at: string; updated_at: string }
export type JobStatus = 'NEW' | 'QUEUED' | 'RUNNING' | 'RETRYING' | 'SUCCEEDED' | 'FAILED' | 'CANCELLED';
export interface Job {
  id: string; kind: string; status: JobStatus; source_id: string | null; batch_id: string | null;
  attempt_count: number; max_attempts: number; events_processed: number; events_failed: number;
  last_error: string | null; created_at: string; started_at: string | null; finished_at: string | null;
}
export interface Page<T> { items: T[]; next_cursor: string | null }
export interface Health { status: 'ok' | 'degraded'; checks: { clickhouse?: 'ok' | 'down'; postgres?: 'ok' | 'down' } }
