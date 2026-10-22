import { call, type Ctx } from './http';

export type OpType =
  | 'rename_column' | 'split_column' | 'merge_columns' | 'find_replace'
  | 'delete_column' | 'reorder_columns' | 'trim_whitespace' | 'drop_empty_rows';

export const OP_TYPES: OpType[] = ['rename_column', 'split_column', 'merge_columns', 'find_replace',
  'delete_column', 'reorder_columns', 'trim_whitespace', 'drop_empty_rows'];

export interface OpSpec {
  type: OpType;
  column?: string; columns?: string[];
  new_name?: string; new_column?: string; separator?: string;
  delimiter?: string; new_columns?: string[]; drop_original?: boolean;
  all_columns?: boolean; find?: string; replace?: string; regex?: boolean; case_sensitive?: boolean;
  order?: string[];
}

export interface FormatOptions { input_header_row?: boolean; output_header_row?: boolean; output_delimiter?: string }

export interface Warning { row_number: number; message: string }
export interface PreviewResult { header: string[]; rows: string[][]; truncated: boolean; warnings: Warning[] }
export interface VersionSummary {
  version_id: string; file_object_id: string; version_number: number; header: string[];
  output_delimiter: string; header_row: boolean; size_bytes: number; row_count: number;
  download_url?: string; download_expires_at?: string; created_at: string;
}

const path = (fileId: string, tail = '') => `/v1/files/uploads/${encodeURIComponent(fileId)}${tail}`;

export const preview = (c: Ctx, base: string, fileId: string, ops: OpSpec[], fmt: FormatOptions, limit?: number) =>
  call<PreviewResult>(c, base, path(fileId, '/preview'), { method: 'POST', body: { ops, ...fmt, limit } });

export const saveVersion = (c: Ctx, base: string, fileId: string, ops: OpSpec[], fmt: FormatOptions, filename?: string) =>
  call<VersionSummary>(c, base, path(fileId, '/versions'), { method: 'POST', body: { ops, ...fmt, filename } });

export const listVersions = (c: Ctx, base: string, fileId: string) =>
  call<{ items: VersionSummary[] }>(c, base, path(fileId, '/versions'));

export const getVersion = (c: Ctx, base: string, fileId: string, versionId: string) =>
  call<VersionSummary>(c, base, path(fileId, `/versions/${encodeURIComponent(versionId)}`));

// Module-level hand-off for the file being edited: the hash router matches page keys exactly (no params), so
// the selected file id travels this way instead, same pattern as auth.ts's getSource()/setSource().
let target: string | null = null;
export function getEditTarget(): string | null { return target; }
export function setEditTarget(id: string | null) { target = id; }

/** Default op fields for a freshly added row of this type (kept minimal; the server validates fully). */
export function defaultOp(type: OpType): OpSpec {
  switch (type) {
    case 'rename_column': return { type, column: '', new_name: '' };
    case 'split_column': return { type, column: '', delimiter: '', new_columns: ['', ''] };
    case 'merge_columns': return { type, columns: ['', ''], new_column: '', separator: ' ' };
    case 'find_replace': return { type, column: '', find: '', replace: '' };
    case 'delete_column': return { type, column: '' };
    case 'reorder_columns': return { type, order: [] };
    case 'trim_whitespace': return { type, column: '' };
    case 'drop_empty_rows': return { type };
  }
}

/** Light client-side guard only; internal/transform on the server is the source of truth. */
export function validateOp(op: OpSpec): string | undefined {
  switch (op.type) {
    case 'rename_column':
      return !op.column || !op.new_name ? 'column and new name are required' : undefined;
    case 'split_column':
      return !op.column || !op.delimiter || (op.new_columns ?? []).filter(Boolean).length < 2
        ? 'column, delimiter and at least 2 new column names are required' : undefined;
    case 'merge_columns':
      return (op.columns ?? []).filter(Boolean).length < 2 || !op.new_column
        ? 'at least 2 columns and a new column name are required' : undefined;
    case 'find_replace':
      return (!op.all_columns && !op.column) || !op.find ? 'a scope and a non-empty find value are required' : undefined;
    case 'delete_column':
      return !op.column ? 'column is required' : undefined;
    case 'reorder_columns':
      return (op.order ?? []).length < 1 ? 'target order is required' : undefined;
    case 'trim_whitespace':
      return !op.all_columns && !op.column ? 'a scope is required' : undefined;
    case 'drop_empty_rows':
      return undefined;
  }
}
