-- ROLLBACK PLAN: forward-only. Non-prod: drop the two constraints.
-- Prod: ship a forward migration after a backup and compatibility review.
-- Existing rows are checked before constraints are installed.

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM file_batch_lineage
    WHERE source_checksum !~ '^[0-9a-fA-F]{64}$'
       OR valid_rows < 0
       OR invalid_rows < 0
       OR valid_rows + invalid_rows <> last_row - first_row + 1
  ) THEN
    RAISE EXCEPTION 'file_batch_lineage contains rows incompatible with 0018 invariants';
  END IF;
END
$$;

ALTER TABLE file_batch_lineage
  ADD CONSTRAINT file_batch_lineage_sha256_ck
    CHECK (source_checksum ~ '^[0-9a-fA-F]{64}$'),
  ADD CONSTRAINT file_batch_lineage_row_count_ck
    CHECK (valid_rows + invalid_rows = last_row - first_row + 1);
