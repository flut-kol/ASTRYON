-- ROLLBACK PLAN: forward-only. Non-prod: ALTER TABLE jobs DROP COLUMN metadata.
-- GPS metadata is separate from content_labels: labels never carry values.
ALTER TABLE jobs ADD COLUMN metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE jobs ADD CONSTRAINT jobs_metadata_object_ck CHECK (jsonb_typeof(metadata) = 'object');
