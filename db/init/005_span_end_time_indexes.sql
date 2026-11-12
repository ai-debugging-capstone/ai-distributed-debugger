-- The RCA worker buckets spans by completion time (a span is exported, and so
-- becomes observable, only when it ends). Index end_time the same way
-- start_time is indexed so per-run and live windows stay cheap.
-- Safe to apply repeatedly on PostgreSQL 15+.

BEGIN;

CREATE INDEX IF NOT EXISTS spans_run_end_time_idx ON spans (run_id, end_time);
CREATE INDEX IF NOT EXISTS spans_end_time_brin_idx ON spans USING brin (end_time);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '5')
ON CONFLICT (key) DO UPDATE
SET value = GREATEST(schema_metadata.value::integer, EXCLUDED.value::integer)::text,
    updated_at = now();

COMMIT;
