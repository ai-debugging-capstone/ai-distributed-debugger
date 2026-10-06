-- Preserve the exact pre-rerank feature vector used by online learned-model
-- inference so later exports cannot reconstruct a different vector from the
-- learned score/rank that replaced the heuristic values.

BEGIN;

ALTER TABLE candidate_rankings
    ADD COLUMN IF NOT EXISTS online_features jsonb NOT NULL DEFAULT '{}'::jsonb;

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '3')
ON CONFLICT (key) DO UPDATE
SET value = GREATEST(schema_metadata.value::integer, EXCLUDED.value::integer)::text,
    updated_at = now();

COMMIT;
