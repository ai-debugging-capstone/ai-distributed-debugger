-- Idempotent integrity upgrade for databases initialized before the experiment
-- controller existed.  New databases receive the same constraints directly in
-- 001_schema.sql; operators may safely apply this file once to an existing
-- PostgreSQL 15+ database.

BEGIN;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'experiment_runs'::regclass
          AND conname = 'experiment_runs_valid_interval'
    ) THEN
        ALTER TABLE experiment_runs
            ADD CONSTRAINT experiment_runs_valid_interval
            CHECK (ended_at IS NULL OR started_at IS NULL OR ended_at >= started_at)
            NOT VALID;
        ALTER TABLE experiment_runs
            VALIDATE CONSTRAINT experiment_runs_valid_interval;
    END IF;
END
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'incidents'::regclass
          AND conname = 'incidents_id_run_key'
    ) THEN
        ALTER TABLE incidents
            ADD CONSTRAINT incidents_id_run_key UNIQUE (id, run_id);
    END IF;
END
$$;

DO $$
DECLARE
    old_constraint text;
    old_foreign_key text;
BEGIN
    -- Drop only the original four-column identity constraint. It collapsed
    -- labels for distinct incidents in the same experiment into one row.
    SELECT conname INTO old_constraint
    FROM pg_constraint
    WHERE conrelid = 'ground_truth_labels'::regclass
      AND contype = 'u'
      AND pg_get_constraintdef(oid) =
          'UNIQUE (run_id, entity_type, entity_id, fault_class)'
    LIMIT 1;
    IF old_constraint IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE ground_truth_labels DROP CONSTRAINT %I',
            old_constraint
        );
    END IF;

    -- The legacy single-column FK cannot enforce run ownership and its
    -- SET-NULL action can collide with the NULLS-NOT-DISTINCT identity key.
    SELECT conname INTO old_foreign_key
    FROM pg_constraint
    WHERE conrelid = 'ground_truth_labels'::regclass
      AND contype = 'f'
      AND pg_get_constraintdef(oid) LIKE
          'FOREIGN KEY (incident_id) REFERENCES incidents(id) ON DELETE SET NULL%'
    LIMIT 1;
    IF old_foreign_key IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE ground_truth_labels DROP CONSTRAINT %I',
            old_foreign_key
        );
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ground_truth_labels'::regclass
          AND conname = 'ground_truth_labels_incident_run_fkey'
          AND pg_get_constraintdef(oid) NOT LIKE '%ON DELETE CASCADE%'
    ) THEN
        ALTER TABLE ground_truth_labels
            DROP CONSTRAINT ground_truth_labels_incident_run_fkey;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ground_truth_labels'::regclass
          AND conname = 'ground_truth_labels_identity_key'
    ) THEN
        ALTER TABLE ground_truth_labels
            ADD CONSTRAINT ground_truth_labels_identity_key
            UNIQUE NULLS NOT DISTINCT (
                run_id, incident_id, entity_type, entity_id, fault_class
            );
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ground_truth_labels'::regclass
          AND conname = 'ground_truth_labels_incident_run_fkey'
    ) THEN
        ALTER TABLE ground_truth_labels
            ADD CONSTRAINT ground_truth_labels_incident_run_fkey
            FOREIGN KEY (incident_id, run_id)
            REFERENCES incidents(id, run_id) ON DELETE CASCADE
            NOT VALID;
        ALTER TABLE ground_truth_labels
            VALIDATE CONSTRAINT ground_truth_labels_incident_run_fkey;
    END IF;
END
$$;

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '2')
ON CONFLICT (key) DO UPDATE
SET value = GREATEST(schema_metadata.value::integer, EXCLUDED.value::integer)::text,
    updated_at = now();

COMMIT;
