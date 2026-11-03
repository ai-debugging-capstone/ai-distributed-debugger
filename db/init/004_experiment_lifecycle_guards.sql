-- Guard the internal live-telemetry namespace and make injected-fault event
-- boundaries durable across retries. Safe to apply repeatedly on PostgreSQL 15+.

BEGIN;

CREATE OR REPLACE FUNCTION protect_reserved_experiment_run()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.id = '00000000-0000-0000-0000-000000000000'::uuid THEN
        RAISE EXCEPTION 'the reserved live-telemetry experiment cannot be mutated'
            USING ERRCODE = '23514';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS protect_reserved_experiment_run_trigger ON experiment_runs;
CREATE TRIGGER protect_reserved_experiment_run_trigger
    BEFORE UPDATE OR DELETE ON experiment_runs
    FOR EACH ROW EXECUTE FUNCTION protect_reserved_experiment_run();

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'fault_injections'::regclass
          AND conname = 'fault_injections_not_reserved'
    ) THEN
        ALTER TABLE fault_injections
            ADD CONSTRAINT fault_injections_not_reserved
            CHECK (run_id <> '00000000-0000-0000-0000-000000000000'::uuid)
            NOT VALID;
        ALTER TABLE fault_injections
            VALIDATE CONSTRAINT fault_injections_not_reserved;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'fault_injections'::regclass
          AND conname = 'fault_injections_valid_interval'
    ) THEN
        ALTER TABLE fault_injections
            ADD CONSTRAINT fault_injections_valid_interval
            CHECK (cleared_at IS NULL OR cleared_at >= injected_at)
            NOT VALID;
        ALTER TABLE fault_injections
            VALIDATE CONSTRAINT fault_injections_valid_interval;
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'ground_truth_labels'::regclass
          AND conname = 'ground_truth_labels_not_reserved'
    ) THEN
        ALTER TABLE ground_truth_labels
            ADD CONSTRAINT ground_truth_labels_not_reserved
            CHECK (run_id <> '00000000-0000-0000-0000-000000000000'::uuid)
            NOT VALID;
        ALTER TABLE ground_truth_labels
            VALIDATE CONSTRAINT ground_truth_labels_not_reserved;
    END IF;
END
$$;

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '4')
ON CONFLICT (key) DO UPDATE
SET value = GREATEST(schema_metadata.value::integer, EXCLUDED.value::integer)::text,
    updated_at = now();

COMMIT;
