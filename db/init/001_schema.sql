-- Dependency-Aware Debugger schema
-- PostgreSQL is the durable backing store; product behavior lives in the
-- telemetry, analytics, ranking, evidence, and runbook services.

SET TIME ZONE 'UTC';

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS schema_metadata (
    key text PRIMARY KEY,
    value text NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO schema_metadata (key, value)
VALUES ('schema_version', '1')
ON CONFLICT (key) DO UPDATE
SET value = GREATEST(schema_metadata.value::integer, EXCLUDED.value::integer)::text,
    updated_at = now();

-- Reproducible experiment and dataset metadata is intentionally kept separate
-- from model features so fault labels cannot leak into training data.
CREATE TABLE IF NOT EXISTS experiment_runs (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    source_repository text,
    source_version text,
    source_sha text,
    seed bigint,
    workload text,
    status text NOT NULL DEFAULT 'created'
        CHECK (status IN ('created', 'warming', 'running', 'recovering', 'completed', 'failed')),
    split text CHECK (split IS NULL OR split IN ('train', 'validation', 'test', 'holdout')),
    started_at timestamptz,
    ended_at timestamptz,
    configuration jsonb NOT NULL DEFAULT '{}'::jsonb,
    environment jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT experiment_runs_valid_interval
        CHECK (ended_at IS NULL OR started_at IS NULL OR ended_at >= started_at)
);

-- Reserved namespace for ordinary, unlabeled live telemetry. Experiment
-- controllers replace this UUID at run boundaries; exporters exclude this
-- running, unlabeled row from training data.
INSERT INTO experiment_runs (
    id, name, source_repository, source_version, status, configuration
) VALUES (
    '00000000-0000-0000-0000-000000000000'::uuid,
    'unscoped-live-telemetry',
    'local/live-stream',
    '1',
    'running',
    '{"reserved":true,"training_eligible":false}'::jsonb
)
ON CONFLICT (id) DO NOTHING;

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

CREATE TABLE IF NOT EXISTS fault_injections (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id uuid NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
    flag_name text NOT NULL,
    variant text,
    injector text NOT NULL,
    causal_entity_type text NOT NULL,
    causal_entity_id text NOT NULL,
    causal_edge_source text,
    causal_edge_target text,
    mechanism text NOT NULL,
    impacted_sli text NOT NULL,
    injected_at timestamptz NOT NULL,
    cleared_at timestamptz,
    expected_runbook_key text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT fault_injections_not_reserved
        CHECK (run_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CONSTRAINT fault_injections_valid_interval
        CHECK (cleared_at IS NULL OR cleared_at >= injected_at)
);

CREATE INDEX IF NOT EXISTS fault_injections_run_time_idx
    ON fault_injections (run_id, injected_at);

CREATE TABLE IF NOT EXISTS services (
    id bigserial PRIMARY KEY,
    service_key text NOT NULL UNIQUE,
    name text NOT NULL,
    namespace text,
    version text,
    entity_type text NOT NULL DEFAULT 'service',
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS service_instances (
    id bigserial PRIMARY KEY,
    service_id bigint NOT NULL REFERENCES services(id) ON DELETE CASCADE,
    instance_key text NOT NULL,
    first_seen timestamptz NOT NULL DEFAULT now(),
    last_seen timestamptz NOT NULL DEFAULT now(),
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (service_id, instance_key)
);

CREATE TABLE IF NOT EXISTS spans (
    id bigserial PRIMARY KEY,
    trace_id text NOT NULL,
    span_id text NOT NULL,
    parent_span_id text,
    service_name text NOT NULL,
    service_namespace text,
    service_instance_id text,
    operation_name text NOT NULL,
    span_kind text,
    start_time timestamptz NOT NULL,
    end_time timestamptz NOT NULL,
    duration_ms double precision NOT NULL CHECK (duration_ms >= 0),
    status_code text,
    status_message text,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    resource_attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    scope_name text,
    scope_version text,
    run_id uuid REFERENCES experiment_runs(id) ON DELETE SET NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    raw jsonb,
    UNIQUE (trace_id, span_id)
);

CREATE INDEX IF NOT EXISTS spans_service_time_idx ON spans (service_name, start_time DESC);
CREATE INDEX IF NOT EXISTS spans_trace_idx ON spans (trace_id);
CREATE INDEX IF NOT EXISTS spans_parent_idx ON spans (trace_id, parent_span_id);
CREATE INDEX IF NOT EXISTS spans_run_time_idx ON spans (run_id, start_time);
CREATE INDEX IF NOT EXISTS spans_time_brin_idx ON spans USING brin (start_time);
CREATE INDEX IF NOT EXISTS spans_attributes_gin_idx ON spans USING gin (attributes jsonb_path_ops);

CREATE TABLE IF NOT EXISTS metric_points (
    id bigserial PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    service_name text NOT NULL,
    metric_name text NOT NULL,
    metric_type text NOT NULL,
    time_unix timestamptz NOT NULL,
    start_time timestamptz,
    value double precision,
    count bigint,
    sum double precision,
    min double precision,
    max double precision,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    resource_attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    exemplars jsonb NOT NULL DEFAULT '[]'::jsonb,
    run_id uuid REFERENCES experiment_runs(id) ON DELETE SET NULL,
    observed_at timestamptz NOT NULL DEFAULT now(),
    raw jsonb
);

CREATE INDEX IF NOT EXISTS metric_points_service_metric_time_idx
    ON metric_points (service_name, metric_name, time_unix DESC);
CREATE INDEX IF NOT EXISTS metric_points_run_time_idx ON metric_points (run_id, time_unix);
CREATE INDEX IF NOT EXISTS metric_points_time_brin_idx ON metric_points USING brin (time_unix);

CREATE TABLE IF NOT EXISTS log_records (
    id bigserial PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    service_name text NOT NULL,
    time_unix timestamptz NOT NULL,
    observed_time timestamptz,
    severity_number integer,
    severity_text text,
    body text,
    trace_id text,
    span_id text,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    resource_attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    run_id uuid REFERENCES experiment_runs(id) ON DELETE SET NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    raw jsonb
);

CREATE INDEX IF NOT EXISTS log_records_service_time_idx ON log_records (service_name, time_unix DESC);
CREATE INDEX IF NOT EXISTS log_records_trace_idx ON log_records (trace_id) WHERE trace_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS log_records_run_time_idx ON log_records (run_id, time_unix);
CREATE INDEX IF NOT EXISTS log_records_time_brin_idx ON log_records USING brin (time_unix);

CREATE TABLE IF NOT EXISTS rejected_records (
    id bigserial PRIMARY KEY,
    signal_type text NOT NULL,
    reason text NOT NULL,
    payload jsonb,
    headers jsonb NOT NULL DEFAULT '{}'::jsonb,
    received_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS rejected_records_received_idx ON rejected_records (received_at DESC);

CREATE TABLE IF NOT EXISTS dependency_edges (
    id bigserial PRIMARY KEY,
    bucket_start timestamptz NOT NULL,
    source_service text NOT NULL,
    target_service text NOT NULL,
    protocol text NOT NULL DEFAULT 'unknown',
    request_count bigint NOT NULL DEFAULT 0,
    error_count bigint NOT NULL DEFAULT 0,
    error_rate double precision NOT NULL DEFAULT 0,
    latency_ms_p50 double precision,
    latency_ms_p95 double precision,
    latency_ms_p99 double precision,
    first_abnormal_at timestamptz,
    run_id uuid REFERENCES experiment_runs(id) ON DELETE CASCADE,
    attributes jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket_start, source_service, target_service, protocol, run_id)
);

CREATE INDEX IF NOT EXISTS dependency_edges_time_idx ON dependency_edges (bucket_start DESC);
CREATE INDEX IF NOT EXISTS dependency_edges_source_target_idx
    ON dependency_edges (source_service, target_service, bucket_start DESC);

CREATE TABLE IF NOT EXISTS baselines (
    id bigserial PRIMARY KEY,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    feature_name text NOT NULL,
    context_key text NOT NULL DEFAULT 'global',
    median double precision,
    mad double precision,
    mean double precision,
    stddev double precision,
    sample_count bigint NOT NULL DEFAULT 0,
    trained_from timestamptz,
    trained_to timestamptz,
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (entity_type, entity_id, feature_name, context_key)
);

CREATE TABLE IF NOT EXISTS feature_buckets (
    id bigserial PRIMARY KEY,
    bucket_start timestamptz NOT NULL,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    run_id uuid REFERENCES experiment_runs(id) ON DELETE CASCADE,
    request_count bigint NOT NULL DEFAULT 0,
    error_count bigint NOT NULL DEFAULT 0,
    error_rate double precision NOT NULL DEFAULT 0,
    latency_ms_p50 double precision,
    latency_ms_p95 double precision,
    latency_ms_p99 double precision,
    cpu_percent double precision,
    memory_bytes double precision,
    log_error_count bigint NOT NULL DEFAULT 0,
    anomaly_score double precision NOT NULL DEFAULT 0,
    onset_score double precision NOT NULL DEFAULT 0,
    features jsonb NOT NULL DEFAULT '{}'::jsonb,
    missingness jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (bucket_start, entity_type, entity_id, run_id)
);

CREATE INDEX IF NOT EXISTS feature_buckets_entity_time_idx
    ON feature_buckets (entity_type, entity_id, bucket_start DESC);
CREATE INDEX IF NOT EXISTS feature_buckets_run_time_idx ON feature_buckets (run_id, bucket_start);
CREATE INDEX IF NOT EXISTS feature_buckets_anomaly_idx
    ON feature_buckets (anomaly_score DESC, bucket_start DESC);

CREATE TABLE IF NOT EXISTS incidents (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    status text NOT NULL DEFAULT 'open'
        CHECK (status IN ('open', 'monitoring', 'resolved', 'inconclusive')),
    started_at timestamptz NOT NULL,
    detected_at timestamptz NOT NULL DEFAULT now(),
    ended_at timestamptz,
    severity text NOT NULL DEFAULT 'warning'
        CHECK (severity IN ('info', 'warning', 'critical')),
    summary text NOT NULL,
    trigger_entity text,
    max_anomaly double precision NOT NULL DEFAULT 0,
    affected_entities text[] NOT NULL DEFAULT ARRAY[]::text[],
    run_id uuid REFERENCES experiment_runs(id) ON DELETE SET NULL,
    model_version text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT incidents_id_run_key UNIQUE (id, run_id)
);

CREATE INDEX IF NOT EXISTS incidents_status_detected_idx ON incidents (status, detected_at DESC);
CREATE INDEX IF NOT EXISTS incidents_run_idx ON incidents (run_id);

CREATE TABLE IF NOT EXISTS incident_events (
    id bigserial PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    occurred_at timestamptz NOT NULL,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    event_type text NOT NULL,
    severity text,
    title text NOT NULL,
    details jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS incident_events_incident_time_idx
    ON incident_events (incident_id, occurred_at);

CREATE TABLE IF NOT EXISTS dataset_snapshots (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    version text NOT NULL,
    source_sha text,
    manifest_uri text,
    checksum text,
    run_count integer NOT NULL DEFAULT 0,
    statistics jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE TABLE IF NOT EXISTS model_versions (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    name text NOT NULL,
    version text NOT NULL,
    model_kind text NOT NULL,
    status text NOT NULL DEFAULT 'challenger'
        CHECK (status IN ('challenger', 'champion', 'retired', 'failed')),
    artifact_uri text,
    artifact_checksum text,
    dataset_snapshot_id uuid REFERENCES dataset_snapshots(id) ON DELETE SET NULL,
    configuration jsonb NOT NULL DEFAULT '{}'::jsonb,
    metrics jsonb NOT NULL DEFAULT '{}'::jsonb,
    trained_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (name, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS one_champion_per_model_idx
    ON model_versions (name) WHERE status = 'champion';

CREATE TABLE IF NOT EXISTS candidate_rankings (
    id bigserial PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    rank integer NOT NULL CHECK (rank > 0),
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    score double precision NOT NULL,
    probability double precision CHECK (probability IS NULL OR probability BETWEEN 0 AND 1),
    fault_class text,
    confidence text CHECK (confidence IS NULL OR confidence IN ('low', 'medium', 'high')),
    evidence_summary text,
    online_features jsonb NOT NULL DEFAULT '{}'::jsonb,
    model_version text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (incident_id, rank),
    UNIQUE (incident_id, entity_type, entity_id, model_version)
);

CREATE INDEX IF NOT EXISTS candidate_rankings_incident_rank_idx
    ON candidate_rankings (incident_id, rank);

CREATE TABLE IF NOT EXISTS evidence_items (
    id bigserial PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    candidate_id bigint REFERENCES candidate_rankings(id) ON DELETE CASCADE,
    evidence_type text NOT NULL,
    entity_id text NOT NULL,
    occurred_at timestamptz,
    signal_name text,
    value double precision,
    baseline double precision,
    contribution double precision,
    summary text,
    details jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS evidence_items_incident_idx ON evidence_items (incident_id, candidate_id);

CREATE TABLE IF NOT EXISTS runbooks (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    runbook_key text NOT NULL UNIQUE,
    fault_class text NOT NULL,
    entity_type text NOT NULL DEFAULT 'any',
    title text NOT NULL,
    description text NOT NULL,
    required_evidence jsonb NOT NULL DEFAULT '[]'::jsonb,
    diagnostic_steps jsonb NOT NULL,
    remediation_steps jsonb NOT NULL,
    verification_steps jsonb NOT NULL,
    rollback_steps jsonb NOT NULL DEFAULT '[]'::jsonb,
    enabled boolean NOT NULL DEFAULT true,
    version integer NOT NULL DEFAULT 1,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS recommendations (
    id bigserial PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    candidate_id bigint REFERENCES candidate_rankings(id) ON DELETE CASCADE,
    runbook_id uuid NOT NULL REFERENCES runbooks(id),
    compatibility_score double precision,
    rationale text,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (incident_id, candidate_id, runbook_id)
);

CREATE TABLE IF NOT EXISTS developer_feedback (
    id bigserial PRIMARY KEY,
    incident_id uuid NOT NULL REFERENCES incidents(id) ON DELETE CASCADE,
    candidate_id bigint REFERENCES candidate_rankings(id) ON DELETE SET NULL,
    verdict text CHECK (verdict IS NULL OR verdict IN ('correct', 'partially_correct', 'incorrect', 'unknown')),
    comment text,
    user_name text,
    accepted boolean,
    actual_entity_type text,
    actual_entity_id text,
    actual_fault_class text,
    notes text,
    metadata jsonb NOT NULL DEFAULT '{}'::jsonb,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS ground_truth_labels (
    id bigserial PRIMARY KEY,
    run_id uuid NOT NULL REFERENCES experiment_runs(id) ON DELETE CASCADE,
    incident_id uuid,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    fault_class text NOT NULL,
    relevance integer NOT NULL DEFAULT 1 CHECK (relevance BETWEEN 0 AND 3),
    is_primary boolean NOT NULL DEFAULT true,
    created_at timestamptz NOT NULL DEFAULT now(),
    CONSTRAINT ground_truth_labels_not_reserved
        CHECK (run_id <> '00000000-0000-0000-0000-000000000000'::uuid),
    CONSTRAINT ground_truth_labels_incident_run_fkey
        FOREIGN KEY (incident_id, run_id)
        REFERENCES incidents(id, run_id) ON DELETE CASCADE,
    CONSTRAINT ground_truth_labels_identity_key
        UNIQUE NULLS NOT DISTINCT (
            run_id, incident_id, entity_type, entity_id, fault_class
        )
);

-- Dashboard-friendly read models. Grafana remains a presentation layer and
-- does not contain product logic.
CREATE OR REPLACE VIEW v_incident_overview AS
SELECT
    i.id,
    i.status,
    i.severity,
    i.started_at,
    i.detected_at,
    i.ended_at,
    i.summary,
    i.trigger_entity,
    i.max_anomaly,
    i.affected_entities,
    c.entity_type AS top_entity_type,
    c.entity_id AS top_entity_id,
    c.probability AS top_probability,
    c.fault_class AS top_fault_class,
    c.confidence,
    c.model_version
FROM incidents i
LEFT JOIN LATERAL (
    SELECT cr.*
    FROM candidate_rankings cr
    WHERE cr.incident_id = i.id
    ORDER BY cr.rank
    LIMIT 1
) c ON true;

CREATE OR REPLACE VIEW v_telemetry_ingest_health AS
SELECT 'spans'::text AS signal, count(*)::bigint AS records, max(observed_at) AS latest_at FROM spans
UNION ALL
SELECT 'metrics', count(*)::bigint, max(observed_at) FROM metric_points
UNION ALL
SELECT 'logs', count(*)::bigint, max(received_at) FROM log_records
UNION ALL
SELECT 'rejected', count(*)::bigint, max(received_at) FROM rejected_records;

INSERT INTO runbooks (
    runbook_key, fault_class, entity_type, title, description,
    required_evidence, diagnostic_steps, remediation_steps, verification_steps, rollback_steps
) VALUES
(
    'database-lock-contention-v1', 'database_lock', 'database',
    'Investigate PostgreSQL lock contention',
    'Identify blocking transactions and validate application transaction boundaries before taking action.',
    '["database_wait_time", "dependency_latency", "pg_lock_evidence"]'::jsonb,
    '["Inspect pg_stat_activity and pg_locks", "Identify the blocking and blocked transaction IDs", "Correlate the blocked query with affected trace IDs"]'::jsonb,
    '["Cancel only a verified safe blocking transaction", "Shorten the application transaction or move slow work outside it", "Add a lock timeout where appropriate"]'::jsonb,
    '["Confirm blocked sessions return to zero", "Confirm dependency p95 latency returns to baseline", "Replay the affected user journey"]'::jsonb,
    '["Restore the previous timeout configuration", "Revert the transaction-boundary change if error rate increases"]'::jsonb
),
(
    'dependency-unreachable-v1', 'dependency_unreachable', 'edge',
    'Restore an unreachable dependency',
    'Verify discovery, routing, health, and timeout behavior on the failing dependency edge.',
    '["connection_errors", "failed_spans", "dependency_edge"]'::jsonb,
    '["Check dependency health and DNS resolution", "Inspect connection-refused and timeout spans", "Validate endpoint and port configuration"]'::jsonb,
    '["Restore the correct endpoint or service", "Use bounded retries with backoff", "Enable a circuit breaker for repeated failures"]'::jsonb,
    '["Run a direct health request", "Confirm successful spans on the dependency edge", "Confirm the affected SLI has recovered"]'::jsonb,
    '["Restore the previous endpoint configuration if the new route is unhealthy"]'::jsonb
),
(
    'resource-saturation-v1', 'resource_saturation', 'service',
    'Investigate CPU or memory saturation',
    'Determine whether load, a leak, garbage collection, or an inefficient operation caused saturation.',
    '["cpu_or_memory_anomaly", "latency_change"]'::jsonb,
    '["Compare resource growth with request volume", "Inspect runtime and garbage-collection metrics", "Find the highest-latency operations and traces"]'::jsonb,
    '["Reduce or rate-limit pathological work", "Correct the leak or hot path", "Temporarily scale the service only when demand is genuine"]'::jsonb,
    '["Confirm resource usage stabilizes", "Confirm latency and error rate return to baseline"]'::jsonb,
    '["Return temporary scaling to its prior level after the cause is corrected"]'::jsonb
),
(
    'queue-lag-v1', 'queue_lag', 'queue',
    'Recover a lagging message pipeline',
    'Distinguish producer overload from consumer slowdown before changing capacity.',
    '["queue_lag", "producer_rate", "consumer_rate"]'::jsonb,
    '["Compare publish and consume rates", "Inspect consumer errors and processing latency", "Check partition and broker health"]'::jsonb,
    '["Repair or restart the failing consumer", "Apply producer backpressure", "Scale consumers when processing is healthy but insufficient"]'::jsonb,
    '["Confirm lag is decreasing", "Confirm end-to-end processing latency recovers"]'::jsonb,
    '["Return temporary consumer capacity to the prior level after backlog recovery"]'::jsonb
),
(
    'service-error-v1', 'service_error', 'service',
    'Investigate a service error-rate increase',
    'Use failing operations, exception templates, and upstream/downstream context to isolate the defect.',
    '["error_rate", "failed_operations"]'::jsonb,
    '["Inspect the first new exception or status-code pattern", "Compare successful and failed traces", "Check recent configuration and dependency changes"]'::jsonb,
    '["Correct or roll back the failing code or configuration", "Add input validation for the observed failure mode"]'::jsonb,
    '["Replay the failing operation", "Confirm error rate returns to baseline"]'::jsonb,
    '["Roll back the remediation if it introduces a new regression"]'::jsonb
),
(
    'workload-spike-v1', 'workload_spike', 'external',
    'Handle an external workload spike',
    'Validate that the workload is legitimate before scaling or applying traffic controls.',
    '["request_rate", "service_saturation"]'::jsonb,
    '["Compare request mix and source with the healthy baseline", "Check whether errors begin only after saturation", "Verify no internal dependency degraded first"]'::jsonb,
    '["Apply rate limits for abusive traffic", "Scale the constrained tier for legitimate demand", "Protect expensive endpoints with quotas or caching"]'::jsonb,
    '["Confirm queueing, latency, and errors recover under the controlled load"]'::jsonb,
    '["Remove temporary traffic controls after demand normalizes"]'::jsonb
)
ON CONFLICT (runbook_key) DO NOTHING;

-- Register the always-available transparent ranker so every live decision has a
-- discoverable model-governance record even before a learned challenger is
-- promoted. Learned models use their own name/version lifecycle.
INSERT INTO model_versions (
    name, version, model_kind, status, configuration, metrics
) VALUES (
    'transparent-root-cause-ranker',
    'heuristic-v1',
    'evidence-weighted-graph-ranker',
    'champion',
    '{"anomaly_weight":0.35,"temporal_weight":0.25,"propagation_weight":0.30,"quality_weight":0.10,"supports_abstention":true}'::jsonb,
    '{"baseline":true,"evaluation_required_for_learned_promotion":true}'::jsonb
)
ON CONFLICT (name, version) DO UPDATE SET
    model_kind = EXCLUDED.model_kind,
    configuration = EXCLUDED.configuration,
    metrics = EXCLUDED.metrics;
