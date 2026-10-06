# RCA engine integration contract

The analytical functions do not depend on SQL. The live worker expects the
following PostgreSQL contract from the application migrations.

## Raw inputs

- `spans(trace_id, span_id, parent_span_id, service_name, operation_name,
  span_kind, start_time, end_time, duration_ms, status_code, attributes,
  resource_attributes, run_id, observed_at)`
- `metric_points(service_name, metric_name, metric_type, time_unix, value,
  count, sum, min, max, attributes, resource_attributes, run_id, observed_at)`
- `log_records(service_name, time_unix, observed_time, severity_number,
  severity_text, body, trace_id, span_id, attributes, resource_attributes,
  run_id)`

Times are `timestamptz`, `run_id` is nullable UUID, JSON fields are `jsonb`, and
numeric telemetry values accept double precision. The worker only processes
closed time buckets.

The synthetic sender targets:

```text
POST /api/v1/events/batch
Content-Type: application/json

{"run_id":"<uuid>","events":[{"signal":"span|metric|log", ...}]}
```

## Materialized outputs

- `dependency_edges(bucket_start, source_service, target_service, protocol,
  request_count, error_count, error_rate, latency_ms_p50, latency_ms_p95,
  latency_ms_p99, run_id, attributes)`
- `feature_buckets(bucket_start, entity_type, entity_id, run_id, request_count,
  error_count, error_rate, latency_ms_p50, latency_ms_p95, latency_ms_p99,
  cpu_percent, memory_bytes, log_error_count, anomaly_score, onset_score,
  features, missingness)`
- `incidents(id, run_id, status, started_at, detected_at, ended_at, severity, summary,
  trigger_entity, max_anomaly, affected_entities, metadata)`
- `candidate_rankings(id, incident_id, rank, entity_type, entity_id, score,
  probability, fault_class, confidence, evidence_summary, online_features,
  model_version,
  created_at)`
- `evidence_items(incident_id, candidate_id, evidence_type, entity_id,
  occurred_at, signal_name, value, baseline, contribution, details)`

`incidents.id` must be UUID primary key. `candidate_rankings.id` must be a
generated integer key. `candidate_id` in `evidence_items` references it. JSON
fields are `jsonb`; `affected_entities` is `text[]`. `value` and `baseline`
are nullable `double precision` values in the canonical schema; categorical
context belongs in the evidence `details` JSON object.

The worker makes feature and edge writes idempotent with scoped delete/insert,
uses deterministic UUIDv5 incident IDs, and replaces rankings/evidence for an
updated incident in one transaction. It acquires a PostgreSQL transaction-level
advisory lock so only one replica processes a cycle.

## Commands

From the `499` directory:

```powershell
python -m rca.synthetic --scenario database_lock --output synthetic-run.json
python -m rca.synthetic --scenario database_lock --post-url http://localhost:8000/api/v1/events/batch
python -m rca.worker --database-url postgresql://rca:rca-local-password@localhost:55432/rca --once
python -m rca.worker --database-url postgresql://rca:rca-local-password@localhost:55432/rca --check
python -m rca.export_dataset --database-url postgresql://rca:rca-local-password@localhost:55432/rca --include-run <real-run-uuid> --source-repository https://github.com/open-telemetry/opentelemetry-demo.git --source-version 3.1.0 --source-sha dedc0178918e260823323b8d95005a8cb924b007
python -m rca.train data/exports/candidate-ranking-<snapshot-version>.jsonl --output-dir artifacts/champion --portable --database-url postgresql://rca:rca-local-password@localhost:55432/rca
python -m rca.promote --database-url postgresql://rca:rca-local-password@localhost:55432/rca --model-name candidate-ranker --model-version <model-version> --output-dir artifacts/champion
python -m unittest discover -s tests/rca -v
```

The exporter includes only completed runs with explicit ground-truth labels by
default. It derives model features from the online ranking/evidence records,
writes an immutable checksum-bearing metadata sidecar with frozen sorted run
provenance, registers `dataset_snapshots`, and
fails closed when the true entity never appeared in an incident's candidate
set. Use `--allow-unmatched` only for candidate-recall diagnosis, not champion
training. Repeatable `--include-run` values and the optional exact source
repository/version/SHA predicates are applied to every read in the same
repeatable-read transaction and are frozen into snapshot statistics.

Docker build context is the `rca` directory (matching Compose):

```powershell
docker build -t rca-worker rca
```
