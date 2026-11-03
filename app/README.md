# Telemetry and incident API

This service is the live input and query boundary for the dependency-aware
debugging platform. It accepts OTLP/HTTP protobuf, normalizes all three
OpenTelemetry signals, performs idempotent PostgreSQL writes, and exposes the
RCA engine's incident results to Grafana or another client.

It does **not** detect incidents or run a model. Those jobs read the normalized
tables and publish their results to the derived tables queried by this API.

## Run

```powershell
$env:DATABASE_URL = 'postgresql://rca:rca-local-password@localhost:55432/rca'
python -m uvicorn telemetry_api.main:app --host 0.0.0.0 --port 8000
```

From this directory, install developer dependencies with
`pip install -r requirements-dev.txt` and run `pytest`.

## Endpoints

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/health/live` | Process liveness; never claims that PostgreSQL is ready |
| `GET` | `/health/ready` | PostgreSQL connection and required raw-table readiness |
| `POST` | `/otlp/v1/traces` | OTLP/HTTP protobuf trace export |
| `POST` | `/otlp/v1/metrics` | OTLP/HTTP protobuf metric export |
| `POST` | `/otlp/v1/logs` | OTLP/HTTP protobuf log export |
| `POST` | `/api/v1/events/batch` | Validated JSON telemetry for controlled experiments |
| `POST` | `/api/v1/experiments` | Register or enrich a reproducible experiment run |
| `GET` / `PATCH` | `/api/v1/experiments/{run_id}` | Read or advance run state/timestamps |
| `POST` | `/api/v1/experiments/{run_id}/faults` | Record the exact injected fault interval and cause |
| `POST` | `/api/v1/experiments/{run_id}/labels` | Upsert isolated ground-truth labels for offline evaluation |
| `GET` | `/api/v1/incidents` | Paginated/filterable incident list |
| `GET` | `/api/v1/incidents/{id}` | Incident detail |
| `GET` | `/api/v1/incidents/{id}/candidates` | Ranked causal candidates |
| `GET` | `/api/v1/incidents/{id}/evidence` | Evidence supporting or opposing candidates |
| `GET` | `/api/v1/incidents/{id}/recommendations` | Reviewed runbooks selected for ranked candidates |
| `GET` | `/api/v1/incidents/{id}/graph` | Incident dependency graph |
| `POST` | `/api/v1/incidents/{id}/feedback` | Developer verification of a prediction |

Canonical collector aliases `/v1/traces`, `/v1/metrics`, and `/v1/logs` are
also accepted. OTLP bodies may use `Content-Encoding: gzip`. The maximum
compressed *and* decompressed body size defaults to 16 MiB.

Interactive OpenAPI documentation is served at `/docs`.

## OTLP entity identity

Explicit `service.name` remains authoritative. Infrastructure receivers do not
always emit it, so the normalizer deterministically infers the measured entity
from stable OpenTelemetry semantic-convention attributes instead of merging
all such measurements into `unknown_service`. Resource attributes take
precedence in this order: `container.name`, Kubernetes workload identity,
`faas.name`, `server.address`, PostgreSQL database identity, Kafka cluster
identity, database/messaging system identity, and finally `host.name`.
Container names have a Docker `/` prefix removed so they correlate with the
same service names found in application traces. If a metrics bridge puts these
identity fields on the data point rather than the resource, the same fields
(plus `service.name` and `process.executable.name`) are used as a lower-priority
fallback. The original resource and point attributes are retained for audit.

## Experiment controller contract

Generate one UUID before a controlled run, register it with
`POST /api/v1/experiments`, and stamp the same value into all three OTLP signals
as the resource attribute `experiment.run_id` (the aliases
`telemetry.run_id` and `run.id` are also accepted). Register the fault interval
and one or more labels through the child endpoints, then `PATCH` the run to
`completed` only after the recovery window closes. This makes traces, metrics,
logs, features, incidents, and ground truth joinable without putting the label
inside online model features.

The experiment endpoints accept the database status vocabulary: `created`,
`warming`, `running`, `recovering`, `completed`, or `failed`. A repeated POST
with the same run UUID is idempotent and enriches any placeholder that was
created when telemetry arrived before controller metadata.

## Synthetic runner contract

The batch body contains an optional run-level UUID and one or more events. An
event-level `run_id` overrides the batch value.

```json
{
  "run_id": "970c8464-87bf-42d2-a25f-cff3b01f1001",
  "events": [
    {
      "signal": "span",
      "service_name": "checkoutservice",
      "trace_id": "00112233445566778899aabbccddeeff",
      "span_id": "0011223344556677",
      "parent_span_id": null,
      "operation_name": "POST /checkout",
      "span_kind": "SERVER",
      "start_time": "2026-09-30T16:00:00Z",
      "end_time": "2026-09-30T16:00:00.125Z",
      "status_code": "OK",
      "attributes": {"http.response.status_code": 200},
      "resource_attributes": {"deployment.environment.name": "experiment"}
    },
    {
      "signal": "metric",
      "service_name": "checkoutservice",
      "metric_name": "http.server.request.duration",
      "metric_type": "histogram",
      "time_unix": "2026-09-30T16:00:10Z",
      "count": 25,
      "sum": 3.15,
      "min": 0.04,
      "max": 0.62,
      "attributes": {"http.route": "/checkout"},
      "resource_attributes": {}
    },
    {
      "signal": "log",
      "service_name": "checkoutservice",
      "time_unix": "2026-09-30T16:00:10Z",
      "severity_number": 17,
      "severity_text": "ERROR",
      "body": "payment dependency timed out",
      "trace_id": "00112233445566778899aabbccddeeff",
      "span_id": "0011223344556677",
      "attributes": {"error.type": "DeadlineExceeded"},
      "resource_attributes": {}
    }
  ]
}
```

The response distinguishes accepted, newly inserted, duplicate, and rejected
records. Replaying an identical batch is safe.

## Exact database contract

Migrations are intentionally not performed by the API. The deployment must
provide these columns and constraints:

### `spans`

`id bigserial`, `trace_id text`, `span_id text`, `parent_span_id text null`,
`service_name text`, `service_namespace text null`,
`service_instance_id text null`, `operation_name text`, `span_kind text`,
`start_time timestamptz`, `end_time timestamptz`,
`duration_ms double precision`, `status_code text`, `status_message text null`,
`attributes jsonb`, `resource_attributes jsonb`, `scope_name text null`,
`scope_version text null`, `run_id uuid null`, `observed_at timestamptz`,
`raw jsonb`, and `UNIQUE (trace_id, span_id)`.

### `metric_points`

`id bigserial`, `idempotency_key text UNIQUE`, `service_name text`,
`metric_name text`, `metric_type text`, `time_unix timestamptz`,
`start_time timestamptz null`, `value double precision null`,
`count bigint null`, `sum/min/max double precision null`, `attributes jsonb`,
`resource_attributes jsonb`, `exemplars jsonb`, `run_id uuid null`,
`observed_at timestamptz`, and `raw jsonb`.

### `log_records`

`id bigserial`, `idempotency_key text UNIQUE`, `service_name text`,
`time_unix timestamptz`, `observed_time timestamptz`, `severity_number int`,
`severity_text text null`, `body text`, `trace_id text null`,
`span_id text null`, `attributes jsonb`, `resource_attributes jsonb`,
`run_id uuid null`, `received_at timestamptz DEFAULT now()`, and `raw jsonb`.

### `rejected_records`

`id bigserial`, `signal_type text`, `reason text`, `payload jsonb`, `headers
jsonb DEFAULT '{}'`, and `received_at timestamptz`.

The query API also reads `incidents`, `candidate_rankings`, `evidence_items`,
and `dependency_edges`. These retain their full RCA-engine records in API
responses rather than silently throwing away newly introduced fields.
`developer_feedback` must contain at least `incident_id`, nullable
`candidate_id`, `verdict`, nullable `comment`, nullable `user_name`, `metadata
jsonb`, and a database-generated identifier/timestamp. Verdicts are persisted
as `correct`, `partially_correct`, `incorrect`, or `unknown`; the request also
accepts the friendly aliases `confirmed`, `rejected`, and `inconclusive`.

## Failure behavior

- Database failures return `503` and `Retry-After: 5`; OTLP exporters can retry.
- Malformed protobuf returns `400` and is recorded in `rejected_records` when
  PostgreSQL is available.
- A malformed individual OTLP record is quarantined while valid siblings are
  committed, and the protobuf response carries OTLP `partial_success` counts.
- Duplicate spans, metric points, and logs are successful no-ops.
- `/health/ready` stays false until PostgreSQL and all four raw tables exist.
