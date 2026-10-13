# TraceLens RCA

TraceLens RCA is a dependency-aware debugging system for distributed applications.
It consumes OpenTelemetry traces, metrics, and logs, detects sustained incidents,
reconstructs the dependency path active during each incident, ranks likely root
causes, and presents the evidence and a reviewed debugging runbook in Grafana.

PostgreSQL is the durable store behind the system. It is deliberately not the
center of the product: the product is the analysis loop that turns telemetry into
an explainable debugging decision.

## What the system does

```text
Instrumented services
        |
        | OTLP traces, metrics, logs
        v
OpenTelemetry gateway
        |
        v
Telemetry API ----> validation quarantine
        |
        v
Normalized event-time telemetry in PostgreSQL
        |
        v
Feature windows + observed dependency graph
        |
        v
Persistent incident detector
        |
        v
Candidate generation and root-cause ranking
        |
        +---- deterministic evidence ranker (always available)
        +---- trained tabular champion
        +---- temporal Transformer + graph-attention challenger
        |
        v
Evidence, uncertainty, runbook, and developer feedback
        |
        v
Grafana debugging workspace
```

For one concrete example, if checkout requests become slow because the payment
service is unreachable, TraceLens does not merely report a latency alert. It:

1. detects that the checkout symptoms persist beyond normal variation;
2. rebuilds the active path from frontend to checkout to payment;
3. compares anomaly onset times along that path;
4. ranks the `checkout -> payment` dependency edge and payment service above
   downstream victims;
5. shows the error, latency, trace, topology, and missing-signal observations that
   produced the decision; and
6. attaches a reviewed dependency-unreachable runbook with diagnostic,
   remediation, verification, and rollback steps.

If the evidence is weak or contradictory, the result is **inconclusive**. The
system never presents an attention weight or simple correlation as causal proof,
and it never performs remediation automatically.

The local detector requires two consecutive closed 10-second abnormal buckets
by default. That filters one-off spikes without losing short bursts from
low-traffic dependencies; `RCA_MIN_INCIDENT_BUCKETS` remains configurable.

## Product surfaces

- **Grafana:** the developer-facing debugging workspace at
  `http://localhost:3000`.
- **Incident API:** machine-readable incidents, candidates, evidence, graph, and
  feedback at `http://localhost:8000`.
- **OTLP gateway:** OTLP/gRPC on port `4317` and OTLP/HTTP on port `4318`.
- **PostgreSQL:** the local persistence and analytical read-model store on host
  port `55432` by default (the containers use PostgreSQL's normal internal port
  `5432`).

The default local credentials are intentionally development-only and are listed
in `.env.example`.

## Quick start

### Prerequisites

- Docker Desktop with Docker Compose v2
- at least 8 GB of free memory for the product plus the complete OpenTelemetry
  Demo; the product stack alone needs substantially less
- PowerShell 7 on Windows, or Bash and `make` on Linux/macOS

### 1. Start TraceLens

PowerShell:

```powershell
Copy-Item .env.example .env
docker compose config --quiet
docker compose up --build -d
docker compose ps
```

Bash:

```bash
cp .env.example .env
make up
make ps
```

Wait until `rca-db`, `rca-api`, `rca-worker`, `otel-gateway`, and `grafana` are
healthy. Then open Grafana and sign in with `admin` / `admin` unless you changed
the values in `.env`.

### 2. Run the self-contained smoke experiment

PowerShell:

```powershell
.\scripts\run-synthetic-demo.ps1
```

Bash:

```bash
./scripts/run-synthetic-demo.sh
```

The script sends a healthy baseline followed by a labeled failure episode. The
worker should open an incident and persist ranked candidates, evidence, a graph,
and an applicable runbook. This is the fastest way to prove the complete product
path before running the larger microservice demo.

### 3. Use the pinned OpenTelemetry Demo testbed

The repository includes the upstream source at the exact commit used by the
experiment manifest. To fetch or verify it again and start the reduced profile:

```powershell
.\scripts\fetch-demo.ps1
.\scripts\start-demo.ps1 -Profile core
```

For the full demo:

```powershell
.\scripts\start-demo.ps1 -Profile full
```

On Bash systems, use the corresponding `fetch-demo.sh` and `start-demo.sh`
scripts. The integration deliberately excludes the demo's generative-AI agent
compose file and leaves raw-PII emission disabled.

To stop the demo without deleting TraceLens data:

```powershell
.\scripts\stop-demo.ps1
```

### 4. Capture a labeled fault experiment

With the demo running, execute a reversible feature-flag fault. The runner gives
the warm-up, fault, and recovery telemetry one experiment UUID, records the
exact upstream commit and flag interval through the API, and restores both the
flag and collector state even when the run is interrupted:

```powershell
.\scripts\run-demo-experiment.ps1 `
  -Scenario paymentFailure -Variant "50%" -Seed 1101 -Split train
```

For a quick integration check, shorten the three phases with
`-WarmupSeconds`, `-DurationSeconds`, and `-RecoverySeconds`. Use longer defaults
for a useful training capture. Before a run can become `completed`, the runner
checks PostgreSQL-backed capture evidence through the API: spans, metrics, and
logs must all exist; the worker must have produced an incident and candidates;
and the injected causal service or explicitly declared dependency edge must
rank in the top three. Override only the
ranking cutoff with `-AcceptanceTopK`; a failed check terminates the run as
`failed` with `training_eligible=false` and preserves the rejection reasons in
the experiment manifest. Set `-Split` to `train`, `validation`, `test`, or
`holdout` before capture; its default `unassigned` value leaves the API field
unset rather than inventing a post-hoc partition.

The mounted flag file is applied by flagd's live file watcher. The runner does
not restart flagd at fault boundaries, avoiding a second outage that would
contaminate the causal ranking.

### 5. Export a leakage-safe dataset and train a baseline

Only completed runs with explicit labels are exported. Candidate features come
from the online diagnosis that existed before the label was joined; runs remain
whole when assigned to train, validation, or test splits.

```powershell
python -m rca.export_dataset `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --include-run <completed-real-run-uuid-1> `
  --include-run <completed-real-run-uuid-2> `
  --source-repository https://github.com/open-telemetry/opentelemetry-demo.git `
  --source-version 3.1.0 `
  --source-sha dedc0178918e260823323b8d95005a8cb924b007 `
  --snapshot-name otel-demo-candidate-ranking `
  --snapshot-version <immutable-snapshot-version> `
  --output data/exports/otel-demo-candidates-<immutable-snapshot-version>.jsonl

python -m rca.train data/exports/otel-demo-candidates-<immutable-snapshot-version>.jsonl `
  --output-dir artifacts/champion `
  --portable `
  --feature-profile compact `
  --seed <training-seed> `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca

python -m rca.promote `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --model-name candidate-ranker `
  --model-version <model-version-from-training> `
  --output-dir artifacts/champion

docker compose up -d --build rca-worker
```

The portable option proves the full training/evaluation contract without an
optional ML installation. `--feature-profile compact` deliberately limits a
smoke-scale fit to five stable candidate-level inputs; use the higher-dimensional
`full` profile only after collecting substantially more independent incidents.
Training writes an immutable content-addressed model
and—with the database option—registers it as a **challenger** linked to the exact
dataset snapshot; it never changes the worker's stable alias. The separate
promotion command validates the registered checksum and challenger lifecycle,
commits the registry promotion, then atomically publishes
`artifacts/champion/model.json`. A publish failure triggers a best-effort
registry compensation while promotion is serialized by a database advisory
lock. When scikit-learn is available, omit
`--portable` to train the optional scikit-learn tabular implementation; its runtime image
must also include the optional ML dependencies before a pickle is selected.
The numeric temporal Transformer and edge-aware graph-attention forward
architecture lives in `rca/models.py`, but it is research scaffolding rather
than the deployed model path: the current dataset exporter, trainer,
serialization, and worker integration are tabular. Neither path uses an LLM.
Promotion is manual after evaluation review; registry lifecycle and checksum
validation intentionally do not pretend to be an automatic quality gate.

The worker verifies the selected artifact, records a checksum-derived model
version in every decision, and falls back to `heuristic-v1` if loading or
inference fails. Only load pickle artifacts produced and reviewed inside this
project. Dataset and model files are immutable by checksum, so use a new output
name/snapshot version when exporting a later experiment batch.

`--include-run` is a repeatable, fail-closed allowlist: every requested UUID
must be an eligible positively labeled run. The repository, version, and SHA
filters are exact matches and apply to every exporter query. The complete
selector is frozen into the metadata sidecar and PostgreSQL snapshot record, so
a later training result can be traced to the precise set of real captures.

## Verified real-data snapshot

The checked-in real-data snapshot was captured from the full OpenTelemetry Demo
`3.1.0` deployment at commit
`dedc0178918e260823323b8d95005a8cb924b007`. Four independently reset and
accepted runs contain 156,942 spans, 127,038 metric points, and 50,739 log
records. Their immutable export contains 40 candidate rows and has SHA-256
`39270c57baca1bd9182f29a32b367cb4d4fd80e8868749887d187a94fdb764db`.

The promoted compact portable model has SHA-256
`1df99581e825cf1a03f7162ed4bd4143a54271931e40e032b415a7e7ee283533`.
It achieved Hit@3 of 1.0 on one validation run and one test run. Those are
smoke-scale checks, not statistically persuasive evidence of generalization or
production readiness. A separately frozen, independent and previously unseen
`paymentUnreachable` run then passed the live top-3 gate at rank 1 while the
checksum-identified champion was active. The five runs represented in the
audit export produced 199,218 spans, 158,966 metric points, and 64,240 logs;
the export contains 50 candidate rows. The holdout remained outside fitting,
validation, and test selection.

See [`docs/REAL_DATA_RESULTS.md`](docs/REAL_DATA_RESULTS.md) for the accepted run
ledger, exact artifact paths and checksums, feature schema, reproduction
commands, and the completed holdout evidence.

## Debugging workflow

The main Grafana dashboard is designed to answer the questions in the order an
engineer actually asks them:

1. **Is this a real incident?** Incident state, duration, severity, and affected
   services are shown alongside telemetry freshness.
2. **Where did it begin?** The onset timeline compares anomalies across services
   and edges.
3. **What is most likely responsible?** A ranked candidate table includes entity
   type, failure mechanism, score, calibrated confidence, model version, and
   abstention state.
4. **Why is it ranked there?** Evidence panels show observed values, baselines,
   deviations, traces, affected paths, and contradicting signals.
5. **What should I inspect next?** The selected runbook provides bounded,
   reviewed steps and explicit verification and rollback instructions.
6. **Was the diagnosis useful?** Developer feedback records acceptance and the
   eventual cause for later evaluation; it does not silently retrain a model.

Equivalent API calls are available for automation:

```text
GET  /health/live
GET  /health/ready
POST /api/v1/events/batch
POST /otlp/v1/traces
POST /otlp/v1/metrics
POST /otlp/v1/logs
POST /api/v1/experiments
GET  /api/v1/experiments/{run_id}
GET  /api/v1/experiments/{run_id}/capture-summary
PATCH /api/v1/experiments/{run_id}
POST /api/v1/experiments/{run_id}/faults
POST /api/v1/experiments/{run_id}/labels
GET  /api/v1/incidents
GET  /api/v1/incidents/{incident_id}
GET  /api/v1/incidents/{incident_id}/candidates
GET  /api/v1/incidents/{incident_id}/evidence
GET  /api/v1/incidents/{incident_id}/recommendations
GET  /api/v1/incidents/{incident_id}/graph
POST /api/v1/incidents/{incident_id}/feedback
```

The canonical OTLP endpoints are also mounted at `/v1/traces`, `/v1/metrics`, and
`/v1/logs` for direct collector compatibility.

## Analysis design

### Incident detection

Telemetry is aligned into event-time buckets. Robust baselines use the median and
median absolute deviation, with explicit cold-start and missingness indicators.
An incident opens only after anomaly conditions persist for the configured number
of buckets. Nearby symptoms that are connected in the observed dependency graph
are grouped into one incident instead of producing an alert storm.

### Dependency understanding

The graph is derived from real span parent/child relationships and peer/resource
attributes. Nodes include services, instances, PostgreSQL, Kafka, Valkey, hosts,
external load, and a `no_fault` option. Directed edges retain request volume,
errors, latency, protocol, freshness, and the trace paths on which they appeared.

### Root-cause ranking

The baseline ranker combines:

- temporal precedence;
- robust anomaly magnitude;
- propagation-path coverage;
- graph direction and neighborhood consistency;
- service and edge error/latency changes;
- resource saturation and queue signals; and
- telemetry quality and missingness.

This deterministic ranker is the safe fallback and the explanation anchor. The
first learned champion is a compact tabular classifier/ranker trained from fault
injection runs. The research challenger is a numeric temporal Transformer for
within-entity history plus edge-aware graph attention for cross-entity context.
It is not a language model, does not consume source code or prompts, and must beat
the champion on held-out experiment runs before promotion.

### Evaluation without leakage

The OpenTelemetry Demo is a reproducible workload and fault generator, not a
pre-labeled static dataset. Each run records the source commit, configuration,
random seed, traffic profile, fault command, injection interval, and ground-truth
entity/mechanism. Ground truth is isolated from online features.

Splits are performed by complete experiment run—not by individual telemetry row—
so adjacent windows from the same fault cannot leak across training and test
sets. Reports include Hit@1, Hit@3, mean reciprocal rank, false incidents per
healthy hour, detection delay, calibration error, abstention coverage, robustness
under missing signals, and per-fault-family performance.

The complete scenario catalog is in
[`experiments/otel-demo-v3.1.0.yaml`](experiments/otel-demo-v3.1.0.yaml).

## Repository map

```text
app/                    Telemetry and incident HTTP API
db/init/                PostgreSQL schema, views, and seeded runbooks
docs/                   Architecture, requirements, and evaluation contract
experiments/            Versioned OpenTelemetry Demo experiment manifest
infra/grafana/          Provisioned datasource and RCA dashboard
infra/otel-collector/   OTLP gateway configurations
infra/demo/             Safe upstream-demo compose override
rca/                    Detection, graph, ranking, training, and worker code
scripts/                Setup, smoke-test, demo, training, and evaluation tools
tests/                  Automated behavior and integration tests
vendor/                 Pinned OpenTelemetry Demo source
```

Important design documents:

- [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
- [`docs/PRODUCT_REQUIREMENTS.md`](docs/PRODUCT_REQUIREMENTS.md)
- [`docs/DATASET_AND_EVALUATION.md`](docs/DATASET_AND_EVALUATION.md)

## Automated tests

See [docs/TESTING.md](docs/TESTING.md) for Windows setup, unit/component commands,
and real PostgreSQL integration tests using disposable databases. From the
repository root, the offline suite is:

```powershell
.\.venv\Scripts\python.exe -m pytest -c pytest.ini -m "not postgres"
```

Run both offline and PostgreSQL tests before each subsequent milestone. Database
tests require explicit opt-in; a default run that skips them does not establish
real persistence. These tests verify integration, not root-cause accuracy.

## Operations

Follow logs:

```powershell
docker compose logs -f --tail=200 rca-api rca-worker otel-gateway
```

Check ingest and incident state directly:

```powershell
Invoke-RestMethod http://localhost:8000/health/ready
Invoke-RestMethod http://localhost:8000/api/v1/incidents
```

Stop containers while retaining database and Grafana volumes:

```powershell
docker compose down
```

Deleting volumes permanently removes local telemetry, incidents, feedback, and
dashboard state and is therefore intentionally not part of the normal commands.

## Safety and scope

- TraceLens is advisory: developers choose and execute remediation.
- Runbooks are versioned reviewed records, not generated instructions.
- Model confidence is calibrated and the system may abstain.
- The target demo database and TraceLens database are separate failure domains.
- Collector retry plus idempotent storage protects against transient delivery
  failures.
- This capstone release is a rigorous local/research system, not a claim of
  arbitrary multi-tenant production scale.
