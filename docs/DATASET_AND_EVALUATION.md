# Dataset and Evaluation Protocol

## Testbed

The repository pins OpenTelemetry Demo `3.1.0` at commit
`dedc0178918e260823323b8d95005a8cb924b007`. Core and full non-agentic services
are allowed. `compose.agent.yaml` is intentionally excluded so neither the
dataset nor runtime depends on an LLM.

The upstream project is a workload and fault-injection testbed, not a labeled
machine-learning dataset. The dataset produced by this project consists of the
captured signals, derived feature windows, topology snapshots, and separately
stored ground-truth manifests from controlled runs.

## Root-cause label

The root cause is the smallest controllable entity or intervention whose
reversal restores the affected service-level indicator. A label has four
separate dimensions:

- injector: where the experiment was activated;
- causal entity or edge: what should be repaired;
- failure mechanism: what went wrong; and
- impacted SLI: which externally relevant behavior degraded.

This prevents ambiguous experiments from being forced into a misleading single
service label.

## Single-fault capture procedure

1. Record repository SHA, image versions, host resources, collector settings,
   trace sampling, workload configuration, and seed.
2. Start from reset containers and persistent state.
3. Validate expected telemetry signal coverage.
4. Run a healthy warm-up long enough to establish stable traffic.
5. Activate exactly one known fault and record event time, variant, injector,
   causal entity, mechanism, and expected SLI.
6. Generate traffic that exercises the faulted path.
7. Hold the fault long enough to expose its dynamics.
8. Clear the fault and record a recovery interval.
9. Reset state before the next independent run.

Healthy high-load episodes must be captured alongside fault episodes. Otherwise
the model will learn that traffic volume itself means failure.

### Workload and seed provenance

The executable Docker runner currently leaves the upstream OpenTelemetry Demo
Locust task mix unchanged. Every run it creates therefore records the **actual
workload** as `mixed-commerce`. A scenario's `intended_workload_exercise` (for
example, `repeated-checkout`) is stored separately as experiment metadata; it
describes the path expected to expose the injected fault, not a traffic filter
that the runner did not apply.

`--seed` / `-Seed` is a required capture and dataset-partition seed. It is saved
in the local manifest, `experiment_runs.seed`, the run configuration, and the
environment record. The pinned stock Locust 2.44.4 launcher exposes no supported
seed option, and cooperative request scheduling depends on response timing, so
the manifest explicitly records `upstream_workload_rng_controlled: false`. The
seed must not be presented as making the request sequence deterministic.

`--split` / `-Split` assigns the whole run to `train`, `validation`, `test`, or
`holdout` at registration time. The default `unassigned` value is represented
as a null API split. Assign independent real captures explicitly before export
when a deterministic three-way evaluation is required.

Before API registration or warm-up, the runner restarts the complete selected
Demo Compose model—including `load-generator`—and waits until every configured
service is running and every declared health check is healthy. Only then does it
write `reset_completed_at` and allow registration. This enforces the manifest's
`reset_between_runs: true` policy instead of treating it as documentation only.

## Implemented lineage and export path

The controller contract is deliberately separate from telemetry ingestion:

1. Generate a UUID and register the run at `POST /api/v1/experiments`.
2. Stamp that UUID on traces, metrics, and logs as `experiment.run_id`.
3. Record each injection at `POST /api/v1/experiments/{run_id}/faults` and
   each causal label at `POST /api/v1/experiments/{run_id}/labels`.
4. After recovery, query the run-scoped capture summary and mark the run
   `completed` only when persisted spans, metric points, and log records are all
   present, an incident has candidates, and the injected causal entity or its
   explicitly declared dependency edge appears within the configured rank
   cutoff (top three by default). A rejection is
   terminal `failed`, `capture_valid=false`, and `training_eligible=false`.
5. Export candidate rows with `python -m rca.export_dataset --database-url
   postgresql://...`; the default artifact name includes the immutable snapshot
   version. For real-only datasets, repeat `--include-run UUID` for the captured
   runs and use exact `--source-repository`, `--source-version`, and
   `--source-sha` filters.
6. Train with `python -m rca.train
   data/exports/candidate-ranking-<snapshot-version>.jsonl`.

For a smoke-scale capture set, pass `--feature-profile compact` explicitly.
That profile projects each candidate onto `baseline_inverse_rank`,
`baseline_probability`, `baseline_score`, `candidate_is_dependency_edge`, and
`candidate_is_service`. The default `full` profile retains the richer evidence
dimensions and should be used only when enough independent incidents exist to
estimate their effects without memorizing individual runs. The selected profile
and exact feature schema are stored in both the evaluation report and model
registry configuration.

The exporter reads completed labeled runs, candidate rankings, and their stored
evidence. Ground-truth fields become targets and reporting metadata only; they
are never copied into the `features` object. The exporter fails if an
incident's candidate generator did not include the true entity, because turning
every available candidate into a negative example would silently corrupt the
training set. It writes an atomic JSONL artifact, SHA-256 metadata sidecar, and
`dataset_snapshots` registry row. Run-level splitting remains the responsibility
of `rca.train`, which asserts disjoint run IDs across partitions.
The exporter freezes its complete run allowlist and source selector into both
the metadata sidecar and snapshot statistics. An explicitly included UUID that
is absent, outside the requested status scope, unlabeled, or otherwise
ineligible aborts the export rather than silently shrinking the requested
dataset.

The runner records and synchronizes `fault_started_at` immediately before
changing the fault flag, then records and synchronizes `fault_ended_at`
immediately before beginning restoration. flagd watches the mounted JSON file
and reloads it live; the runner deliberately does not restart flagd at either
boundary. This keeps the intervention inside its declared label interval and
prevents a flag-service outage from contaminating the diagnosis.

## Current real-data snapshot

The verified October 4, 2026 snapshot contains four accepted full-profile Demo
runs: two train runs, one validation run, and one test run. Its 156,942 spans,
127,038 metric points, and 50,739 log records produced 40 candidate rows. The
frozen dataset checksum is
`39270c57baca1bd9182f29a32b367cb4d4fd80e8868749887d187a94fdb764db`.

The promoted compact model records perfect Hit@3 on its single validation and
single test examples. A later, separately frozen independent run that was
unseen during development also ranked the causal `checkout->payment` edge
first, with the holdout kept outside fitting, validation, and test selection.
This verifies the implemented end-to-end pipeline, not broad model
generalization: every evaluation partition still has only one incident. The
accepted run ledger, artifacts, checksums, exact reproduction commands, and completed holdout evidence are in
[`REAL_DATA_RESULTS.md`](REAL_DATA_RESULTS.md).

## Robustness suites

After single-fault training, reserve these conditions for evaluation:

- unseen severities and workloads;
- leave-one-fault-class-out;
- leave-one-service-out with anonymized identity features;
- a later OpenTelemetry Demo topology/version;
- missing logs, metrics, or traces;
- reduced trace sampling;
- concurrent independent faults; and
- generic delay, packet loss, process restart, CPU, and memory chaos.

## Leakage controls

- Split complete experiment runs, never individual or overlapping windows.
- Keep all windows from one seed/run in one split.
- Fit normalization and healthy baselines on training data only.
- Use causal windows; online predictions cannot read future samples.
- Exclude flags, scheduler events, run IDs, injection timestamps, and explicit
  "fault activated" messages from features.
- Template or mask exceptions that directly name an injected scenario, then
  report a logs-disabled ablation.
- Keep sampling policy constant or provide sampling/missingness as explicit
  features.
- Reset leaked memory, caches, queues, and database locks between runs.
- Report a service-prior-only baseline to expose candidate-frequency leakage.

## Model comparison

1. Static thresholds.
2. Rolling median/MAD and EWMA.
3. Isolation Forest for detection, not causal ranking.
4. Transparent onset/topology heuristic.
5. Trained tabular ranker.
6. Anomaly score plus dependency propagation.
7. Temporal-only Transformer.
8. Graph-only attention network.
9. Complete graph-temporal challenger.

Each learned model is evaluated with graph, modality, service-identity, and log
leakage ablations.

## Metrics

Detection:

- event-level precision, recall, F1/F2, and PR-AUC;
- false alerts per healthy hour; and
- p50/p95 time to detect.

Root-cause ranking:

- Hit@1, Hit@3, Hit@5, MRR, and nDCG@k;
- MAP@k for multi-root incidents;
- macro results by service and fault class; and
- random, prior-only, and strongest non-graph baselines.

Trust and operation:

- Brier score, expected calibration error, and risk-coverage curves;
- degradation under each robustness condition;
- p95 inference latency, throughput, CPU, memory, and storage growth; and
- explanation occlusion faithfulness and compatible-runbook accuracy.

Confidence intervals are bootstrapped by complete run, not by correlated window.
