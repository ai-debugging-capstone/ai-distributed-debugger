# Product Requirements

## Required outcome

Given an OpenTelemetry-instrumented distributed system, the debugger must detect
a sustained incident, reconstruct the active dependency context, rank plausible
causal entities, explain the ranking with concrete telemetry, and display a
reviewed debugging runbook in Grafana.

## Functional requirements

### Telemetry

- Accept OTLP traces, metrics, and logs through the gateway.
- Preserve trace/span correlation, resource identity, event time, attributes,
  status, and explicit missingness.
- Tolerate replay and collector retry without duplicating records.
- Quarantine invalid records with a human-readable reason.

### System understanding

- Derive directed dependencies from observed traces and span links.
- Retain edge volume, error, duration, protocol, and freshness.
- Model service and infrastructure nodes, including PostgreSQL, Kafka, Valkey,
  hosts, external load, and `no_fault`.

### Incident analysis

- Detect sustained deviation from a healthy, traffic-aware baseline.
- Group connected symptoms into an incident window.
- Preserve anomaly onset order and distinguish likely origin from downstream
  effects.

### Root-cause analysis

- Integrate the existing free DoWhy GCM engine for supported latency incidents.
- Fit its built-in causal mechanisms only to healthy pre-incident observations;
  do not use experiment labels or future samples during analysis.
- Return an ordered candidate list with engine/version, score meaning, failure
  class, evidence, and explicit limitations. DoWhy attribution is signed and its
  probability must be null; no calibration is claimed.
- Record the requested provider, actual execution status, reason, target, and
  baseline sample count on the incident.
- Abstain when evidence is insufficient.
- Attach supporting and contradicting signal evidence.
- Continue with an identified heuristic fallback for unsupported incidents,
  cyclic/incomplete graphs, insufficient history, or an unavailable provider.
- Preserve optional heuristic/tabular comparison modes without requiring custom
  Transformer development, a paid service, or an LLM.

### Debugging assistance

- Select a versioned runbook compatible with the predicted mechanism and
  evidence.
- Show diagnostic, remediation, verification, and rollback steps.
- Never execute remediation automatically in this release.

### User experience

- Use Grafana as the only end-user interface.
- Show incident summary, ranking, temporal propagation, live dependency graph,
  evidence, signal health, runbook, uncertainty, actual engine, and fallback
  status/reason. Display unavailable probability as n/a, never zero confidence.
- Capture whether the developer accepted the diagnosis and the eventual cause.

## Initial acceptance criteria

These are verification targets. Historical real-data results and controlled
integration checks do not establish that every target has been met.

- The complete local stack starts with one documented command.
- Synthetic smoke data produces an incident, candidates, evidence, graph, and
  runbook without manual database edits.
- A pinned OpenTelemetry Demo can route real OTLP data through the same path.
- A controlled latency experiment executes the actual DoWhy library, preserves
  raw signed attribution, and identifies the injected component; fallback-only
  execution must fail that integration check.
- Healthy baseline samples precede incident onset, and other runs, labels,
  future buckets, missing spans, and unsupported graphs cannot contaminate them.
- A persistent injected fault is ranked within 60 seconds after detection.
- Held-out known-fault experiments achieve Hit@3 of at least 0.85 before the
  learned model is called production-ready.
- Healthy test traffic produces fewer than one false incident per hour.
- For any future probability-producing model, calibrated expected calibration
  error is at most 0.10 on a sufficiently large independent evaluation set.
- Report all attempted fault diagnoses and healthy controls; capture-time top-3
  acceptance alone is not an unbiased accuracy benchmark.
- Every prediction is tied to a model version and every dataset to source SHA,
  environment, seed, and manifest.
- Unit, database, API, worker, replay, and end-to-end tests pass.

## Explicit non-goals

- Generating fixes with an LLM.
- Autonomous code changes or remediation.
- Claiming that correlation or attention is causal proof.
- Supporting arbitrary multi-tenant production scale in the capstone release.
- Replacing full observability tools for unrestricted ad-hoc telemetry search.
