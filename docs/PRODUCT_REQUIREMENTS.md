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

- Return an ordered candidate list with score, calibrated probability, model
  version, failure class, and confidence.
- Abstain when evidence is insufficient.
- Attach supporting and contradicting signal evidence.
- Continue with a transparent fallback if a learned artifact is unavailable.

### Debugging assistance

- Select a versioned runbook compatible with the predicted mechanism and
  evidence.
- Show diagnostic, remediation, verification, and rollback steps.
- Never execute remediation automatically in this release.

### User experience

- Use Grafana as the only end-user interface.
- Show incident summary, ranking, temporal propagation, live dependency graph,
  evidence, signal health, runbook, uncertainty, and model status.
- Capture whether the developer accepted the diagnosis and the eventual cause.

## Initial acceptance criteria

- The complete local stack starts with one documented command.
- Synthetic smoke data produces an incident, candidates, evidence, graph, and
  runbook without manual database edits.
- A pinned OpenTelemetry Demo can route real OTLP data through the same path.
- A persistent injected fault is ranked within 60 seconds after detection.
- Held-out known-fault experiments achieve Hit@3 of at least 0.85 before the
  learned model is called production-ready.
- Healthy test traffic produces fewer than one false incident per hour.
- Calibrated expected calibration error is at most 0.10.
- Every prediction is tied to a model version and every dataset to source SHA,
  environment, seed, and manifest.
- Unit, database, API, worker, replay, and end-to-end tests pass.

## Explicit non-goals

- Generating fixes with an LLM.
- Autonomous code changes or remediation.
- Claiming that correlation or attention is causal proof.
- Supporting arbitrary multi-tenant production scale in the capstone release.
- Replacing full observability tools for unrestricted ad-hoc telemetry search.
