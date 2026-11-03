# Free DoWhy GCM integration

TraceLens integrates [DoWhy GCM](https://github.com/py-why/dowhy), an existing
causal-analysis library under the [MIT license](https://github.com/py-why/dowhy/blob/main/LICENSE).
The worker installs `dowhy==0.14`; analysis runs locally without a paid API,
subscription, LLM, or externally hosted inference. Local CPU, memory, and storage
are still required. DoWhy is an analysis library, not a turnkey commercial
observability platform or a universally pretrained debugger.

## Data and analysis flow

```text
OpenTelemetry traces, metrics, logs
    -> normalized PostgreSQL telemetry
    -> 10-second service features and observed call graph
    -> incident detector
    -> DoWhy GCM latency adapter
    -> signed attribution, evidence, engine status, runbook
    -> PostgreSQL incident API and Grafana
```

The adapter observes healthy and incident feature buckets. For a supported
latency incident it selects aligned service p95 latency observations strictly
before incident onset, excludes abnormal baseline buckets, and fits DoWhy's
built-in causal mechanisms to that baseline. It then attributes abnormal target
latency to modeled components using DoWhy's anomaly attribution implementation.
Ground truth, fault flags, and experiment labels are reserved for evaluation.

DoWhy's [official microservice example](https://www.pywhy.org/dowhy/v0.14/example_notebooks/gcm_rca_microservice_architecture.html)
explains the central modeling assumption: when a caller waits for a synchronous
dependency, dependency latency influences caller latency. The causal arrows for
latency therefore reverse the observed service-call arrows. A service graph is
useful context, but does not prove that assumption for arbitrary applications.

## Scope and fallback

The initial integration handles observed service-latency propagation on a
bounded acyclic graph. It requires an observed target latency anomaly, enough
aligned healthy samples, and measured spans for the modeled services.

Unsupported or insufficient inputs use the existing heuristic diagnosis with
an explicit reason. Examples include a cold start, missing service spans,
constant/unusable baseline data, cyclic dependencies, asynchronous message
propagation, errors-only incidents, and resource-only incidents. A missing or
failed DoWhy installation also produces an identified fallback. Fallback
results retain their own model version; they are never relabeled as successful
DoWhy execution.

Unmeasured common causes such as shared network congestion can invalidate the
causal model. Aggregated p95 time buckets also lose per-request relationships.
Results are conditional investigative evidence, not proof of a cause, and
human review remains necessary.

## Score and evidence contract

- Successful candidates use a version such as
  `dowhy-gcm-0.14-latency-v1`.
- `score` and the corresponding evidence contribution retain the signed DoWhy
  attribution. Negative values can represent contributions that reduce the
  observed anomaly and must not be clamped into positive probabilities.
- `probability` is null. The initial integration does not provide calibrated
  certainty; a numeric attribution must not be rendered as a percentage.
- Incident `metadata.analysis` includes `provider`, `status`, `reason`, `target`,
  and `baseline_samples`, plus available graph, time-window, and assumption
  details. `status=analyzed` means DoWhy ran; `status=fallback` explains why it
  did not produce the diagnosis.
- Grafana shows actual engine/version, status, reason, raw attribution/ranking
  scores, and n/a for unavailable probability. Historical classifier scores
  remain distinguishable from attribution values.

## Configure and run

The Compose worker installs DoWhy automatically. Existing `.env` files should
include:

```dotenv
RCA_ANALYSIS_ENGINE=dowhy
```

Rebuild and recreate the worker after changing its engine:

```powershell
docker compose up -d --build rca-worker
```

For comparison, `RCA_ANALYSIS_ENGINE=heuristic` selects deterministic ranking.
`RCA_ANALYSIS_ENGINE=champion` selects the historical tabular artifact configured
by `RCA_MODEL_PATH`. Those comparison modes do not invoke DoWhy.

Run the controlled library integration check without a database or Docker:

```powershell
python -m pip install -r rca/requirements.txt
python scripts/dowhy_smoke.py
```

Exercise that same fixture through the running API, worker, and PostgreSQL:

```powershell
python scripts/dowhy_smoke.py --api-url http://localhost:8000
```

Both commands require actual DoWhy execution, the injected payment service at
rank one, and null candidate probabilities. The live check additionally requires
stored evidence, a dependency graph, and a recommendation. They write a local
report to `.runtime/dowhy-smoke.json`; use `--output` to retain separate reports.
These are controlled synthetic checks, not generalization benchmarks.

Use the normal synthetic smoke and OpenTelemetry Demo scripts to test other
faults through the deployed path. Inspect incident
metadata and candidate engine versions to confirm whether a specific run used
DoWhy or the heuristic fallback.

The legacy `rca.export_dataset` command is for supervised heuristic/champion
reranker features. It rejects DoWhy candidates rather than mixing signed
attributions into that historical feature schema.

## Evaluation boundaries

The frozen results in [REAL_DATA_RESULTS.md](REAL_DATA_RESULTS.md) describe the
earlier heuristic/tabular system. They do not establish DoWhy accuracy.
The controlled library check verifies integration on a known numerical latency
fault; a representative benchmark still needs diverse real fault runs, healthy
controls, missing-data cases, and measured processing/detection time.

Retain failed diagnoses in evaluation. The current real-capture acceptance gate
marks only successful top-3 diagnoses eligible, so evaluating its accepted runs
alone introduces selection bias. Compare DoWhy, heuristic, and optional tabular
results on the same untouched experiments, report fallback frequency, and keep
causal attribution scores separate from probability-calibration metrics.
