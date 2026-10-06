# Verified Real-Data Results

This document freezes the training state produced on October 4, 2026 and the
independent unseen holdout completed on October 5, 2026. The holdout is present
only in a separate audit export and never enters model fitting, validation, or
test selection.

## Source and capture contract

- Testbed: OpenTelemetry Demo `3.1.0`
- Repository: `https://github.com/open-telemetry/opentelemetry-demo.git`
- Exact source commit: `dedc0178918e260823323b8d95005a8cb924b007`
- Demo profile: `full`
- Workload recorded by every run: `mixed-commerce`
- Warm-up / fault / recovery: 130 / 120 / 40 seconds
- Capture acceptance cutoff: causal service or declared causal edge in top 3
- Isolation: the complete Demo deployment was reset before every run

The capture seed is a provenance and partition seed. It does **not** make the
stock Locust 2.44.4 request schedule deterministic; response timing and
cooperative scheduling still affect the exact request sequence.

## Accepted run ledger

| Split | Run ID | Injected fault | Seed | Spans | Metric points | Logs | Capture-time accepted candidate |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| train | `0281b905-7a9f-43b3-97f2-d7368d845978` | `paymentFailure=100%` | 2101 | 39,003 | 30,453 | 12,559 | `checkout->payment`, rank 1 |
| train | `16bfae6e-46c7-45b4-9b54-ee7772eda3e5` | `paymentUnreachable=on` | 2301 | 42,330 | 32,160 | 13,566 | `checkout->payment`, rank 1 |
| validation | `60bcaebc-0aed-41cb-b8d1-f83a3a2a941a` | `paymentFailure=100%` | 2113 | 39,011 | 32,393 | 12,597 | `checkout->payment`, rank 1 |
| test | `12f32149-e26d-40ae-9379-81970f79a8c1` | `paymentFailure=100%` | 2123 | 36,598 | 32,032 | 12,017 | `checkout->payment`, rank 1 |
| holdout | `4fc172f3-61a4-4596-bf50-254a427b351f` | `paymentUnreachable=on` | 2407 | 42,276 | 31,928 | 13,501 | `checkout->payment`, rank 1 |
| **Total** | **5 runs** |  |  | **199,218** | **158,966** | **64,240** |  |

The accepted candidate column records the online decision used by the capture
gate. For the first four runs it records the
pre-training heuristic decision; the holdout row records the already-promoted
learned champion. Failed, aborted, invalid, unlabelled, and non-allowlisted runs
are excluded from both frozen exports.

## Immutable dataset

- Artifact: [`../data/exports/otel-demo-candidates-20261004-v2.jsonl`](../data/exports/otel-demo-candidates-20261004-v2.jsonl)
- Provenance sidecar: [`../data/exports/otel-demo-candidates-20261004-v2.jsonl.metadata.json`](../data/exports/otel-demo-candidates-20261004-v2.jsonl.metadata.json)
- Registry identity: `otel-demo-candidate-ranking:20261004-v2`
- SHA-256: `39270c57baca1bd9182f29a32b367cb4d4fd80e8868749887d187a94fdb764db`
- Rows: 40 candidate rows across four runs and four incidents
- Positive rows: 7
- Unmatched incidents: 0
- Partition: two train runs, one validation run, one test run, zero holdout runs

This four-run artifact is the immutable **model-development dataset** covering
training, validation, and test; it deliberately does not contain the later
holdout. The exporter joins labels only after the online candidate features
have been stored. The metadata sidecar freezes the exact run allowlist,
repository, version, commit, source provenance, acceptance evidence, and
telemetry counts.

## Promoted compact champion

- Registry identity: `candidate-ranker:otel-demo-20261004-v3-compact`
- Backend: `portable-linear`
- Training seed: `2301`
- Feature profile: `compact`
- Model artifact: [`../artifacts/champion/model-1df99581e825cf1a.json`](../artifacts/champion/model-1df99581e825cf1a.json)
- Evaluation artifact: [`../artifacts/champion/evaluation-1df99581e825cf1a.json`](../artifacts/champion/evaluation-1df99581e825cf1a.json)
- Model SHA-256: `1df99581e825cf1a03f7162ed4bd4143a54271931e40e032b415a7e7ee283533`

The explicit compact feature schema is:

1. `baseline_inverse_rank`
2. `baseline_probability`
3. `baseline_score`
4. `candidate_is_dependency_edge`
5. `candidate_is_service`

This profile is intentional. With only two independent training incidents, a
high-dimensional fit can memorize run-specific evidence magnitudes and service
mix. Compact training keeps the transparent heuristic's ordering, score, and
probability plus entity type, while still producing a learned re-ranking. The
`full` profile remains available for a future dataset with enough independent
runs to support its evidence dimensions.

## Recorded evaluation

| Partition | Complete runs | Ranking examples | Hit@1 | Hit@3 | MRR | nDCG@3 | MAP@5 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| validation | 1 | 1 | 1.000 | 1.000 | 1.000 | 0.920 | 0.833 |
| test | 1 | 1 | 1.000 | 1.000 | 1.000 | 0.920 | 0.833 |
| holdout | 0 | 0 | n/a | n/a | n/a | n/a | n/a |

These metrics verify that the pipeline, run-level split, serialization, and
worker inference contract operate end to end. They do **not** establish a
reliable population estimate: validation and test each contain only one
`paymentFailure` incident, while the only `paymentUnreachable` capture is in
training. Confidence intervals and per-fault generalization claims would be
meaningless at this sample size. The model is a capstone-scale champion with a
heuristic fallback, not a production-ready universal RCA model.

## Fresh unseen champion holdout

**Status on October 5, 2026: COMPLETED AND ACCEPTED.** An earlier interrupted
attempt remains marked aborted and excluded. The replacement run completed the
normal independent reset, warm-up, timed fault, recovery, and top-3 acceptance
gate while the checksum-identified learned champion was active.

| Field | Result |
| --- | --- |
| Run ID | `4fc172f3-61a4-4596-bf50-254a427b351f` |
| Incident ID | `9e1913e0-1dfd-511d-9d47-d35c572187ae` |
| Scenario / variant | `paymentUnreachable=on` |
| Split | `holdout` |
| Model version observed on candidates | `champion-portable-linear-sha256:1df99581e825` |
| Causal `checkout->payment` rank | `1` |
| Score / probability | `0.9999997795202018` |
| Top-3 acceptance | `passed` |
| Telemetry counts | 42,276 spans; 31,928 metric points; 13,501 logs |

The separate, fail-closed audit export adds the holdout without changing the
training artifact or registry:

- Artifact: [`../data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl`](../data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl)
- Provenance sidecar: [`../data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl.metadata.json`](../data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl.metadata.json)
- Dataset SHA-256: `b04f9af1a7e84608f6618b9e4bb39a570f50071de3cfd8cb48f8becaea5b82de`
- Contents: 50 candidates, five runs/incidents, eight positives, zero unmatched incidents
- Audit evaluation: [`../artifacts/holdout-audit-20261005-v3/evaluation-1df99581e825cf1a.json`](../artifacts/holdout-audit-20261005-v3/evaluation-1df99581e825cf1a.json)
- Audit evaluation SHA-256: `7f50c1bc6ecb403db7817a9c4eb814dd598033bd357f9239568233992a9dbd3b`

Re-fitting from only the same two declared training runs reproduced the exact
deployed model checksum. The untouched holdout has one ranking example with
Hit@1, Hit@3, MRR, nDCG@3, and MAP@5 all equal to `1.0`. This is genuine
end-to-end evidence on an unseen run, but one holdout incident is still far too
small for a production-scale statistical claim.

## Reproduce collection and artifacts

Start TraceLens and the full pinned demo, then repeat the five independent
captures. New captures receive new UUIDs, so these commands reproduce the
protocol rather than the immutable run identities above:

```powershell
Copy-Item .env.example .env
docker compose up --build -d
.\scripts\fetch-demo.ps1
.\scripts\start-demo.ps1 -Profile full

.\scripts\run-demo-experiment.ps1 -Scenario paymentFailure -Variant "100%" -Seed 2101 -Split train -WarmupSeconds 130 -DurationSeconds 120 -RecoverySeconds 40 -AcceptanceTopK 3
.\scripts\run-demo-experiment.ps1 -Scenario paymentUnreachable -Variant "on" -Seed 2301 -Split train -WarmupSeconds 130 -DurationSeconds 120 -RecoverySeconds 40 -AcceptanceTopK 3
.\scripts\run-demo-experiment.ps1 -Scenario paymentFailure -Variant "100%" -Seed 2113 -Split validation -WarmupSeconds 130 -DurationSeconds 120 -RecoverySeconds 40 -AcceptanceTopK 3
.\scripts\run-demo-experiment.ps1 -Scenario paymentFailure -Variant "100%" -Seed 2123 -Split test -WarmupSeconds 130 -DurationSeconds 120 -RecoverySeconds 40 -AcceptanceTopK 3
.\scripts\run-demo-experiment.ps1 -Scenario paymentUnreachable -Variant "on" -Seed 2407 -Split holdout -WarmupSeconds 130 -DurationSeconds 120 -RecoverySeconds 40 -AcceptanceTopK 3
```

To reproduce the checked-in dataset bytes from the already persisted accepted
runs, use the exact fail-closed allowlist and source selector:

```powershell
python -m rca.export_dataset `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --include-run 0281b905-7a9f-43b3-97f2-d7368d845978 `
  --include-run 16bfae6e-46c7-45b4-9b54-ee7772eda3e5 `
  --include-run 60bcaebc-0aed-41cb-b8d1-f83a3a2a941a `
  --include-run 12f32149-e26d-40ae-9379-81970f79a8c1 `
  --source-repository https://github.com/open-telemetry/opentelemetry-demo.git `
  --source-version 3.1.0 `
  --source-sha dedc0178918e260823323b8d95005a8cb924b007 `
  --snapshot-name otel-demo-candidate-ranking `
  --snapshot-version 20261004-v2 `
  --output data/exports/otel-demo-candidates-20261004-v2.jsonl
```

Train the exact compact model and register it against that snapshot:

```powershell
python -m rca.train data/exports/otel-demo-candidates-20261004-v2.jsonl `
  --output-dir artifacts/champion `
  --portable `
  --feature-profile compact `
  --seed 2301 `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --model-name candidate-ranker `
  --model-version otel-demo-20261004-v3-compact
```

On a fresh registry, inspect the generated evaluation first and then explicitly
promote the challenger:

```powershell
python -m rca.promote `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --model-name candidate-ranker `
  --model-version otel-demo-20261004-v3-compact `
  --output-dir artifacts/champion

docker compose up -d --build rca-worker
```

In the existing project registry this version is already the champion, so the
promotion step should not be repeated. Verify `artifacts/champion/model.json`
has the model checksum above and verify worker decisions report
`champion-portable-linear-sha256:1df99581e825`.

After the holdout completes, reproduce the audit artifacts without registering
a new dataset snapshot or model version:

```powershell
python -m rca.export_dataset `
  --database-url postgresql://rca:rca-local-password@localhost:55432/rca `
  --include-run 0281b905-7a9f-43b3-97f2-d7368d845978 `
  --include-run 16bfae6e-46c7-45b4-9b54-ee7772eda3e5 `
  --include-run 60bcaebc-0aed-41cb-b8d1-f83a3a2a941a `
  --include-run 12f32149-e26d-40ae-9379-81970f79a8c1 `
  --include-run 4fc172f3-61a4-4596-bf50-254a427b351f `
  --source-repository https://github.com/open-telemetry/opentelemetry-demo.git `
  --source-version 3.1.0 `
  --source-sha dedc0178918e260823323b8d95005a8cb924b007 `
  --snapshot-name otel-demo-candidate-ranking-audit `
  --snapshot-version 20261005-v3-holdout-audit `
  --output data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl `
  --no-register-snapshot

python -m rca.train `
  data/exports/otel-demo-candidates-20261005-v3-holdout-audit.jsonl `
  --output-dir artifacts/holdout-audit-20261005-v3 `
  --portable `
  --feature-profile compact `
  --seed 2301
```

The second command trains only from rows whose declared split is `train`; the
`holdout` rows are passed exclusively to final evaluation. Omitting
`--database-url` prevents the audit replay from mutating the model registry.
