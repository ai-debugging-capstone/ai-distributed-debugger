# M6 known-failure RCA evaluation

Run from the repository root with the environment described in [TESTING.md](TESTING.md).
The evaluator uses only existing dependencies. It reads the manifests produced by
`scripts/run-demo-experiment.ps1` / `.sh` and run-scoped capture summaries, reuses
the runner's scenario/causal-edge labels and `rca.evaluation.hit_at_k`, and writes
a JSON report. It never starts containers, injects faults, registers experiments,
exports training data, trains models, or writes to the database.

## Offline example

These checked-in fixtures contain **handwritten diagnoses**, not real captures
or predictions from the champion. They demonstrate payment, cart, and ad-service
reporting, including failures. Their manifests contain only the runner fields
required for evaluation.

```powershell
.\.venv\Scripts\python.exe -m rca.experiment_evaluation `
  tests/fixtures/rca_evaluation/payment.json `
  tests/fixtures/rca_evaluation/cart.json `
  tests/fixtures/rca_evaluation/ad.json `
  --summary-dir tests/fixtures/rca_evaluation/summaries `
  --output tmp/m6-fixture-report.json
```

Use a new output filename each time: existing files are never overwritten. Omit
`--output` to print repeatable JSON to stdout without creating a file. This example
deliberately exits **1**, with the following results:

| Scenario | Expected primary cause | Observed accepted cause rank | Status | Hit@1 | Hit@3 |
| --- | --- | ---: | --- | ---: | ---: |
| paymentFailure | service:payment (declared checkout->payment edge also accepted) | 1 | passed | 1 | 1 |
| cartFailure | service:cart | 4 | failed | 0 | 0 |
| adFailure | service:ad | absent | inconclusive: no logs or incident | 0 | 0 |

Overall Hit@1 = Hit@3 = **1/3**, with one passed, one failed, and one inconclusive
diagnosis. These numbers are an evaluator regression example, **not M6 model
accuracy evidence**.

## Evaluate existing captures

Select an explicit allowlist of manifests from `.runtime/experiments/<UUID>.json`.
Include all attempts in the predeclared evaluation batch, including aborted ones.
Do not filter by `capture_valid`, `training_eligible`, acceptance, or presence in
the training export: the existing capture gate requires a top-three hit and
therefore an accepted-only sample would systematically hide diagnosis failures.
Use independent holdout runs when measuring the deployed model's generalization;
the evaluator preserves each manifest's split but does not enforce partitioning.

```powershell
# Replace these placeholders with the explicit selected capture UUIDs.
$m6Manifests = @(
  '.runtime/experiments/<payment-run-id>.json'
  '.runtime/experiments/<cart-run-id>.json'
  '.runtime/experiments/<ad-run-id>.json'
)

# Read-only GET requests against already captured runs:
.\.venv\Scripts\python.exe -m rca.experiment_evaluation @m6Manifests `
  --api-url http://localhost:8000 --output tmp/m6-existing-captures.json
```

For reproducible offline replay, save each raw GET response from
`/api/v1/experiments/<UUID>/capture-summary` as `<UUID>.json`. Keep the manifests
and summaries together as evidence. For example, using that same allowlist:

```powershell
# This must be a NEW directory; creation fails if it already exists.
$m6SummaryDir = 'tmp/m6-saved-summaries'
New-Item -ItemType Directory -Path $m6SummaryDir -ErrorAction Stop | Out-Null
$m6Utf8 = New-Object System.Text.UTF8Encoding $false
foreach ($m6Manifest in $m6Manifests) {
  $m6RunId = (Get-Content -Raw -LiteralPath $m6Manifest | ConvertFrom-Json).experiment_id
  $m6Summary = Invoke-RestMethod "http://localhost:8000/api/v1/experiments/$m6RunId/capture-summary"
  $m6SummaryPath = Join-Path (Resolve-Path $m6SummaryDir) "$m6RunId.json"
  [System.IO.File]::WriteAllText($m6SummaryPath, ($m6Summary | ConvertTo-Json -Depth 100), $m6Utf8)
}
.\.venv\Scripts\python.exe -m rca.experiment_evaluation @m6Manifests `
  --summary-dir $m6SummaryDir --output tmp/m6-saved-replay.json
```

The snapshot example writes UTF-8 without BOM on both Windows PowerShell and
PowerShell 7. `--api-url` and `--summary-dir` are mutually
exclusive. Missing, malformed, or unavailable summaries remain inconclusive rows;
other selected runs are still evaluated. Duplicate run IDs are rejected.

## New experiments require separate approval

No live experiment was run for this implementation. Obtain approval before using
the existing injection scripts. After approval, use their existing single-fault
reset, warm-up, injection, recovery, cleanup, and manifest path; do not build a
second injector. A minimal proposed batch is:

| Scenario argument | Variant | Expected primary cause | Example capture seed |
| --- | --- | --- | ---: |
| paymentFailure | 100% | service:payment | 6101 |
| cartFailure | 100% | service:cart | 6102 |
| adFailure | on | service:ad | 6103 |

For each row, pass `-Scenario`, `-Variant`, `-Seed`, and `-Split holdout` to
`scripts/run-demo-experiment.ps1` (or the matching Bash options), following the
[capture protocol](DATASET_AND_EVALUATION.md#single-fault-capture-procedure).
Predeclare timing/severity and repeat independent runs before making accuracy
claims. The seed records provenance/partitioning; it does not control stock Locust
request scheduling. Retain the manifest even if the runner rejects the capture.
Evaluate the entire selected batch using the read-only commands above.

## Report semantics and limits

- Each run records scenario, variant, seed, split, capture status, expected primary
  cause, accepted exact cause identities, telemetry counts, and input locations.
  Identity includes entity type. Only explicitly declared edges are alternatives;
  arbitrary callers or substring matches do not count.
- Each incident records the complete actual ranking, model versions supplied by
  the summary, best accepted cause rank (`null` if absent), Hit@1, Hit@3, status,
  and reasons. Input order is normalized using stored ranks. All run-scoped
  incidents are evaluated separately, including warm-up/recovery incidents; no
  best incident is selected. This is a conservative run-level smoke evaluation,
  not an event-alignment/detection-latency benchmark.
- `passed` means a valid diagnosis hit the top three; `failed` means a valid
  diagnosis missed the top three (including an absent expected candidate).
  Inconclusive means missing signals, incidents, candidates, an unavailable or
  mismatched summary, malformed ranking, or an unfinished capture/fault interval.
  Aborted captures with complete intervals and usable diagnoses can still be
  scored. Every inconclusive diagnosis contributes zero to both metrics.
- Ranks must be unique, positive, contiguous integers starting at one; duplicate
  candidate identities or unknown incident membership make output inconclusive.
  Multiple incidents never share one flattened ranking. A run with no incidents
  contributes one inconclusive placeholder diagnosis.
- Aggregate and per-scenario Hit@k average incidents within each run, then weight
  runs equally. Counts refer to diagnoses, while `run_count` reports experiments.
  This prevents a run producing many incidents from dominating the result.
- Exit codes: **0** all diagnoses pass top three; **1** any failed/inconclusive
  diagnosis; **2** invalid invocation, manifest, duplicate run, or output error.
  A top-three pass does not imply Hit@1 = 1. Evaluation never changes the existing
  capture acceptance rules or makes rejected runs training-eligible.
- API summaries reflect current persisted rankings; freeze responses before
  comparing versions. Predeclared independent batches and more repetitions are
  needed for statistical claims. Existing real-data evidence is payment-only;
  there is no new real cart/ad accuracy measurement in this change.

## Verification on November 3, 2026

Python 3.13.7 / pytest 9.1.1 on Windows:

```powershell
.\.venv\Scripts\python.exe -m pytest -c pytest.ini --junitxml=tmp/m6-full-offline.xml
.\.venv\Scripts\python.exe -m pip check
```

**199 collected: 188 passed, 11 PostgreSQL tests skipped, zero failures.** This
adds 34 evaluation cases to the 165-test M5 suite and retains all existing offline
coverage, including six component tests. The final run completed in 1.13 seconds.
`pip check` reported no broken requirements; `git diff --check` passed. The offline
example above was also executed and wrote `tmp/m6-fixture-report.json` with the
expected exit code and results. Generated reports/JUnit evidence stay in ignored
`tmp/`; fixture inputs and this result record are checked-in source files.

The sandbox initially hung in the first API TestClient setup at Python's internal
Windows `socketpair()` initialization. The successful full run used approved
execution outside the sandbox. A shared pytest temporary directory also had an
ownership conflict; the new tests now use isolated `TemporaryDirectory` fixtures,
consistent with existing tests. Neither workaround changes product behavior.

During the initial offline verification, the Docker client existed, but its Linux engine pipe
`dockerDesktopLinuxEngine` was unavailable, and no test-only PostgreSQL admin URL
was configured. The 11 database tests were therefore not run; no full 199-test
database-inclusive pass is claimed. Use the disposable-database procedure in
[TESTING.md](TESTING.md) when Docker is available. One existing Starlette HTTPX
deprecation warning remains. No live injections, model changes, schema changes,
dashboard changes, existing-data deletion, commits, or pushes were performed.

### Complete PostgreSQL-inclusive rerun

Later on November 3, 2026, after Docker Desktop was started, the documented
disposable-server procedure completed successfully using `m6-testing-postgres`,
`postgres:17.6-alpine`, and localhost:55433. This container had its own newly
created anonymous volume and used the documented test-only credentials.

```powershell
$env:TEST_POSTGRES_ADMIN_URL = 'postgresql://m5_test:m5-test-only@127.0.0.1:55433/m5_admin'
.\.venv\Scripts\python.exe -m pytest -c pytest.ini --run-postgres --junitxml=tmp/m6-testing-full.xml
Remove-Item Env:TEST_POSTGRES_ADMIN_URL
```

**199 passed, zero failed, zero skipped in 3.94 seconds**, including all 11 real
PostgreSQL integration tests. No M6 code fixes were necessary. One existing
Starlette HTTPX deprecation warning remains. The run used execution outside the
Windows sandbox for Docker access and asyncio loopback support.
The JUnit file records 201 cases because pytest also emits the two existing
unittest subtests; the collected test count and console result are 199.

The post-test query for `m5_test_%` databases returned zero rows. Only the exact
container created for this rerun was stopped; its `--rm` policy removed the
container and its anonymous volume, and both removals were verified. The main
application database, containers, and volumes were untouched. No live injection,
dashboard/model changes, commits, or pushes were performed.
