#!/usr/bin/env python3
"""Capture a labeled OpenTelemetry Demo dataset as a resumable batch of runs.

Each planned run is one invocation of the existing single-run controller
(``run-demo-experiment.sh`` or ``run-demo-experiment.ps1``), so every safety
property of that controller — full Demo reset, recorded fault boundaries,
byte-exact flag restoration, and the acceptance gate — applies unchanged.

The batch adds what one run cannot:

* coverage: every Docker-runnable scenario (and optionally every variant),
  repeated over several capture seeds, interleaved so a partial batch is still
  balanced across scenarios;
* healthy controls, whose incidents are counted as false positives;
* deterministic, run-level split assignment (no run is split across
  train/validation/test), with optional whole-scenario holdout for
  leave-one-scenario-out evaluation;
* a JSONL state file, so an interrupted batch resumes where it stopped.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import demo_experiment  # noqa: E402

EXPERIMENT_ID_PATTERN = re.compile(
    r"Experiment ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"
)
# The worker needs min_history (10) closed 10-second buckets before it can
# score anything; a shorter warm-up puts fault buckets into the baseline.
MIN_WARMUP_SECONDS = 120
TERMINAL_OK = "completed"


@dataclass(frozen=True)
class PlannedRun:
    key: str
    scenario: str
    kind: str
    variant: str
    seed: int
    split: str
    hold_seconds: int


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _stable_rank(*parts: object) -> str:
    return hashlib.sha256("|".join(str(part) for part in parts).encode("utf-8")).hexdigest()


def assign_splits(count: int) -> list[str]:
    """Run-level split shares for one scenario group, in rank order.

    Small groups still land in the evaluation splits: one run trains, two add a
    test run, and three or more reserve at least one validation and one test
    run (about 20% each) with the remainder in train.
    """

    if count <= 0:
        return []
    if count == 1:
        return ["train"]
    if count == 2:
        return ["train", "test"]
    evaluation = max(1, round(count * 0.2))
    train = count - 2 * evaluation
    if train < 1:
        evaluation, train = 1, count - 2
    return ["train"] * train + ["validation"] * evaluation + ["test"] * evaluation


def runnable_scenarios(profile: str) -> list[str]:
    names = []
    for name, scenario in demo_experiment.SCENARIOS.items():
        if scenario.get("platform", "docker") != "docker":
            continue
        required = scenario.get("required_profile")
        if required and required != profile:
            continue
        names.append(name)
    return sorted(names)


def _select(requested: str, available: Sequence[str], label: str) -> list[str]:
    if requested == "all":
        return list(available)
    if requested == "none":
        return []
    chosen = [item.strip() for item in requested.split(",") if item.strip()]
    unknown = sorted(set(chosen) - set(available))
    if unknown:
        raise SystemExit(
            f"Unknown or unsupported {label}: {', '.join(unknown)}. "
            f"Available: {', '.join(available)}"
        )
    return chosen


def build_plan(
    *,
    scenarios: Sequence[str],
    controls: Sequence[str],
    variants: str,
    seeds: int,
    base_seed: int,
    holdout_scenarios: Iterable[str] = (),
    hold_override: int | None = None,
) -> list[PlannedRun]:
    """Deterministic plan, interleaved round-robin by seed.

    Variant lists in the scenario mirror are ordered mildest first, so
    ``strongest`` takes the last one: the variant most likely to produce a
    detectable incident and therefore the fastest route to scenario coverage.
    """

    if seeds < 1:
        raise SystemExit("--seeds must be at least 1")
    holdout = set(holdout_scenarios)
    unknown_holdout = holdout - set(scenarios)
    if unknown_holdout:
        raise SystemExit(
            "Holdout scenarios must also be planned: " + ", ".join(sorted(unknown_holdout))
        )
    groups: dict[str, list[tuple[str, str, int]]] = defaultdict(list)
    for scenario in scenarios:
        declared = tuple(demo_experiment.SCENARIOS[scenario]["variants"])
        chosen = declared if variants == "all" else declared[-1:]
        for variant in chosen:
            for index in range(seeds):
                groups[scenario].append((scenario, variant, base_seed + index))
    for control in controls:
        for index in range(seeds):
            groups[control].append((control, demo_experiment.CONTROL_VARIANT, base_seed + index))

    planned: list[PlannedRun] = []
    for group, members in groups.items():
        kind = "control" if demo_experiment.is_control(group) else "fault"
        ordered = sorted(members, key=lambda item: _stable_rank(*item))
        if kind == "control":
            # Controls carry no labels; they are evaluation-only by design.
            splits = ["unassigned"] * len(ordered)
        elif group in holdout:
            splits = ["holdout"] * len(ordered)
        else:
            splits = assign_splits(len(ordered))
        for (scenario, variant, seed), split in zip(ordered, splits):
            spec = (
                demo_experiment.HEALTHY_CONTROLS[scenario]
                if kind == "control"
                else demo_experiment.SCENARIOS[scenario]
            )
            planned.append(
                PlannedRun(
                    key=f"{scenario}|{variant}|{seed}",
                    scenario=scenario,
                    kind=kind,
                    variant=variant,
                    seed=seed,
                    split=split,
                    hold_seconds=int(hold_override or spec["hold_seconds"]),
                )
            )
    # Round-robin: all scenarios for the first seed, then the second, ... so a
    # batch stopped early still covers every scenario.
    planned.sort(key=lambda run: (run.seed, run.kind != "control", run.scenario, run.variant))
    return planned


def read_state(path: Path) -> dict[str, dict[str, Any]]:
    """Latest record per run key; later lines supersede earlier ones."""

    latest: dict[str, dict[str, Any]] = {}
    if not path.exists():
        return latest
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            latest[str(record["key"])] = record
    return latest


def append_state(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def runner_command(
    shell: str,
    run: PlannedRun,
    *,
    warmup: int,
    recovery: int,
    top_k: int,
) -> list[str]:
    if shell == "powershell":
        executable = shutil.which("pwsh") or shutil.which("powershell") or "powershell"
        return [
            executable,
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(SCRIPT_DIR / "run-demo-experiment.ps1"),
            "-Scenario", run.scenario,
            "-Variant", run.variant,
            "-Seed", str(run.seed),
            "-Split", run.split,
            "-WarmupSeconds", str(warmup),
            "-DurationSeconds", str(run.hold_seconds),
            "-RecoverySeconds", str(recovery),
            "-AcceptanceTopK", str(top_k),
        ]
    return [
        "bash",
        str(SCRIPT_DIR / "run-demo-experiment.sh"),
        "--scenario", run.scenario,
        "--variant", run.variant,
        "--seed", str(run.seed),
        "--split", run.split,
        "--warmup", str(warmup),
        "--duration", str(run.hold_seconds),
        "--recovery", str(recovery),
        "--acceptance-top-k", str(top_k),
    ]


def manifest_outcome(experiment_id: str | None) -> dict[str, Any]:
    """Summarize the single-run manifest the controller left behind."""

    if not experiment_id:
        return {}
    path = PROJECT_ROOT / ".runtime" / "experiments" / f"{experiment_id}.json"
    if not path.is_file():
        return {"manifest": str(path), "manifest_missing": True}
    manifest = demo_experiment.load_json(path)
    acceptance = manifest.get("acceptance") or {}
    matched = acceptance.get("matched_candidate") or {}
    return {
        "manifest": str(path),
        "manifest_status": manifest.get("status"),
        "capture_valid": manifest.get("capture_valid"),
        "accepted": acceptance.get("passed"),
        "matched_rank": matched.get("rank"),
        "matched_entity": matched.get("entity_id"),
        "false_positive_incidents": acceptance.get("false_positive_incidents"),
        "failure_reason": manifest.get("failure_reason"),
    }


def execute(run: PlannedRun, command: Sequence[str], timeout_seconds: int) -> tuple[int, str | None]:
    """Run one controller, streaming its output, and return (exit code, run id)."""

    experiment_id: str | None = None
    process = subprocess.Popen(
        command,
        cwd=PROJECT_ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        errors="replace",
        bufsize=1,
    )
    deadline = time.monotonic() + timeout_seconds
    assert process.stdout is not None
    try:
        for line in process.stdout:
            sys.stdout.write(f"  [{run.scenario}] {line}")
            sys.stdout.flush()
            match = EXPERIMENT_ID_PATTERN.search(line)
            if match and experiment_id is None:
                experiment_id = match.group(1)
            if time.monotonic() > deadline:
                process.terminate()
                break
        return process.wait(timeout=600), experiment_id
    except KeyboardInterrupt:
        # The controller received the same interrupt; let its cleanup restore
        # the flag file, flagd, Locust and the gateway before we exit.
        print("\nInterrupt received; waiting for the run controller to clean up...")
        process.wait()
        raise


def summarize(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    faults: dict[str, Counter] = defaultdict(Counter)
    controls: dict[str, Counter] = defaultdict(Counter)
    for record in records:
        outcome = record.get("outcome") or {}
        if record["kind"] == "control":
            bucket = controls[record["scenario"]]
            bucket["runs"] += 1
            bucket["valid"] += int(bool(outcome.get("capture_valid")))
            incidents = outcome.get("false_positive_incidents")
            if isinstance(incidents, int):
                bucket["false_positive_incidents"] += incidents
                bucket["runs_with_false_positive"] += int(incidents > 0)
        else:
            bucket = faults[record["scenario"]]
            bucket["runs"] += 1
            bucket["accepted"] += int(outcome.get("accepted") is True)
            rank = outcome.get("matched_rank")
            if isinstance(rank, int):
                bucket["hit_at_1"] += int(rank == 1)
    return {
        "faults": {name: dict(counter) for name, counter in sorted(faults.items())},
        "controls": {name: dict(counter) for name, counter in sorted(controls.items())},
    }


def print_summary(summary: dict[str, Any]) -> None:
    print("\nBatch summary")
    print(f"  {'fault scenario':32} runs  accepted(top-k)  hit@1")
    for name, row in summary["faults"].items():
        print(
            f"  {name:32} {row.get('runs', 0):>4}  {row.get('accepted', 0):>15}  "
            f"{row.get('hit_at_1', 0):>5}"
        )
    if summary["controls"]:
        print(f"  {'healthy control':32} runs  valid  false-positive incidents (runs with any)")
        for name, row in summary["controls"].items():
            print(
                f"  {name:32} {row.get('runs', 0):>4}  {row.get('valid', 0):>5}  "
                f"{row.get('false_positive_incidents', 0)} "
                f"({row.get('runs_with_false_positive', 0)})"
            )


def active_profile() -> str:
    state = PROJECT_ROOT / ".runtime" / "demo-profile"
    if state.is_file():
        return state.read_text(encoding="utf-8").strip() or "full"
    return os.environ.get("OTEL_DEMO_PROFILE", "full")


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    root.add_argument("--scenarios", default="all", help="'all' or comma-separated flag names")
    root.add_argument(
        "--controls", default="all", help="'all', 'none', or comma-separated healthy controls"
    )
    root.add_argument("--variants", choices=("strongest", "all"), default="strongest")
    root.add_argument("--seeds", type=int, default=3, help="Capture seeds per scenario/variant")
    root.add_argument("--base-seed", type=int, default=3000)
    root.add_argument(
        "--holdout-scenario",
        action="append",
        default=[],
        help="Put every run of this scenario in the holdout split (repeatable)",
    )
    # Matches defaults.warmup_seconds in experiments/otel-demo-v3.1.0.yaml. Every
    # run starts from a full Demo restart; a shorter warm-up builds the baseline
    # from process start-up and turns its settling into false positives.
    root.add_argument("--warmup", type=int, default=600)
    root.add_argument("--hold", type=int, default=None, help="Override per-scenario hold seconds")
    root.add_argument("--recovery", type=int, default=120)
    root.add_argument("--acceptance-top-k", type=int, default=3)
    root.add_argument("--profile", default=None, help="Defaults to the running Demo profile")
    root.add_argument(
        "--shell",
        choices=("auto", "bash", "powershell"),
        default="auto",
        help="Which single-run controller to invoke",
    )
    root.add_argument("--state", type=Path, default=None, help="Batch state JSONL (resume target)")
    root.add_argument(
        "--retry-failed", action="store_true", help="Re-run keys whose last attempt failed"
    )
    root.add_argument("--max-consecutive-failures", type=int, default=3)
    root.add_argument("--dry-run", action="store_true", help="Print the plan and exit")
    root.add_argument("--allow-short-warmup", action="store_true")
    return root


def main(argv: Sequence[str] | None = None) -> int:
    arguments = parser().parse_args(argv)
    if arguments.warmup < MIN_WARMUP_SECONDS and not arguments.allow_short_warmup:
        raise SystemExit(
            f"--warmup {arguments.warmup}s is shorter than the {MIN_WARMUP_SECONDS}s the worker "
            "needs to build a baseline before the fault; pass --allow-short-warmup to override"
        )
    if not 1 <= arguments.acceptance_top_k <= 100:
        raise SystemExit("--acceptance-top-k must be from 1 through 100")
    profile = arguments.profile or active_profile()
    scenarios = _select(arguments.scenarios, runnable_scenarios(profile), "scenarios")
    controls = _select(arguments.controls, sorted(demo_experiment.HEALTHY_CONTROLS), "controls")
    plan = build_plan(
        scenarios=scenarios,
        controls=controls,
        variants=arguments.variants,
        seeds=arguments.seeds,
        base_seed=arguments.base_seed,
        holdout_scenarios=arguments.holdout_scenario,
        hold_override=arguments.hold,
    )
    shell = arguments.shell
    if shell == "auto":
        shell = "powershell" if os.name == "nt" else "bash"
    state_path = arguments.state or (
        PROJECT_ROOT
        / ".runtime"
        / "batches"
        / f"batch-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    )
    previous = read_state(state_path)
    pending = [
        run
        for run in plan
        if previous.get(run.key, {}).get("status") != TERMINAL_OK
        and (arguments.retry_failed or run.key not in previous)
    ]
    per_run = arguments.warmup + arguments.recovery + 420  # reset + acceptance margin
    estimate = sum(per_run + run.hold_seconds for run in pending)
    print(
        f"Profile {profile}; {len(plan)} planned runs, {len(pending)} to execute "
        f"(~{estimate / 3600:.1f} h); state: {state_path}"
    )
    split_counts = Counter(run.split for run in plan)
    print("  splits: " + ", ".join(f"{name}={count}" for name, count in sorted(split_counts.items())))
    if arguments.dry_run:
        for run in plan:
            marker = "todo" if run in pending else "done"
            print(f"  {marker} {run.key:48} {run.kind:7} {run.split:10} hold={run.hold_seconds}s")
        return 0

    consecutive_failures = 0
    for index, run in enumerate(pending, start=1):
        print(f"\n[{index}/{len(pending)}] {run.key} ({run.kind}, split={run.split})")
        started = utc_now()
        command = runner_command(
            shell,
            run,
            warmup=arguments.warmup,
            recovery=arguments.recovery,
            top_k=arguments.acceptance_top_k,
        )
        try:
            exit_code, experiment_id = execute(
                run, command, timeout_seconds=per_run + run.hold_seconds + 900
            )
        except KeyboardInterrupt:
            append_state(
                state_path,
                {**asdict(run), "status": "interrupted", "started_at": started, "ended_at": utc_now()},
            )
            return 130
        outcome = manifest_outcome(experiment_id)
        status = TERMINAL_OK if exit_code == 0 else "failed"
        append_state(
            state_path,
            {
                **asdict(run),
                "status": status,
                "exit_code": exit_code,
                "experiment_id": experiment_id,
                "started_at": started,
                "ended_at": utc_now(),
                "outcome": outcome,
            },
        )
        consecutive_failures = 0 if status == TERMINAL_OK else consecutive_failures + 1
        if consecutive_failures >= arguments.max_consecutive_failures:
            print(
                f"Stopping: {consecutive_failures} consecutive runs failed. Check the Demo and "
                f"resume with --state {state_path} --retry-failed"
            )
            break

    records = [
        record
        for key, record in read_state(state_path).items()
        if key in {run.key for run in plan}
    ]
    summary = summarize(records)
    print_summary(summary)
    summary_path = state_path.with_suffix(".summary.json")
    demo_experiment.atomic_write(summary_path, {"state": str(state_path), **summary})
    print(f"Summary written to {summary_path}")
    failed = [record for record in records if record.get("status") != TERMINAL_OK]
    return 1 if failed or len(records) < len(plan) else 0


if __name__ == "__main__":
    raise SystemExit(main())
