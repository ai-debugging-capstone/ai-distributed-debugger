"""Read-only evaluation of known Demo failures; never invokes an injector."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any
from uuid import UUID

from rca.evaluation import RankingExample, hit_at_k
from scripts.demo_experiment import (
    expected_causal_candidates,
    load_json,
    request_json,
    validate_scenario_profile,
)


def identity(candidate: dict[str, Any]) -> str:
    # Include the type: a service and a database with the same name differ.
    return json.dumps([candidate["entity_type"], candidate["entity_id"]])


def evaluate_capture(manifest: dict[str, Any], summary: dict[str, Any]) -> dict[str, Any]:
    """Score every incident, retaining misses and unavailable/malformed output.

    Each run has equal weight; within a run, average over all its incidents.
    Inconclusive incidents contribute zero rather than disappearing from metrics.
    Capture acceptance/training eligibility never selects evaluation membership.
    """
    run_id = str(UUID(manifest["experiment_id"]))
    scenario = validate_scenario_profile(manifest["scenario"], manifest["profile"])
    expected = expected_causal_candidates(scenario)
    relevant = frozenset(map(identity, expected))
    problems = []
    if summary.get("run_id") != run_id:
        problems.append("capture-summary run_id does not match manifest")
    if not manifest.get("fault_started_at") or not manifest.get("fault_ended_at"):
        problems.append("manifest has no complete fault interval")
    if manifest.get("status") not in {"completed", "aborted"}:
        problems.append("capture is not terminal; wait for recovery to finish")
    telemetry = summary.get("telemetry")
    if not isinstance(telemetry, dict):
        telemetry = {}
    for signal in ("spans", "metric_points", "log_records"):
        count = telemetry.get(signal)
        if type(count) is not int or count <= 0:
            problems.append(f"no persisted {signal} for run")
    incidents = summary.get("incidents")
    candidates = summary.get("candidates")
    if not isinstance(incidents, list) or not incidents:
        problems.append("no incident was produced for run")
        incidents = []
    incident_ids = [item.get("id") if isinstance(item, dict) else None for item in incidents]
    if any(not isinstance(item, str) or not item for item in incident_ids) or len(set(incident_ids)) != len(incident_ids):
        problems.append("invalid or duplicate incident IDs")
        incident_ids = []
    if not isinstance(candidates, list):
        problems.append("missing candidate list")
        candidates = []
    if any(not isinstance(item, dict) or item.get("incident_id") not in incident_ids for item in candidates):
        problems.append("candidate belongs to an unknown incident")
        candidates = []

    diagnoses = []
    for incident_id in sorted(incident_ids) or [None]:
        reasons = list(problems)
        ranked = [item for item in candidates if item["incident_id"] == incident_id]
        valid = all(
            type(item.get("rank")) is int and item["rank"] > 0
            and isinstance(item.get("entity_type"), str) and bool(item["entity_type"])
            and isinstance(item.get("entity_id"), str) and bool(item["entity_id"])
            for item in ranked
        )
        if valid:
            ranked.sort(key=lambda item: item["rank"])
            valid = (
                [item["rank"] for item in ranked] == list(range(1, len(ranked) + 1))
                and len({identity(item) for item in ranked}) == len(ranked)
            )
        if not ranked:
            reasons.append("incident has no root-cause candidates")
        elif not valid:
            reasons.append("ranking must have unique entities and contiguous integer ranks starting at 1")
        example = RankingExample(relevant, tuple(map(identity, ranked)) if valid and not reasons else ())
        hit1, hit3 = hit_at_k([example], 1), hit_at_k([example], 3)
        expected_rank = next((item["rank"] for item in ranked if valid and identity(item) in relevant), None)
        status = "inconclusive" if reasons else "passed" if hit3 else "failed"
        if status == "failed":
            reasons.append("expected root cause is absent from top 3")
        diagnoses.append({
            "incident_id": incident_id,
            "status": status,
            "expected_rank": expected_rank,
            "actual_ranking": ranked,
            "hit_at_1": hit1,
            "hit_at_3": hit3,
            "reasons": reasons,
        })
    return {
        "run_id": run_id,
        "scenario": manifest["scenario"],
        "variant": manifest.get("variant"),
        "seed": manifest.get("seed"),
        "split": manifest.get("split"),
        "capture_status": manifest.get("status"),
        "expected_root_cause": expected[0],
        "accepted_root_causes": expected,
        "telemetry": telemetry,
        "diagnoses": diagnoses,
        "hit_at_1": sum(row["hit_at_1"] for row in diagnoses) / len(diagnoses),
        "hit_at_3": sum(row["hit_at_3"] for row in diagnoses) / len(diagnoses),
    }


def build_report(runs: list[dict[str, Any]]) -> dict[str, Any]:
    if not runs:
        raise ValueError("at least one capture is required")
    if len({run["run_id"] for run in runs}) != len(runs):
        raise ValueError("duplicate run IDs would weight an experiment more than once")
    diagnoses = [row for run in runs for row in run["diagnoses"]]

    def metrics(items):
        return {key: sum(run[key] for run in items) / len(items) for key in ("hit_at_1", "hit_at_3")}

    return {
        "schema_version": 1,
        "metric_policy": "equal weight per run; average all incidents within run; inconclusive counts as zero",
        "run_count": len(runs),
        "diagnosis_count": len(diagnoses),
        "passed": sum(row["status"] == "passed" for row in diagnoses),
        "failed": sum(row["status"] == "failed" for row in diagnoses),
        "inconclusive": sum(row["status"] == "inconclusive" for row in diagnoses),
        **metrics(runs),
        "by_scenario": {
            scenario: {"run_count": len(items), **metrics(items)}
            for scenario in sorted({run["scenario"] for run in runs})
            if (items := [run for run in runs if run["scenario"] == scenario])
        },
        "runs": runs,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifests", type=Path, nargs="+", help="explicit runner manifest allowlist (including aborted attempts)")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--summary-dir", type=Path, help="offline capture-summary files named <run_id>.json")
    source.add_argument("--api-url", help="GET existing capture summaries; never creates or changes a run")
    parser.add_argument("--output", type=Path, help="new JSON report file; refuses to overwrite existing files")
    args = parser.parse_args(argv)
    try:
        manifests = [load_json(path) for path in args.manifests]
        # Validate the entire allowlist before reading the API or writing output.
        ids = [str(UUID(item["experiment_id"])) for item in manifests]
        if len(set(ids)) != len(ids):
            raise ValueError("duplicate run IDs in manifest allowlist")
        for manifest in manifests:
            validate_scenario_profile(manifest["scenario"], manifest["profile"])
        runs = []
        for path, manifest, run_id in zip(args.manifests, manifests, ids):
            location = (
                str(args.summary_dir / f"{run_id}.json") if args.summary_dir
                else f"{args.api_url.rstrip('/')}/api/v1/experiments/{run_id}/capture-summary"
            )
            error = None
            try:
                summary = load_json(Path(location)) if args.summary_dir else request_json("GET", location)
                if not isinstance(summary, dict):
                    raise ValueError("capture summary must be a JSON object")
            except (OSError, ValueError, SystemExit) as exc:
                error = f"capture_summary_unavailable: {exc}"
                summary = {}
            run = evaluate_capture(manifest, summary)
            run["manifest"] = str(path)
            run["summary_source"] = location
            if error:
                for diagnosis in run["diagnoses"]:
                    diagnosis["reasons"].insert(0, error)
            runs.append(run)
        report = build_report(runs)
        body = json.dumps(report, indent=2, allow_nan=False) + "\n"
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            with args.output.open("x", encoding="utf-8") as handle:
                handle.write(body)
        sys.stdout.write(body)
        return 1 if report["failed"] or report["inconclusive"] else 0
    except (OSError, ValueError, KeyError, TypeError, SystemExit) as exc:
        parser.error(str(exc))


if __name__ == "__main__":
    raise SystemExit(main())
