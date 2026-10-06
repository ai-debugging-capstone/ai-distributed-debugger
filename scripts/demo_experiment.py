#!/usr/bin/env python3
"""Safely edit a Demo feature flag and maintain reproducible run manifests."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import tempfile
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import UUID, uuid5

ACTUAL_WORKLOAD = "mixed-commerce"
UPSTREAM_LOAD_GENERATOR = "Locust 2.44.4"
UPSTREAM_WORKLOAD_RNG_NOTE = (
    "The stock OpenTelemetry Demo Locust launcher has no supported seed option; "
    "concurrent request scheduling remains nondeterministic."
)


# Mirrors the executable scenarios in experiments/otel-demo-v3.1.0.yaml.  The
# controller keeps this compact runtime mapping in the standard-library script
# so PowerShell/Bash users do not need a host PyYAML installation.
SCENARIOS: dict[str, dict[str, Any]] = {
    "adFailure": {
        "entity_type": "service",
        "entity_id": "ad",
        "mechanism": "service_error",
        "sli": "frontend_ad_availability",
        "intended_workload": "browse-products",
        "runbook": "service-error-v1",
    },
    "adHighCpu": {
        "entity_type": "service",
        "entity_id": "ad",
        "mechanism": "resource_saturation",
        "sli": "frontend_ad_latency",
        "intended_workload": "browse-products",
        "runbook": "resource-saturation-v1",
    },
    "adManualGc": {
        "entity_type": "service",
        "entity_id": "ad",
        "mechanism": "garbage_collection_pause",
        "sli": "frontend_ad_latency",
        "intended_workload": "browse-products",
        "runbook": "resource-saturation-v1",
    },
    "cartFailure": {
        "entity_type": "service",
        "entity_id": "cart",
        "mechanism": "service_error",
        "sli": "cart_operation_success",
        "intended_workload": "add-and-empty-cart",
        "runbook": "service-error-v1",
    },
    "emailMemoryLeak": {
        "entity_type": "service",
        "entity_id": "email",
        "mechanism": "memory_leak",
        "sli": "checkout_confirmation_latency",
        "intended_workload": "repeated-checkout",
        "runbook": "resource-saturation-v1",
    },
    "failedReadinessProbe": {
        "entity_type": "instance",
        "entity_id": "cart",
        "mechanism": "readiness_failure",
        "sli": "cart_availability",
        "intended_workload": "cart-operations",
        "runbook": "service-error-v1",
        "platform": "kubernetes",
    },
    "imageSlowLoad": {
        "entity_type": "edge",
        "entity_id": "frontend-proxy->image-provider",
        "edge_source": "frontend-proxy",
        "edge_target": "image-provider",
        "mechanism": "dependency_latency",
        "sli": "page_render_latency",
        "intended_workload": "browse-products",
        "runbook": "dependency-unreachable-v1",
    },
    "intlShippingSlowdown": {
        "entity_type": "service",
        "entity_id": "shipping",
        "mechanism": "conditional_dependency_latency",
        "sli": "international_checkout_latency",
        "intended_workload": "international-checkout",
        "runbook": "service-error-v1",
    },
    "kafkaQueueProblems": {
        "entity_type": "queue",
        "entity_id": "kafka",
        "mechanism": "queue_lag",
        "sli": "order_processing_latency",
        "intended_workload": "repeated-checkout",
        "runbook": "queue-lag-v1",
        "required_profile": "full",
    },
    "loadGeneratorFloodHomepage": {
        "entity_type": "external",
        "entity_id": "workload",
        "mechanism": "workload_spike",
        "sli": "frontend_latency",
        "intended_workload": "homepage-flood",
        "runbook": "workload-spike-v1",
    },
    "paymentFailure": {
        "entity_type": "service",
        "entity_id": "payment",
        "edge_source": "checkout",
        "edge_target": "payment",
        "mechanism": "service_error",
        "sli": "checkout_success",
        "intended_workload": "repeated-checkout",
        "runbook": "service-error-v1",
    },
    "paymentUnreachable": {
        "entity_type": "edge",
        "entity_id": "checkout->payment",
        "edge_source": "checkout",
        "edge_target": "payment",
        "mechanism": "dependency_unreachable",
        "sli": "checkout_success",
        "intended_workload": "repeated-checkout",
        "runbook": "dependency-unreachable-v1",
    },
    "productCatalogFailure": {
        "entity_type": "service",
        "entity_id": "product-catalog",
        "mechanism": "conditional_service_error",
        "sli": "product_detail_availability",
        "intended_workload": "targeted-product-OLJCESPC7Z",
        "runbook": "service-error-v1",
    },
    "productCatalogLockContention": {
        "entity_type": "database",
        "entity_id": "astronomy-db",
        "edge_source": "product-catalog",
        "edge_target": "astronomy-db",
        "mechanism": "database_lock",
        "sli": "catalog_and_checkout_latency",
        "intended_workload": "browse-and-checkout",
        "runbook": "database-lock-contention-v1",
    },
    "recommendationCacheFailure": {
        "entity_type": "service",
        "entity_id": "recommendation",
        "mechanism": "cache_failure",
        "sli": "recommendation_latency",
        "intended_workload": "browse-products",
        "runbook": "service-error-v1",
    },
}


def load_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def atomic_write(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existing_mode = stat.S_IMODE(path.stat().st_mode) if path.exists() else None
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.", suffix=".tmp", dir=path.parent
    )
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(value, handle, indent=2, sort_keys=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if existing_mode is not None:
            os.chmod(temporary_name, existing_mode)
        os.replace(temporary_name, path)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def atomic_copy(source: Path, destination: Path) -> None:
    """Copy a file byte-for-byte and replace the destination atomically."""

    destination.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{destination.name}.", suffix=".tmp", dir=destination.parent
    )
    os.close(descriptor)
    try:
        shutil.copy2(source, temporary_name)
        os.replace(temporary_name, destination)
    except BaseException:
        try:
            os.unlink(temporary_name)
        except FileNotFoundError:
            pass
        raise


def snapshot_file(arguments: argparse.Namespace) -> None:
    if not arguments.file.is_file():
        raise SystemExit(f"Cannot snapshot missing file: {arguments.file}")
    atomic_copy(arguments.file, arguments.backup)
    if arguments.file.read_bytes() != arguments.backup.read_bytes():
        raise SystemExit(f"Byte verification failed for snapshot {arguments.backup}")


def restore_file(arguments: argparse.Namespace) -> None:
    if not arguments.backup.is_file():
        raise SystemExit(f"Cannot restore missing snapshot: {arguments.backup}")
    expected = arguments.backup.read_bytes()
    atomic_copy(arguments.backup, arguments.file)
    if arguments.file.read_bytes() != expected:
        raise SystemExit(f"Byte verification failed while restoring {arguments.file}")
    arguments.backup.unlink()


def validate_scenario_profile(scenario_name: str, profile: str) -> dict[str, Any]:
    if scenario_name not in SCENARIOS:
        available = ", ".join(sorted(SCENARIOS))
        raise SystemExit(
            f"No ground-truth mapping for {scenario_name!r}. Available: {available}"
        )
    scenario = SCENARIOS[scenario_name]
    if scenario.get("platform", "docker") != "docker":
        raise SystemExit(
            f"Scenario {scenario_name!r} is {scenario['platform']}-only and cannot "
            "produce valid ground truth with the Docker experiment runner"
        )
    required_profile = scenario.get("required_profile")
    if required_profile and profile != required_profile:
        raise SystemExit(
            f"Scenario {scenario_name!r} requires the {required_profile!r} demo "
            f"profile; the active profile is {profile!r}"
        )
    return scenario


def flag_entry(document: dict[str, Any], name: str) -> dict[str, Any]:
    flags = document.get("flags", {})
    if name not in flags:
        available = ", ".join(sorted(flags))
        raise SystemExit(f"Unknown feature flag {name!r}. Available: {available}")
    return flags[name]


def get_variant(arguments: argparse.Namespace) -> None:
    document = load_json(arguments.file)
    print(flag_entry(document, arguments.flag)["defaultVariant"])


def set_variant(arguments: argparse.Namespace) -> None:
    document = load_json(arguments.file)
    entry = flag_entry(document, arguments.flag)
    variants = entry.get("variants", {})
    if arguments.variant not in variants:
        available = ", ".join(sorted(variants))
        raise SystemExit(
            f"Invalid variant {arguments.variant!r} for {arguments.flag!r}. "
            f"Available: {available}"
        )
    entry["defaultVariant"] = arguments.variant
    atomic_write(arguments.file, document)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def request_json(
    method: str, url: str, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=(json.dumps(payload).encode("utf-8") if payload is not None else None),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        detail = error.read().decode("utf-8", errors="replace")
        raise SystemExit(
            f"Experiment API returned HTTP {error.code}: {detail}"
        ) from error
    except urllib.error.URLError as error:
        raise SystemExit(f"Could not reach experiment API at {url}: {error}") from error


def sync_api(arguments: argparse.Namespace) -> None:
    """Upsert run lineage and, once known, its fault and ground truth."""

    manifest = load_json(arguments.file)
    if manifest.get("reset_between_runs") is not True:
        raise SystemExit(
            "Refusing to register a capture whose reset_between_runs policy is not true"
        )
    if not manifest.get("reset_completed_at"):
        raise SystemExit(
            "Refusing to register a capture before the Demo reset has completed"
        )
    seed = manifest.get("seed")
    if (
        not isinstance(seed, int)
        or isinstance(seed, bool)
        or not 0 <= seed <= 2_147_483_647
    ):
        raise SystemExit("Capture seed must be an integer from 0 through 2147483647")
    scenario_name = str(manifest["scenario"])
    scenario = validate_scenario_profile(scenario_name, str(manifest["profile"]))
    run_id = str(UUID(str(manifest["experiment_id"])))
    base_url = arguments.api_url.rstrip("/")
    status = {
        "warming_up": "warming",
        "fault_active": "running",
        "recovering": "recovering",
        "completed": "completed",
        "aborted": "failed",
    }[manifest["status"]]
    run_payload = {
        "id": run_id,
        "name": f"otel-demo-{scenario_name}-{run_id[:8]}",
        "source_repository": "https://github.com/open-telemetry/opentelemetry-demo.git",
        "source_version": "3.1.0",
        "source_sha": manifest["system_commit"],
        "seed": seed,
        # The stock Demo load generator emits its weighted mix of commerce
        # paths. The scenario mapping below is an intended exercise, not proof
        # that the runner selected only that transaction path.
        "workload": ACTUAL_WORKLOAD,
        "split": (
            None
            if manifest.get("split", "unassigned") == "unassigned"
            else manifest["split"]
        ),
        "status": status,
        "started_at": manifest["started_at"],
        "ended_at": manifest.get("completed_at")
        if status in {"completed", "failed"}
        else None,
        "configuration": {
            "profile": manifest["profile"],
            "flag": scenario_name,
            "capture_seed": seed,
            "seed_semantics": "capture_partition_seed",
            "actual_workload": ACTUAL_WORKLOAD,
            "intended_scenario_exercise": scenario["intended_workload"],
            "upstream_workload_rng_controlled": False,
            "reset_between_runs": True,
            "reset_completed_at": manifest["reset_completed_at"],
            "variant": manifest["variant"],
            "original_variant": manifest["original_variant"],
            "warmup_seconds": manifest["warmup_seconds"],
            "fault_seconds": manifest["fault_seconds"],
            "recovery_seconds": manifest["recovery_seconds"],
        },
        "environment": {
            "collector": "rca-otel-gateway",
            "runner": "demo_experiment.py",
            "capture_seed": seed,
            "upstream_load_generator": UPSTREAM_LOAD_GENERATOR,
            "upstream_workload_rng": "uncontrolled",
            "upstream_workload_rng_note": UPSTREAM_WORKLOAD_RNG_NOTE,
        },
    }
    if status in {"completed", "failed"}:
        acceptance = manifest.get("acceptance") or {}
        accepted = status == "completed" and acceptance.get("passed") is True
        run_payload["configuration"].update(
            {
                "training_eligible": accepted,
                "capture_valid": accepted,
                "acceptance": acceptance,
            }
        )
        if not accepted:
            run_payload["configuration"]["failure_reason"] = manifest.get(
                "failure_reason", "capture_aborted_before_acceptance"
            )
    request_json("POST", f"{base_url}/api/v1/experiments", run_payload)

    # The injection interval is complete only after fault_ended_at is recorded.
    # Posting it once here avoids partial/duplicated ground-truth rows.
    if manifest.get("fault_started_at") and manifest.get("fault_ended_at"):
        fault_id = str(uuid5(UUID(run_id), f"{scenario_name}:{manifest['variant']}"))
        fault_payload = {
            "id": fault_id,
            "flag_name": scenario_name,
            "variant": str(manifest["variant"]),
            "injector": "flagd",
            "causal_entity_type": scenario["entity_type"],
            "causal_entity_id": scenario["entity_id"],
            "causal_edge_source": scenario.get("edge_source"),
            "causal_edge_target": scenario.get("edge_target"),
            "mechanism": scenario["mechanism"],
            "impacted_sli": scenario["sli"],
            "injected_at": manifest["fault_started_at"],
            "cleared_at": manifest["fault_ended_at"],
            "expected_runbook_key": scenario["runbook"],
            "metadata": {
                "manifest": str(arguments.file),
                "profile": manifest["profile"],
                "capture_seed": seed,
                "actual_workload": ACTUAL_WORKLOAD,
                "intended_scenario_exercise": scenario["intended_workload"],
                "upstream_workload_rng_controlled": False,
            },
        }
        request_json(
            "POST", f"{base_url}/api/v1/experiments/{run_id}/faults", fault_payload
        )
        label_payload = {
            "entity_type": (
                "dependency_edge"
                if scenario["entity_type"] == "edge"
                else scenario["entity_type"]
            ),
            "entity_id": scenario["entity_id"],
            "fault_class": scenario["mechanism"],
            "relevance": 3,
            "is_primary": True,
        }
        request_json(
            "POST", f"{base_url}/api/v1/experiments/{run_id}/labels", label_payload
        )
        # Some infrastructure faults are naturally valid at both the resource
        # and dependency-edge level. Preserve both labels so evaluation rewards
        # either useful debugging granularity without weakening exact matching.
        edge_source = scenario.get("edge_source")
        edge_target = scenario.get("edge_target")
        edge_id = (
            f"{edge_source}->{edge_target}" if edge_source and edge_target else None
        )
        if edge_id and edge_id != scenario["entity_id"]:
            request_json(
                "POST",
                f"{base_url}/api/v1/experiments/{run_id}/labels",
                {
                    "entity_type": "dependency_edge",
                    "entity_id": edge_id,
                    "fault_class": scenario["mechanism"],
                    "relevance": 3,
                    "is_primary": False,
                },
            )
    print(json.dumps({"run_id": run_id, "status": status, "api": base_url}))


def start_manifest(arguments: argparse.Namespace) -> None:
    experiment_id = str(UUID(str(arguments.experiment_id)))
    scenario = validate_scenario_profile(arguments.scenario, arguments.profile)
    if not 0 <= arguments.seed <= 2_147_483_647:
        raise SystemExit("Capture seed must be an integer from 0 through 2147483647")
    manifest = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "system_under_test": "open-telemetry/opentelemetry-demo",
        "system_commit": arguments.commit,
        "profile": arguments.profile,
        "seed": arguments.seed,
        "split": arguments.split,
        "seed_semantics": "capture_partition_seed",
        "scenario": arguments.scenario,
        "workload": {
            "actual": ACTUAL_WORKLOAD,
            "intended_scenario_exercise": scenario["intended_workload"],
        },
        "upstream_workload_rng": {
            "generator": UPSTREAM_LOAD_GENERATOR,
            "controlled": False,
            "note": UPSTREAM_WORKLOAD_RNG_NOTE,
        },
        "reset_between_runs": True,
        "reset_completed_at": None,
        "variant": arguments.variant,
        "original_variant": arguments.original_variant,
        "warmup_seconds": arguments.warmup,
        "fault_seconds": arguments.duration,
        "recovery_seconds": arguments.recovery,
        "status": "reset_pending",
        # The capture clock starts only after the full Demo reset and health
        # wait pass. This prevents reset telemetry from entering the warm-up.
        "started_at": None,
        "fault_started_at": None,
        "fault_ended_at": None,
        "completed_at": None,
        "acceptance": None,
        "training_eligible": False,
        "capture_valid": False,
    }
    atomic_write(arguments.file, manifest)


def mark_reset(arguments: argparse.Namespace) -> None:
    """Record that the required pre-capture Demo reset and health wait passed."""

    manifest = load_json(arguments.file)
    if manifest.get("status") != "reset_pending":
        raise SystemExit("The Demo reset can only be recorded while reset is pending")
    if manifest.get("reset_between_runs") is not True:
        raise SystemExit("Manifest does not require reset_between_runs")
    if manifest.get("reset_completed_at"):
        raise SystemExit("The Demo reset has already been recorded")
    reset_time = utc_now()
    manifest["reset_completed_at"] = reset_time
    manifest["started_at"] = reset_time
    manifest["status"] = "warming_up"
    atomic_write(arguments.file, manifest)


def mark_manifest(arguments: argparse.Namespace) -> None:
    manifest = load_json(arguments.file)
    current_status = manifest.get("status")
    expected_previous = {
        "fault_active": "warming_up",
        "recovering": "fault_active",
        "completed": "recovering",
    }
    required = expected_previous.get(arguments.status)
    if required and current_status != required:
        raise SystemExit(
            f"Cannot move manifest from {current_status!r} to {arguments.status!r}; "
            f"expected {required!r}"
        )
    if arguments.status == "completed" and not (
        (manifest.get("acceptance") or {}).get("passed") is True
    ):
        raise SystemExit(
            "Refusing to complete a capture that has not passed acceptance"
        )
    # A completion API call can fail before the runner knows whether it was
    # committed. Permit the cleanup path to downgrade its local manifest and
    # attempt a failed terminal write; the API's own terminal guard remains the
    # authority if completion actually committed.
    if current_status == "aborted" or (
        current_status == "completed" and arguments.status != "aborted"
    ):
        raise SystemExit(f"Manifest is already terminal ({current_status})")
    manifest["status"] = arguments.status
    timestamp_field = {
        "fault_active": "fault_started_at",
        "recovering": "fault_ended_at",
        "completed": "completed_at",
        "aborted": "completed_at",
    }.get(arguments.status)
    if timestamp_field:
        manifest[timestamp_field] = utc_now()
    if arguments.status == "completed":
        manifest["training_eligible"] = True
        manifest["capture_valid"] = True
    elif arguments.status == "aborted":
        manifest["training_eligible"] = False
        manifest["capture_valid"] = False
    atomic_write(arguments.file, manifest)


def _positive_count(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value > 0


def accept_capture(arguments: argparse.Namespace) -> None:
    """Fail closed unless a recovering run has trainable, persisted RCA output."""

    manifest = load_json(arguments.file)
    if manifest.get("status") != "recovering":
        raise SystemExit("Capture acceptance is only valid during recovery")
    if not 1 <= arguments.top_k <= 100:
        raise SystemExit("Acceptance top-k must be from 1 through 100")

    run_id = str(UUID(str(manifest["experiment_id"])))
    base_url = arguments.api_url.rstrip("/")
    summary_url = f"{base_url}/api/v1/experiments/{run_id}/capture-summary"
    try:
        summary = request_json("GET", summary_url)
    except SystemExit as error:
        failure = f"capture_summary_unavailable: {error}"
        manifest["acceptance"] = {
            "passed": False,
            "checked_at": utc_now(),
            "top_k": arguments.top_k,
            "reasons": [failure],
        }
        manifest["failure_reason"] = failure
        manifest["training_eligible"] = False
        manifest["capture_valid"] = False
        atomic_write(arguments.file, manifest)
        raise

    scenario = validate_scenario_profile(
        str(manifest["scenario"]), str(manifest["profile"])
    )
    expected_type = (
        "dependency_edge"
        if scenario["entity_type"] == "edge"
        else str(scenario["entity_type"])
    )
    expected_id = str(scenario["entity_id"])
    expected_candidates = [
        {"entity_type": expected_type, "entity_id": expected_id}
    ]
    edge_source = scenario.get("edge_source")
    edge_target = scenario.get("edge_target")
    edge_id = (
        f"{edge_source}->{edge_target}" if edge_source and edge_target else None
    )
    if edge_id and not any(
        item["entity_type"] == "dependency_edge" and item["entity_id"] == edge_id
        for item in expected_candidates
    ):
        # A known causal dependency is valid at a more actionable granularity
        # than its endpoint.  Persist both labels; acceptance succeeds when
        # either the injected entity or its explicitly declared edge is top-k.
        expected_candidates.append(
            {"entity_type": "dependency_edge", "entity_id": edge_id}
        )
    telemetry = summary.get("telemetry") or {}
    incidents = summary.get("incidents") or []
    candidates = summary.get("candidates") or []
    reasons: list[str] = []
    for signal_key in ("spans", "metric_points", "log_records"):
        if not _positive_count(telemetry.get(signal_key)):
            reasons.append(f"no persisted {signal_key} for run")
    if not incidents:
        reasons.append("no incident was produced for run")
    if not candidates:
        reasons.append("incident has no root-cause candidates")

    matched_candidate: dict[str, Any] | None = None
    for candidate in candidates:
        try:
            rank = int(candidate.get("rank"))
        except (TypeError, ValueError):
            continue
        identity = {
            "entity_type": str(candidate.get("entity_type")),
            "entity_id": str(candidate.get("entity_id")),
        }
        if rank <= arguments.top_k and identity in expected_candidates:
            matched_candidate = candidate
            break
    if candidates and matched_candidate is None:
        reasons.append(
            "expected causal candidates were not ranked in top "
            f"{arguments.top_k}: "
            + ", ".join(
                f"{item['entity_type']}:{item['entity_id']}"
                for item in expected_candidates
            )
        )

    passed = not reasons
    acceptance = {
        "passed": passed,
        "checked_at": utc_now(),
        "top_k": arguments.top_k,
        "expected_candidate": {
            "entity_type": expected_type,
            "entity_id": expected_id,
        },
        "expected_candidates": expected_candidates,
        "matched_candidate": matched_candidate,
        "telemetry": telemetry,
        "derived": summary.get("derived") or {},
        "incident_ids": [str(item.get("id")) for item in incidents],
        "candidate_count": len(candidates),
        "reasons": reasons,
    }
    manifest["acceptance"] = acceptance
    manifest["training_eligible"] = passed
    manifest["capture_valid"] = passed
    if reasons:
        manifest["failure_reason"] = "capture_acceptance_failed: " + "; ".join(reasons)
    else:
        manifest.pop("failure_reason", None)
    atomic_write(arguments.file, manifest)
    if reasons:
        raise SystemExit(manifest["failure_reason"])
    print(
        json.dumps(
            {
                "run_id": run_id,
                "accepted": True,
                "top_k": arguments.top_k,
                "matched_candidate": matched_candidate,
                "telemetry": telemetry,
            }
        )
    )


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)

    get_command = commands.add_parser("get-variant")
    get_command.add_argument("--file", type=Path, required=True)
    get_command.add_argument("--flag", required=True)
    get_command.set_defaults(function=get_variant)

    set_command = commands.add_parser("set-variant")
    set_command.add_argument("--file", type=Path, required=True)
    set_command.add_argument("--flag", required=True)
    set_command.add_argument("--variant", required=True)
    set_command.set_defaults(function=set_variant)

    snapshot_command = commands.add_parser(
        "snapshot-file", help="Create a verified byte-for-byte file snapshot"
    )
    snapshot_command.add_argument("--file", type=Path, required=True)
    snapshot_command.add_argument("--backup", type=Path, required=True)
    snapshot_command.set_defaults(function=snapshot_file)

    restore_command = commands.add_parser(
        "restore-file", help="Atomically restore and verify a file snapshot"
    )
    restore_command.add_argument("--file", type=Path, required=True)
    restore_command.add_argument("--backup", type=Path, required=True)
    restore_command.set_defaults(function=restore_file)

    start_command = commands.add_parser("start-manifest")
    start_command.add_argument("--file", type=Path, required=True)
    start_command.add_argument("--experiment-id", required=True)
    start_command.add_argument("--commit", required=True)
    start_command.add_argument("--profile", required=True)
    start_command.add_argument("--seed", type=int, required=True)
    start_command.add_argument(
        "--split",
        choices=("train", "validation", "test", "holdout", "unassigned"),
        default="unassigned",
    )
    start_command.add_argument("--scenario", required=True)
    start_command.add_argument("--variant", required=True)
    start_command.add_argument("--original-variant", required=True)
    start_command.add_argument("--warmup", type=int, required=True)
    start_command.add_argument("--duration", type=int, required=True)
    start_command.add_argument("--recovery", type=int, required=True)
    start_command.set_defaults(function=start_manifest)

    reset_command = commands.add_parser(
        "mark-reset", help="Record a successful full Demo restart and health wait"
    )
    reset_command.add_argument("--file", type=Path, required=True)
    reset_command.set_defaults(function=mark_reset)

    mark_command = commands.add_parser("mark-manifest")
    mark_command.add_argument("--file", type=Path, required=True)
    mark_command.add_argument(
        "--status",
        choices=("warming_up", "fault_active", "recovering", "completed", "aborted"),
        required=True,
    )
    mark_command.set_defaults(function=mark_manifest)

    accept_command = commands.add_parser(
        "accept-capture",
        help="Require persisted telemetry and a correctly ranked RCA result",
    )
    accept_command.add_argument("--file", type=Path, required=True)
    accept_command.add_argument("--api-url", default="http://localhost:8000")
    accept_command.add_argument("--top-k", type=int, default=3)
    accept_command.set_defaults(function=accept_capture)

    sync_command = commands.add_parser(
        "sync-api", help="Persist run lineage, the fault interval, and ground truth"
    )
    sync_command.add_argument("--file", type=Path, required=True)
    sync_command.add_argument("--api-url", default="http://localhost:8000")
    sync_command.set_defaults(function=sync_api)
    return root


def main() -> None:
    arguments = parser().parse_args()
    arguments.function(arguments)


if __name__ == "__main__":
    main()
