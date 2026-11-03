#!/usr/bin/env python3
"""Run an end-to-end RCA smoke test without the external demo stack."""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rca.synthetic import SCENARIOS, generate_run, post_run  # noqa: E402


def request_json(url: str, *, timeout: float = 5.0) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def send_json(
    method: str, url: str, payload: dict[str, Any], *, timeout: float = 10.0
) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method=method,
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def wait_ready(base_url: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error = "not attempted"
    while time.monotonic() < deadline:
        try:
            response = request_json(f"{base_url}/health/ready")
            if response.get("status") in {"ready", "ok"}:
                return
            last_error = json.dumps(response)
        except (OSError, urllib.error.URLError, ValueError) as error:
            last_error = str(error)
        time.sleep(1)
    raise RuntimeError(
        f"RCA API did not become ready at {base_url} within {timeout:.0f}s: {last_error}. "
        "Start it with `docker compose up -d --build`."
    )


def incidents(base_url: str) -> list[dict[str, Any]]:
    result = request_json(f"{base_url}/api/v1/incidents?limit=100")
    items = result.get("items", result if isinstance(result, list) else [])
    return list(items)


def incident_for_run(
    items: list[dict[str, Any]], run_id: str, previous: set[str]
) -> dict[str, Any] | None:
    for incident in items:
        metadata = incident.get("metadata") or {}
        if str(metadata.get("run_id")) == run_id:
            return incident
    for incident in items:
        if str(incident.get("id")) not in previous:
            return incident
    return None


def register_experiment(run: Any, scenario: str, seed: int, base_url: str) -> None:
    """Register reproducible run, fault interval, and evaluation ground truth."""

    truth = run.ground_truth
    send_json(
        "POST",
        f"{base_url}/api/v1/experiments",
        {
            "id": run.run_id,
            "name": f"synthetic-smoke-{scenario}",
            "source_repository": "local/rca.synthetic",
            "source_version": "1",
            "source_sha": "deterministic",
            "seed": seed,
            "workload": "synthetic",
            "status": "running",
            "started_at": run.started_at.isoformat(),
            "configuration": {"scenario": scenario, "severity": truth["severity"]},
            "environment": {"runner": "scripts/synthetic_smoke.py"},
        },
    )
    causal_entity = truth.get("causal_entity")
    if not causal_entity:
        return
    entity_type = "dependency_edge" if "->" in causal_entity else (
        "external" if causal_entity == "external_workload" else "service"
    )
    edge_source, edge_target = (causal_entity.split("->", 1) if "->" in causal_entity else (None, None))
    mechanism = str(truth["failure_mechanism"])
    canonical_fault_class = {
        "service_errors": "service_error",
        "service_latency": "service_error",
        "database_lock_contention": "database_lock",
        "queue_backlog": "queue_lag",
        "cpu_saturation": "resource_saturation",
        "memory_leak": "resource_saturation",
        "traffic_flood": "workload_spike",
    }.get(mechanism, mechanism)
    runbook = {
        "database_lock_contention": "database-lock-contention-v1",
        "queue_backlog": "queue-lag-v1",
        "cpu_saturation": "resource-saturation-v1",
        "memory_leak": "resource-saturation-v1",
        "traffic_flood": "workload-spike-v1",
    }.get(mechanism, "service-error-v1")
    send_json(
        "POST",
        f"{base_url}/api/v1/experiments/{run.run_id}/faults",
        {
            "flag_name": scenario,
            "variant": str(truth["severity"]),
            "injector": "rca.synthetic",
            "causal_entity_type": entity_type,
            "causal_entity_id": causal_entity,
            "causal_edge_source": edge_source,
            "causal_edge_target": edge_target,
            "mechanism": mechanism,
            "impacted_sli": truth["impacted_sli"],
            "injected_at": truth["injected_at"],
            "cleared_at": truth["recovery_at"],
            "expected_runbook_key": runbook,
            "metadata": {"seed": seed, "synthetic": True},
        },
    )
    send_json(
        "POST",
        f"{base_url}/api/v1/experiments/{run.run_id}/labels",
        {
            "entity_type": entity_type,
            "entity_id": causal_entity,
            "fault_class": canonical_fault_class,
            "relevance": 3,
            "is_primary": True,
        },
    )


def finish_experiment(
    run_id: str, base_url: str, status: str, error: BaseException | None = None
) -> None:
    payload: dict[str, Any] = {
        "status": status,
        "ended_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
    }
    if error is not None:
        payload["environment"] = {
            "smoke_failure": {
                "type": type(error).__name__,
                "message": str(error)[:2000],
            }
        }
    send_json(
        "PATCH",
        f"{base_url}/api/v1/experiments/{run_id}",
        payload,
    )


@contextmanager
def experiment_lifecycle(
    run: Any, scenario: str, seed: int, base_url: str
) -> Any:
    """Finish every registered synthetic run, without masking its real failure."""

    registration_started = False
    try:
        # The initial run POST is the first operation inside register_experiment.
        # Mark the lifecycle active first so a later fault/label POST failure also
        # closes the already-created run.
        registration_started = True
        register_experiment(run, scenario, seed, base_url)
        yield
        finish_experiment(run.run_id, base_url, "completed")
        registration_started = False
    except BaseException as error:
        if registration_started:
            try:
                finish_experiment(run.run_id, base_url, "failed", error)
            except BaseException as cleanup_error:
                error.add_note(
                    "Additionally failed to mark synthetic experiment "
                    f"{run.run_id} failed: {cleanup_error}"
                )
        raise


def database_scalar(sql: str) -> int:
    command = [
        "docker",
        "compose",
        "--file",
        str(PROJECT_ROOT / "docker-compose.yml"),
        "exec",
        "-T",
        "rca-db",
        "sh",
        "-c",
        'psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --tuples-only --no-align -v ON_ERROR_STOP=1',
    ]
    result = subprocess.run(
        command,
        input=sql,
        text=True,
        capture_output=True,
        check=False,
        cwd=PROJECT_ROOT,
    )
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"Database acceptance query failed: {detail}")
    try:
        return int(result.stdout.strip().splitlines()[-1])
    except (IndexError, ValueError) as error:
        raise RuntimeError(f"Unexpected database acceptance result: {result.stdout!r}") from error


def canonical_entity(value: Any) -> str:
    return str(value or "").strip().lower().replace("→", "->")


def write_manifest(
    path: Path, run: Any, incident: dict[str, Any], candidates: list[dict[str, Any]]
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "completed_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "ground_truth": run.ground_truth,
        "incident": incident,
        "candidates": candidates,
    }
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(payload, indent=2, default=str) + "\n", encoding="utf-8"
    )
    temporary.replace(path)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--api-url", default="http://localhost:8000")
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="database_lock")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--severity", type=float, default=1.25)
    parser.add_argument("--ready-timeout", type=float, default=60)
    parser.add_argument("--incident-timeout", type=float, default=120)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument(
        "--output-directory",
        type=Path,
        default=PROJECT_ROOT / ".runtime" / "smoke",
    )
    arguments = parser.parse_args()
    base_url = arguments.api_url.rstrip("/")

    print(f"Waiting for RCA API readiness at {base_url} ...", flush=True)
    wait_ready(base_url, arguments.ready_timeout)
    before = {str(item.get("id")) for item in incidents(base_url)}

    run = generate_run(
        arguments.scenario, seed=arguments.seed, severity=arguments.severity
    )
    with experiment_lifecycle(run, arguments.scenario, arguments.seed, base_url):
        print(
            f"Posting {len(run.events)} deterministic events for {arguments.scenario} "
            f"(run {run.run_id}) ...",
            flush=True,
        )
        post_run(
            run,
            f"{base_url}/api/v1/events/batch",
            batch_size=arguments.batch_size,
        )

        deadline = time.monotonic() + arguments.incident_timeout
        incident: dict[str, Any] | None = None
        while time.monotonic() < deadline:
            incident = incident_for_run(incidents(base_url), run.run_id, before)
            if incident is not None:
                break
            time.sleep(2)
        if incident is None:
            raise RuntimeError(
                f"No incident was produced for run {run.run_id} within "
                f"{arguments.incident_timeout:.0f}s. "
                "Inspect `docker compose logs rca-worker`."
            )

        incident_id = str(incident["id"])
        candidate_response = request_json(
            f"{base_url}/api/v1/incidents/"
            f"{urllib.parse.quote(incident_id)}/candidates"
        )
        candidates = list(candidate_response.get("items", []))
        if not candidates:
            raise RuntimeError(f"Incident {incident_id} has no root-cause candidates")

        expected = canonical_entity(run.ground_truth.get("causal_entity"))
        top_three = [
            canonical_entity(candidate.get("entity_id")) for candidate in candidates[:3]
        ]
        if expected and expected not in top_three:
            raise RuntimeError(
                f"Expected causal entity {expected!r} was not in the top three: "
                f"{top_three}"
            )

        evidence_response = request_json(
            f"{base_url}/api/v1/incidents/{urllib.parse.quote(incident_id)}/evidence"
        )
        evidence = list(evidence_response.get("items", []))
        if not evidence:
            raise RuntimeError(f"Incident {incident_id} has no supporting evidence")

        graph_response = request_json(
            f"{base_url}/api/v1/incidents/{urllib.parse.quote(incident_id)}/graph"
        )
        graph_edges = list(graph_response.get("edges", []))
        if not graph_edges:
            raise RuntimeError(
                f"Incident {incident_id} has no incident-scoped dependency graph"
            )

        escaped_incident_id = incident_id.replace("'", "''")
        recommendation_count = database_scalar(
            "SELECT count(*) FROM recommendations "
            f"WHERE incident_id = '{escaped_incident_id}'::uuid;\n"
        )
        runbook_count = database_scalar("SELECT count(*) FROM runbooks WHERE enabled;\n")
        if recommendation_count < 1 or runbook_count < 1:
            raise RuntimeError(
                "Incident guidance is incomplete: "
                f"recommendations={recommendation_count}, "
                f"enabled_runbooks={runbook_count}"
            )

        top = candidates[0]
        manifest = arguments.output_directory / f"{run.run_id}.json"
        write_manifest(manifest, run, incident, candidates)
    print(
        json.dumps(
            {
                "status": "passed",
                "run_id": run.run_id,
                "incident_id": incident_id,
                "scenario": arguments.scenario,
                "expected_cause": expected,
                "top_candidate": top.get("entity_id"),
                "top_probability": top.get("probability"),
                "candidate_count": len(candidates),
                "evidence_count": len(evidence),
                "graph_edge_count": len(graph_edges),
                "recommendation_count": recommendation_count,
                "enabled_runbook_count": runbook_count,
                "manifest": str(manifest),
            },
            indent=2,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (RuntimeError, urllib.error.URLError, urllib.error.HTTPError) as error:
        print(f"Smoke test failed: {error}", file=sys.stderr)
        raise SystemExit(1) from error
