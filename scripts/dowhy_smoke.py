#!/usr/bin/env python3
"""Exercise the real DoWhy library, optionally through the running product stack.

The deterministic fixture is a synchronous frontend -> checkout -> payment path.
Payment's intrinsic latency changes after 80 healthy buckets. This is an
integration check, not a population accuracy estimate or a real-world dataset.
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from datetime import UTC, datetime, timedelta
from itertools import groupby
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, uuid4, uuid5

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from rca.dowhy_engine import DoWhyRootCauseRanker  # noqa: E402
from rca.features import StreamingAnomalyScorer, aggregate_feature_buckets  # noqa: E402
from rca.graph import DirectedDependencyGraph, build_dependency_edges  # noqa: E402
from rca.incidents import IncidentDetector  # noqa: E402
from rca.synthetic import SyntheticRun, post_run  # noqa: E402
from scripts.synthetic_smoke import (  # noqa: E402
    experiment_lifecycle,
    request_json,
    wait_ready,
)


def iso(value: datetime) -> str:
    return value.isoformat().replace("+00:00", "Z")


def latency_fixture(seed: int = 42) -> SyntheticRun:
    rng = random.Random(seed)
    run_id = str(uuid4())
    start = datetime.now(UTC).replace(microsecond=0)
    start -= timedelta(seconds=start.second % 10 + 910)
    injected = start + timedelta(seconds=800)
    recovery = injected + timedelta(seconds=60)
    events: list[dict[str, Any]] = []
    for index in range(90):
        when = start + timedelta(seconds=index * 10)
        payment = 25.0 + rng.gauss(0, 2.0) + (120.0 if 80 <= index < 86 else 0)
        checkout = 12.0 + 1.1 * payment + rng.gauss(0, 1.0)
        frontend = 8.0 + 1.05 * checkout + rng.gauss(0, 1.0)
        trace_id = uuid5(NAMESPACE_URL, f"{run_id}:{index}").hex
        parent = None
        for hop, (service, duration) in enumerate(
            (("frontend", frontend), ("checkout", checkout), ("payment", payment))
        ):
            span_id = uuid5(NAMESPACE_URL, f"{trace_id}:{service}").hex[:16]
            began = when + timedelta(milliseconds=hop)
            attrs: dict[str, Any] = {"rpc.system": "grpc"}
            if hop < 2:
                attrs["peer.service"] = ("checkout", "payment")[hop]
            events.append(
                {
                    "signal": "span",
                    "service_name": service,
                    "trace_id": trace_id,
                    "span_id": span_id,
                    "parent_span_id": parent,
                    "operation_name": f"{service}.request",
                    "span_kind": "SERVER",
                    "start_time": iso(began),
                    "end_time": iso(began + timedelta(milliseconds=duration)),
                    "status_code": "OK",
                    "attributes": attrs,
                    "resource_attributes": {"service.name": service},
                }
            )
            events.append(
                {
                    "signal": "metric",
                    "service_name": service,
                    "metric_name": "process.cpu.utilization",
                    "metric_type": "gauge",
                    "time_unix": iso(when + timedelta(seconds=1)),
                    "value": 0.2,
                    "attributes": {},
                    "resource_attributes": {"service.name": service},
                }
            )
            events.append(
                {
                    "signal": "log",
                    "service_name": service,
                    "time_unix": iso(when + timedelta(seconds=2)),
                    "severity_number": 9,
                    "severity_text": "INFO",
                    "body": "latency integration fixture",
                    "attributes": {},
                    "resource_attributes": {"service.name": service},
                }
            )
            parent = span_id
    truth = {
        "run_id": run_id,
        "causal_entity": "payment",
        "severity": 1.0,
        "failure_mechanism": "service_latency",
        "injected_at": iso(injected),
        "recovery_at": iso(recovery),
        "impacted_sli": "request_latency",
        "seed": seed,
        "synthetic": True,
    }
    return SyntheticRun(
        run_id, "dowhy_latency", start, injected, recovery, tuple(events), truth
    )


def assert_dowhy_result(analysis: dict, candidates: list[dict]) -> None:
    if analysis.get("status") != "analyzed":
        raise RuntimeError(f"Real DoWhy did not execute: {analysis}")
    if not candidates or candidates[0].get("entity_id") != "payment":
        raise RuntimeError(f"Expected payment to rank first: {candidates[:3]}")
    if any(
        not str(item.get("model_version", "")).startswith("dowhy-gcm-")
        for item in candidates
    ):
        raise RuntimeError("A fallback result was presented as DoWhy output")
    if any(item.get("probability") is not None for item in candidates):
        raise RuntimeError("Attribution scores must not be presented as probabilities")


def run_offline(run: SyntheticRun) -> dict[str, Any]:
    signals = {
        name: [
            {**event, "run_id": run.run_id}
            for event in run.events
            if event["signal"] == name
        ]
        for name in ("span", "metric", "log")
    }
    buckets = aggregate_feature_buckets(
        signals["span"], signals["metric"], signals["log"]
    )
    edges = build_dependency_edges(signals["span"])
    scorer = StreamingAnomalyScorer(min_history=10)
    detector = IncidentDetector()
    ranker = DoWhyRootCauseRanker()
    for when, grouped in groupby(buckets, key=lambda bucket: bucket.bucket_start):
        results = [scorer.score(bucket) for bucket in grouped]
        ranker.observe(results)
        for event in detector.update(results):
            if event.action != "opened" or when < run.injected_at:
                continue
            graph = DirectedDependencyGraph(
                edge for edge in edges if edge.bucket_start <= when
            )
            decision = ranker.rank(event.incident, results, graph)
            candidates = [
                {
                    "entity_id": item.entity_id,
                    "score": item.score,
                    "probability": item.probability,
                    "model_version": item.model_version,
                    "evidence_count": len(item.evidence),
                }
                for item in decision.actionable_rankings
            ]
            analysis = event.incident.metadata.get("analysis") or {}
            assert_dowhy_result(analysis, candidates)
            return {"mode": "offline", "analysis": analysis, "candidates": candidates}
    raise RuntimeError("The fixture did not produce a detectable latency incident")


def run_live(
    run: SyntheticRun, base_url: str, timeout: float, seed: int
) -> dict[str, Any]:
    wait_ready(base_url, 60)
    with experiment_lifecycle(run, "dowhy_latency", seed, base_url):
        # One transaction prevents deliberately historical buckets being advanced
        # before all the baseline data has arrived.
        post_run(run, f"{base_url}/api/v1/events/batch", batch_size=len(run.events))
        deadline = time.monotonic() + timeout
        last_analysis: dict[str, Any] = {}
        while time.monotonic() < deadline:
            items = request_json(f"{base_url}/api/v1/incidents?limit=100").get(
                "items", []
            )
            matching = [item for item in items if str(item.get("run_id")) == run.run_id]
            for incident in matching:
                last_analysis = (incident.get("metadata") or {}).get("analysis") or {}
                if last_analysis.get("status") != "analyzed":
                    continue
                incident_url = f"{base_url}/api/v1/incidents/{incident['id']}"
                candidates = request_json(f"{incident_url}/candidates").get("items", [])
                assert_dowhy_result(last_analysis, candidates)
                evidence = request_json(f"{incident_url}/evidence").get("items", [])
                graph = request_json(f"{incident_url}/graph").get("edges", [])
                guidance = request_json(f"{incident_url}/recommendations").get(
                    "items", []
                )
                if not evidence or not graph or not guidance:
                    raise RuntimeError(
                        "Diagnosis lacks evidence, dependency graph, or guidance"
                    )
                return {
                    "mode": "live",
                    "incident_id": incident["id"],
                    "analysis": last_analysis,
                    "candidates": candidates,
                    "evidence_count": len(evidence),
                    "graph_edge_count": len(graph),
                    "recommendation_count": len(guidance),
                }
            time.sleep(2)
        raise RuntimeError(
            f"No accepted DoWhy diagnosis within {timeout}s; latest={last_analysis}"
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--api-url", help="Exercise running API/worker instead of offline replay"
    )
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument(
        "--output", type=Path, default=PROJECT_ROOT / ".runtime" / "dowhy-smoke.json"
    )
    args = parser.parse_args()
    began = time.monotonic()
    run = latency_fixture(args.seed)
    result = (
        run_live(run, args.api_url.rstrip("/"), args.timeout, args.seed)
        if args.api_url
        else run_offline(run)
    )
    report = {
        "status": "passed",
        "run_id": run.run_id,
        "fixture": "synthetic_synchronous_payment_latency",
        "expected_cause": "payment",
        "elapsed_seconds": round(time.monotonic() - began, 3),
        **result,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
