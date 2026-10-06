"""Deterministic synthetic telemetry and fault-scenario generator.

This is useful for CI and first-run demonstrations.  It speaks the same normalized
batch contract as the ingestion backend, but does not replace experiments captured
from the pinned OpenTelemetry Demo.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from uuid import NAMESPACE_URL, uuid4, uuid5


ROUTES: tuple[tuple[str, ...], ...] = (
    ("frontend", "checkout", "payment"),
    ("frontend", "checkout", "productcatalog", "postgres"),
    ("frontend", "checkout", "kafka", "accounting"),
    ("frontend", "recommendation", "productcatalog", "postgres"),
)

SCENARIOS = {
    "healthy": {"entity": None, "mechanism": "no_fault"},
    "healthy_high_load": {"entity": None, "mechanism": "no_fault"},
    "payment_failure": {"entity": "payment", "mechanism": "service_errors"},
    "catalog_latency": {"entity": "productcatalog", "mechanism": "service_latency"},
    "database_lock": {"entity": "productcatalog->postgres", "mechanism": "database_lock_contention"},
    "kafka_backlog": {"entity": "checkout->kafka", "mechanism": "queue_backlog"},
    "checkout_cpu": {"entity": "checkout", "mechanism": "cpu_saturation"},
    "recommendation_memory": {"entity": "recommendation", "mechanism": "memory_leak"},
    "traffic_flood": {"entity": "external_workload", "mechanism": "traffic_flood"},
}


@dataclass(frozen=True)
class SyntheticRun:
    run_id: str
    scenario: str
    started_at: datetime
    injected_at: datetime
    recovery_at: datetime
    events: tuple[dict[str, Any], ...]
    ground_truth: dict[str, Any]

    def batches(self, size: int = 500) -> Iterable[dict[str, Any]]:
        for offset in range(0, len(self.events), size):
            yield {"run_id": self.run_id, "events": list(self.events[offset : offset + size])}


def _hex_id(rng: random.Random, width: int) -> str:
    return f"{rng.getrandbits(width * 4):0{width}x}"


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def generate_run(
    scenario: str,
    *,
    seed: int = 42,
    start: datetime | None = None,
    bucket_seconds: int = 10,
    warmup_buckets: int = 12,
    fault_buckets: int = 12,
    recovery_buckets: int = 6,
    requests_per_route: int = 3,
    severity: float = 1.0,
    run_id: str | None = None,
) -> SyntheticRun:
    if scenario not in SCENARIOS:
        raise ValueError(f"unknown scenario {scenario!r}; choose from {sorted(SCENARIOS)}")
    if min(bucket_seconds, warmup_buckets, fault_buckets, recovery_buckets, requests_per_route) <= 0:
        raise ValueError("durations and request count must be positive")
    rng = random.Random(seed)
    total_buckets = warmup_buckets + fault_buckets + recovery_buckets
    # By default create a completed historical run so the worker can process it
    # immediately rather than waiting for future synthetic timestamps to elapse.
    started = start or (
        datetime.now(timezone.utc).replace(microsecond=0)
        - timedelta(seconds=(total_buckets + 1) * bucket_seconds)
    )
    if started.tzinfo is None:
        started = started.replace(tzinfo=timezone.utc)
    identity = run_id or str(uuid5(NAMESPACE_URL, f"rca:{scenario}:{seed}:{started.isoformat()}"))
    injected = started + timedelta(seconds=warmup_buckets * bucket_seconds)
    recovery = injected + timedelta(seconds=fault_buckets * bucket_seconds)
    events: list[dict[str, Any]] = []

    for bucket_index in range(total_buckets):
        bucket_time = started + timedelta(seconds=bucket_index * bucket_seconds)
        fault_active = warmup_buckets <= bucket_index < warmup_buckets + fault_buckets
        fault_progress = (
            (bucket_index - warmup_buckets + 1) / fault_buckets if fault_active else 0.0
        )
        load_multiplier = 3 if scenario == "healthy_high_load" else 1
        if scenario == "traffic_flood" and fault_active:
            load_multiplier = max(3, round(5 * severity))
        for route_index, route in enumerate(ROUTES):
            request_count = requests_per_route * load_multiplier
            for request_index in range(request_count):
                # Keep the random draw in the sequence so scenario noise remains
                # reproducible, but namespace telemetry IDs by the run.  OTLP span
                # identity is globally (trace_id, span_id); reusing a seed for a
                # later experiment must not silently deduplicate every span into
                # the earlier run.
                trace_entropy = _hex_id(rng, 32)
                trace_id = uuid5(
                    NAMESPACE_URL, f"rca:trace:{identity}:{trace_entropy}"
                ).hex
                parent_span_id = ""
                propagated_delay = 0.0
                propagated_error = False
                # Work backward to determine downstream impact, then emit in route order.
                per_service: list[tuple[float, bool]] = []
                for service in reversed(route):
                    own_delay, own_error = _fault_effect(
                        scenario, service, fault_active, fault_progress, severity, rng
                    )
                    propagated_delay = own_delay + propagated_delay * 0.85
                    propagated_error = own_error or (propagated_error and rng.random() < 0.75)
                    per_service.append((propagated_delay, propagated_error))
                per_service.reverse()
                for hop, (service, (extra_delay, is_error)) in enumerate(zip(route, per_service)):
                    span_entropy = _hex_id(rng, 16)
                    span_id = uuid5(
                        NAMESPACE_URL,
                        f"rca:span:{identity}:{trace_id}:{hop}:{span_entropy}",
                    ).hex[:16]
                    base_latency = 8.0 + hop * 3.0 + rng.uniform(0, 4)
                    duration = max(0.2, base_latency + extra_delay)
                    span_start = bucket_time + timedelta(milliseconds=request_index * 20 + hop)
                    span_end = span_start + timedelta(milliseconds=duration)
                    attrs: dict[str, Any] = {
                        "http.request.method": "POST" if "checkout" in route else "GET",
                        "http.response.status_code": 503 if is_error else 200,
                        "synthetic.route": route_index,
                    }
                    if hop + 1 < len(route):
                        attrs["peer.service"] = route[hop + 1]
                        if route[hop + 1] == "postgres":
                            attrs["db.system"] = "postgresql"
                        if route[hop + 1] == "kafka":
                            attrs["messaging.system"] = "kafka"
                    events.append(
                        {
                            "signal": "span",
                            "trace_id": trace_id,
                            "span_id": span_id,
                            "parent_span_id": parent_span_id or None,
                            "service_name": service,
                            "operation_name": f"{service}.request",
                            "span_kind": "SERVER",
                            "start_time": _iso(span_start),
                            "end_time": _iso(span_end),
                            "status_code": "ERROR" if is_error else "OK",
                            "attributes": attrs,
                            "resource_attributes": {"service.name": service, "telemetry.sdk.language": "synthetic"},
                        }
                    )
                    parent_span_id = span_id

        services = sorted({service for route in ROUTES for service in route})
        for service in services:
            cpu = 18.0 + rng.uniform(-3, 3)
            memory = 120_000_000 + rng.uniform(-5_000_000, 5_000_000)
            if fault_active and scenario == "checkout_cpu" and service == "checkout":
                cpu = min(100.0, 80.0 + 15.0 * severity + rng.uniform(-2, 2))
            if fault_active and scenario == "recommendation_memory" and service == "recommendation":
                memory += 900_000_000 * severity * fault_progress
            metric_time = _iso(bucket_time + timedelta(seconds=1))
            events.extend(
                [
                    {
                        "signal": "metric",
                        "service_name": service,
                        "metric_name": "process.cpu.utilization",
                        "metric_type": "gauge",
                        "time_unix": metric_time,
                        "value": cpu / 100.0,
                        "attributes": {},
                        "resource_attributes": {"service.name": service},
                    },
                    {
                        "signal": "metric",
                        "service_name": service,
                        "metric_name": "process.memory.usage.bytes",
                        "metric_type": "gauge",
                        "time_unix": metric_time,
                        "value": memory,
                        "attributes": {},
                        "resource_attributes": {"service.name": service},
                    },
                ]
            )
        if fault_active and scenario == "kafka_backlog":
            events.append(
                {
                    "signal": "metric",
                    "service_name": "kafka",
                    "metric_name": "messaging.queue.lag",
                    "metric_type": "gauge",
                    "time_unix": _iso(bucket_time + timedelta(seconds=1)),
                    "value": 5000 * severity * fault_progress,
                    "attributes": {"messaging.system": "kafka"},
                    "resource_attributes": {"service.name": "kafka"},
                }
            )
        target = SCENARIOS[scenario]["entity"]
        log_service = (target or "frontend").split("->")[-1]
        events.append(
            {
                "signal": "log",
                "service_name": log_service,
                "time_unix": _iso(bucket_time + timedelta(seconds=2)),
                "observed_time": _iso(bucket_time + timedelta(seconds=2, milliseconds=5)),
                "severity_number": 17 if fault_active and target else 9,
                "severity_text": "ERROR" if fault_active and target else "INFO",
                "body": (
                    f"synthetic {SCENARIOS[scenario]['mechanism']} symptom"
                    if fault_active and target
                    else "synthetic health heartbeat"
                ),
                "attributes": {"synthetic": True},
                "resource_attributes": {"service.name": log_service},
            }
        )

    truth = {
        "run_id": identity,
        "scenario": scenario,
        "injector": "rca.synthetic",
        "causal_entity": SCENARIOS[scenario]["entity"],
        "failure_mechanism": SCENARIOS[scenario]["mechanism"],
        "impacted_sli": "request_success_and_latency",
        "injected_at": _iso(injected),
        "recovery_at": _iso(recovery),
        "seed": seed,
        "severity": severity,
    }
    return SyntheticRun(identity, scenario, started, injected, recovery, tuple(events), truth)


def _fault_effect(
    scenario: str,
    service: str,
    active: bool,
    progress: float,
    severity: float,
    rng: random.Random,
) -> tuple[float, bool]:
    if not active:
        return 0.0, False
    delay = 0.0
    error = False
    if scenario == "payment_failure" and service == "payment":
        delay, error = 120 * severity, rng.random() < min(1.0, 0.8 * severity)
    elif scenario == "catalog_latency" and service == "productcatalog":
        delay = 600 * severity
    elif scenario == "database_lock" and service == "postgres":
        delay = 1400 * severity
        error = rng.random() < min(0.8, 0.2 * severity)
    elif scenario == "kafka_backlog" and service in {"kafka", "accounting"}:
        delay = 300 * severity * progress
    elif scenario == "checkout_cpu" and service == "checkout":
        delay = 350 * severity
        error = rng.random() < min(0.5, 0.1 * severity)
    elif scenario == "recommendation_memory" and service == "recommendation":
        delay = 500 * severity * progress
        error = progress > 0.7 and rng.random() < 0.3
    elif scenario == "traffic_flood":
        delay = 40 * severity
    return delay, error


def post_run(run: SyntheticRun, url: str, *, batch_size: int = 500, timeout: float = 30.0) -> None:
    for batch in run.batches(batch_size):
        body = json.dumps(batch).encode("utf-8")
        request = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                if response.status >= 300:
                    raise RuntimeError(f"ingestion returned HTTP {response.status}")
        except urllib.error.HTTPError as error:
            detail = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"ingestion returned HTTP {error.code}: {detail}") from error


def register_run(run: SyntheticRun, database_url: str) -> None:
    """Register experiment metadata before posting events with a foreign-key run ID."""

    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover - deployment dependency
        raise RuntimeError("run registration requires psycopg; install rca/requirements.txt") from exc
    truth = run.ground_truth
    entity = truth.get("causal_entity")
    expected_runbook = {
        "database_lock_contention": "database-lock-contention-v1",
        "queue_backlog": "queue-lag-v1",
        "cpu_saturation": "resource-saturation-v1",
        "memory_leak": "resource-saturation-v1",
        "traffic_flood": "workload-spike-v1",
        "service_errors": "service-error-v1",
        "service_latency": "service-error-v1",
    }.get(str(truth["failure_mechanism"]))
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """INSERT INTO experiment_runs
                   (id,name,source_repository,source_version,seed,workload,status,started_at,ended_at,configuration,environment)
                   VALUES (%s,%s,%s,%s,%s,%s,'completed',%s,%s,%s::jsonb,%s::jsonb)
                   ON CONFLICT (id) DO UPDATE SET status='completed', ended_at=EXCLUDED.ended_at""",
                (
                    run.run_id,
                    f"synthetic-{run.scenario}-seed-{truth['seed']}",
                    "internal://rca.synthetic",
                    "v1",
                    truth["seed"],
                    "deterministic-synthetic",
                    run.started_at,
                    run.recovery_at,
                    json.dumps({"scenario": run.scenario, "severity": truth["severity"]}),
                    json.dumps({"generator": "rca.synthetic"}),
                ),
            )
            if entity:
                edge = str(entity).split("->", 1)
                entity_type = "dependency_edge" if len(edge) == 2 else "service"
                cursor.execute(
                    """INSERT INTO fault_injections
                       (id,run_id,flag_name,variant,injector,causal_entity_type,causal_entity_id,
                        causal_edge_source,causal_edge_target,mechanism,impacted_sli,injected_at,
                        cleared_at,expected_runbook_key,metadata)
                       VALUES (%s,%s,%s,'on','rca.synthetic',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                       ON CONFLICT (id) DO NOTHING""",
                    (
                        str(uuid5(NAMESPACE_URL, f"rca:fault:{run.run_id}")),
                        run.run_id,
                        run.scenario,
                        entity_type,
                        entity,
                        edge[0] if len(edge) == 2 else None,
                        edge[1] if len(edge) == 2 else None,
                        truth["failure_mechanism"],
                        truth["impacted_sli"],
                        run.injected_at,
                        run.recovery_at,
                        expected_runbook,
                        json.dumps({"seed": truth["seed"], "severity": truth["severity"]}),
                    ),
                )
        connection.commit()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scenario", choices=sorted(SCENARIOS), default="database_lock")
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--severity", type=float, default=1.0)
    parser.add_argument("--output", help="Write normalized batch JSON here")
    parser.add_argument("--post-url", help="POST batches, e.g. http://localhost:8000/api/v1/events/batch")
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
        help="Register experiment metadata before POST (defaults to DATABASE_URL)",
    )
    parser.add_argument(
        "--assume-registered",
        action="store_true",
        help="POST without registering; use only when this run_id already exists",
    )
    parser.add_argument("--batch-size", type=int, default=500)
    args = parser.parse_args(argv)
    run = generate_run(args.scenario, seed=args.seed, severity=args.severity)
    payload = {"ground_truth": run.ground_truth, "batches": list(run.batches(args.batch_size))}
    if args.output:
        Path(args.output).write_text(json.dumps(payload, indent=2), encoding="utf-8")
    if args.post_url:
        if args.database_url:
            register_run(run, args.database_url)
        elif not args.assume_registered:
            parser.error("--post-url requires --database-url (or --assume-registered for an existing run)")
        post_run(run, args.post_url, batch_size=args.batch_size)
    print(json.dumps({"run_id": run.run_id, "events": len(run.events), **run.ground_truth}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
