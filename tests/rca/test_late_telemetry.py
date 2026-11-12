"""Regression tests for the pilot capture's missed faults.

The first real pilot (cartFailure, paymentFailure) missed both causes because
telemetry that reached PostgreSQL after its 10 s bucket was scored was never
counted (12-33% of spans), hung requests were filed under their start time,
and a low-QPS service's idle buckets broke every anomaly streak.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from rca.domain import AnomalyResult, FeatureBucket
from rca.features import aggregate_feature_buckets, span_event_time
from rca.graph import build_dependency_edges
from rca.incidents import IncidentDetector
from rca.worker import RunLifecycle, WorkerSettings, incident_processing_mode

UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)


def result(entity: str, offset: int, score: float, requests: float) -> AnomalyResult:
    when = START + timedelta(seconds=10 * offset)
    bucket = FeatureBucket(
        when,
        when + timedelta(seconds=10),
        "service",
        entity,
        "run-1",
        {"request_count": requests, "error_rate": 1.0 if score else 0.0},
    )
    return AnomalyResult(bucket, score, score, {"error_rate": score}, {}, {}, True)


class CompletionTimeBucketingTests(unittest.TestCase):
    def test_hung_request_is_counted_when_it_completes(self) -> None:
        # EmptyCart under cartFailure: starts, hangs ~244 s, then fails.
        span = {
            "trace_id": "t",
            "span_id": "s",
            "service_name": "cart",
            "start_time": START,
            "end_time": START + timedelta(seconds=244),
            "duration_ms": 244_000,
            "status_code": "ERROR",
        }
        bucket = aggregate_feature_buckets([span], [], [])[0]
        self.assertEqual(bucket.bucket_start, START + timedelta(seconds=240))
        self.assertEqual(bucket.features["error_count"], 1)

    def test_event_time_falls_back_to_start_plus_duration(self) -> None:
        self.assertEqual(
            span_event_time({"start_time": START, "duration_ms": 1500}),
            START + timedelta(milliseconds=1500),
        )

    def test_dependency_edges_use_completion_time(self) -> None:
        parent = {
            "trace_id": "t", "span_id": "p", "service_name": "checkout",
            "start_time": START, "end_time": START + timedelta(seconds=31),
        }
        child = {
            "trace_id": "t", "span_id": "c", "parent_span_id": "p", "service_name": "payment",
            "start_time": START, "end_time": START + timedelta(seconds=30), "status_code": "ERROR",
        }
        edge = build_dependency_edges([parent, child])[0]
        self.assertEqual(edge.bucket_start, START + timedelta(seconds=30))
        self.assertEqual(edge.error_count, 1)


class BucketTimeLifecycleTests(unittest.TestCase):
    window = RunLifecycle(
        "recovering",
        fault_started_at=START + timedelta(seconds=60),
        fault_ended_at=START + timedelta(seconds=360),
    )

    def mode(self, offset_seconds: int, *, active: bool = False, lifecycle=None) -> str:
        return incident_processing_mode(
            "run-1",
            lifecycle or self.window,
            has_active_incident=active,
            bucket_start=START + timedelta(seconds=offset_seconds),
        )

    def test_late_scored_warmup_bucket_never_detects(self) -> None:
        running = RunLifecycle("running", fault_started_at=self.window.fault_started_at)
        self.assertEqual(self.mode(50, lifecycle=running), "observe")

    def test_fault_bucket_scored_after_recovery_began_still_detects(self) -> None:
        self.assertEqual(self.mode(350), "detect")

    def test_after_the_window_only_recovery_is_allowed(self) -> None:
        self.assertEqual(self.mode(360), "observe")
        self.assertEqual(self.mode(360, active=True), "recover")

    def test_runs_without_a_published_window_keep_status_gating(self) -> None:
        self.assertEqual(
            incident_processing_mode(
                "run-1", RunLifecycle("running"), has_active_incident=False, bucket_start=START
            ),
            "detect",
        )

    def test_grace_is_configurable(self) -> None:
        with patch.dict("os.environ", {"DATABASE_URL": "postgresql://x", "RCA_INGEST_GRACE_SECONDS": "45"}):
            self.assertEqual(WorkerSettings.from_env().ingest_grace_seconds, 45)


class LowTrafficStreakTests(unittest.TestCase):
    def test_idle_bucket_does_not_break_a_request_error_streak(self) -> None:
        detector = IncidentDetector(min_consecutive=2)
        self.assertEqual(detector.update([result("payment", 0, 20, requests=2)]), [])
        self.assertEqual(detector.update([result("payment", 1, 0, requests=0)]), [])
        opened = detector.update([result("payment", 2, 20, requests=1)])
        self.assertEqual([event.action for event in opened], ["opened"])
        self.assertEqual(opened[0].incident.started_at, START)

    def test_idle_buckets_are_not_evidence_of_recovery(self) -> None:
        detector = IncidentDetector(min_consecutive=2, recovery_consecutive=2, max_idle_hold=3)
        detector.update([result("payment", 0, 20, requests=2)])
        detector.update([result("payment", 1, 20, requests=2)])
        for offset in (2, 3, 4):
            self.assertEqual(detector.update([result("payment", offset, 0, requests=0)]), [])
        # The hold is bounded: continued silence eventually counts as recovery.
        events = detector.update([result("payment", 5, 0, requests=0)])
        events += detector.update([result("payment", 6, 0, requests=0)])
        self.assertEqual([event.action for event in events], ["resolved"])

    def test_metric_only_anomalies_are_not_held(self) -> None:
        # Kafka's periodic ERROR-log bursts carry no request traffic.
        detector = IncidentDetector(min_consecutive=2)
        detector.update([result("kafka", 0, 15, requests=0)])
        detector.update([result("kafka", 1, 0, requests=0)])
        self.assertEqual(detector.update([result("kafka", 2, 15, requests=0)]), [])


class PilotRankingTests(unittest.TestCase):
    def test_ip_server_address_resolves_to_the_rpc_service(self) -> None:
        client = {
            "trace_id": "t", "span_id": "c", "service_name": "checkout", "span_kind": "CLIENT",
            "start_time": START, "end_time": START + timedelta(milliseconds=300),
            "status_code": "ERROR",
            "attributes": {
                "server.address": "172.19.0.25",
                "rpc.method": "oteldemo.PaymentService/Charge",
            },
        }
        edge = build_dependency_edges([client])[0]
        self.assertEqual(edge.edge_id, "checkout->payment")

    def test_load_generator_needs_a_traffic_increase_to_rank(self) -> None:
        from rca.domain import DependencyEdge, Incident
        from rca.graph import DirectedDependencyGraph
        from rca.ranking import HeuristicRootCauseRanker

        graph = DirectedDependencyGraph(
            [
                DependencyEdge(START, "load-generator", "frontend-proxy", request_count=50),
                DependencyEdge(START, "frontend-proxy", "frontend", request_count=50),
            ]
        )
        incident = Incident(
            START, START, "frontend", ["frontend", "frontend-proxy"], 9, "warning", "run-1",
            metadata={"anomaly_times": {"frontend": START.isoformat(), "frontend-proxy": START.isoformat()}},
        )

        def bucket_result(entity, scores):
            bucket = FeatureBucket(START, START + timedelta(seconds=10), "service", entity, "run-1", {"request_count": 50})
            return AnomalyResult(bucket, max(scores.values()), 0, scores, {}, {}, True)

        slow_client = [
            bucket_result("load-generator", {"latency_ms_p95": 20.0, "request_count": 0.0}),
            bucket_result("frontend", {"latency_ms_p95": 9.0}),
            bucket_result("frontend-proxy", {"latency_ms_p95": 6.0}),
        ]
        ranked = HeuristicRootCauseRanker(min_margin=0).rank(incident, slow_client, graph)
        quiet = {item.entity_id: item for item in ranked.actionable_rankings}
        self.assertLess(quiet["load-generator"].score, 0.15)

        flood = [bucket_result("load-generator", {"request_count": 12.0})] + slow_client[1:]
        ranked = HeuristicRootCauseRanker(min_margin=0).rank(incident, flood, graph)
        top_two = [item.entity_id for item in ranked.actionable_rankings[:2]]
        self.assertIn("load-generator", top_two)


if __name__ == "__main__":
    unittest.main()
