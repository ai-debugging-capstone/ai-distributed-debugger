"""Regression tests for the healthy-system false-positive fixes.

Each test pins one mechanism observed in live OpenTelemetry Demo telemetry with
every fault flag off: cumulative CPU counters read as utilization, the normal
latency tail of low-volume services, near-zero CPU jitter, shared-dependency
hubs (flagd) winning propagation credit, observability plumbing and Locust
opening incidents, incidents growing to every service and never recovering,
and incidents orphaned by a worker restart or compared against a stale
pre-restart baseline.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from rca.domain import AnomalyResult, DependencyEdge, FeatureBucket, Incident
from rca.entities import EntityRoles
from rca.features import (
    StreamingAnomalyScorer,
    aggregate_feature_buckets,
    robust_tail_z_score,
    score_feature_bucket,
)
from rca.graph import DirectedDependencyGraph
from rca.incidents import IncidentDetector, IncidentEvent
from rca.ranking import HeuristicRootCauseRanker, canonical_node_semantics
from rca.worker import RCAWorker, WorkerSettings

UTC = timezone.utc
START = datetime(2026, 1, 1, tzinfo=UTC)


def bucket(entity: str, offset: int, features: dict, run_id: str | None = "run-1") -> FeatureBucket:
    when = START + timedelta(seconds=10 * offset)
    return FeatureBucket(
        when,
        when + timedelta(seconds=10),
        "service",
        entity,
        run_id,
        features,
        missingness={"span": 0, "metric": 0, "log": 0},
    )


def anomaly(entity: str, offset: int, score: float, run_id: str | None = "run-1") -> AnomalyResult:
    return AnomalyResult(
        bucket(entity, offset, {"error_rate": 1.0 if score else 0.0}, run_id),
        score,
        score,
        {"error_rate": score},
        {"error_rate": 0.0},
        {"error_rate": 1.0},
        True,
    )


class ResourceMetricExtractionTests(unittest.TestCase):
    def test_cumulative_cpu_time_counters_are_not_utilization(self) -> None:
        metrics = [
            {"service_name": "ad", "time_unix": START, "metric_name": name, "value": value}
            for name, value in (
                ("jvm.cpu.time", 114.4),
                ("container.cpu.usage.total", 850979769000.0),
                ("system.cpu.time", 1500266.4),
                ("container.cpu.utilization", 0.12),
            )
        ]
        features = aggregate_feature_buckets([], metrics, [])[0].features
        self.assertAlmostEqual(features["cpu_percent"], 12.0)

    def test_memory_capacity_and_allocation_counters_are_not_usage(self) -> None:
        metrics = [
            {"service_name": "cart", "time_unix": START, "metric_name": name, "value": value}
            for name, value in (
                ("container.memory.usage.limit", 1572864000.0),
                ("dotnet.gc.heap.total_allocated", 144346448.0),
                ("process.memory.virtual", 56535482368.0),
                ("container.memory.usage.total", 121612947.0),
            )
        ]
        features = aggregate_feature_buckets([], metrics, [])[0].features
        self.assertEqual(features["memory_bytes"], 121612947.0)


class HealthyNoiseScoringTests(unittest.TestCase):
    def test_recurring_latency_tail_is_not_anomalous_but_a_shift_is(self) -> None:
        # p95 of ~6 requests: mostly fast, with routine multi-second outliers.
        healthy = [8, 9, 7, 2600, 8, 10, 9, 1900, 8, 9, 3100, 7, 8, 9, 2200, 8]
        spike, _ = robust_tail_z_score(2900, healthy)
        self.assertLess(spike, 3.5)
        shifted, reference = robust_tail_z_score(10_000, healthy)
        self.assertGreater(shifted, 3.5)
        self.assertGreater(reference, 1000)

    def test_tiny_latency_change_on_fast_service_is_noise(self) -> None:
        score, _ = robust_tail_z_score(3.0, [0.5, 0.4, 0.6, 0.5, 0.5, 0.7, 0.5, 0.4])
        self.assertLess(score, 1.0)

    def test_near_idle_cpu_jitter_does_not_score(self) -> None:
        history = [bucket("ad", i, {"cpu_percent": 0.01}) for i in range(10)]
        result = score_feature_bucket(bucket("ad", 10, {"cpu_percent": 0.03}), history, min_history=6)
        self.assertLess(result.feature_scores["cpu_percent"], 1.0)

    def test_first_failure_on_quiet_service_stays_sensitive(self) -> None:
        history = [bucket("payment", i, {"error_rate": 0.0, "error_count": 0}) for i in range(10)]
        result = score_feature_bucket(
            bucket("payment", 10, {"error_rate": 1.0, "error_count": 1}), history, min_history=6
        )
        self.assertGreaterEqual(result.score, 3.5)

    def test_reset_run_forgets_only_that_runs_baseline(self) -> None:
        scorer = StreamingAnomalyScorer(lookback=10, min_history=2)
        for offset in range(3):
            scorer.score(bucket("cart", offset, {"error_rate": 0.0}, "live"))
            scorer.score(bucket("cart", offset, {"error_rate": 0.0}, "other"))
        scorer.reset_run("live")
        self.assertFalse(scorer.score(bucket("cart", 4, {"error_rate": 0.0}, "live")).sufficient_history)
        self.assertTrue(scorer.score(bucket("cart", 4, {"error_rate": 0.0}, "other")).sufficient_history)


class PropagationSpecificityTests(unittest.TestCase):
    def hub_graph(self) -> DirectedDependencyGraph:
        callers = ["frontend", "cart", "checkout", "ad", "recommendation", "shipping"]
        edges = [DependencyEdge(START, caller, "flagd", request_count=5) for caller in callers]
        edges += [
            DependencyEdge(START, "frontend", "checkout", request_count=5),
            DependencyEdge(START, "checkout", "payment", request_count=5),
        ]
        return DirectedDependencyGraph(edges)

    def test_hub_called_by_everyone_earns_no_credit_for_a_partial_impact(self) -> None:
        graph = self.hub_graph()
        hub, details = graph.propagation_score("flagd", ["frontend", "checkout"])
        specific, _ = graph.propagation_score("payment", ["frontend", "checkout"])
        self.assertGreater(details["background_coverage"], 0.5)
        self.assertGreater(specific, hub)

    def test_impact_on_every_caller_still_credits_the_shared_dependency(self) -> None:
        graph = self.hub_graph()
        everyone = ["frontend", "cart", "checkout", "ad", "recommendation", "shipping"]
        score, details = graph.propagation_score("flagd", everyone, ignored={"payment"})
        self.assertEqual(details["background_coverage"], 0.0)
        self.assertGreater(score, 0.5)

    def test_workload_source_propagates_downstream(self) -> None:
        graph = DirectedDependencyGraph(
            [
                DependencyEdge(START, "load-generator", "frontend-proxy", request_count=50),
                DependencyEdge(START, "frontend-proxy", "frontend", request_count=50),
            ]
        )
        upstream, _ = graph.propagation_score("load-generator", ["frontend-proxy", "frontend"])
        downstream, _ = graph.propagation_score(
            "load-generator", ["frontend-proxy", "frontend"], direction="downstream"
        )
        self.assertEqual(upstream, 0.0)
        self.assertGreater(downstream, 0.5)


class EntityRoleTests(unittest.TestCase):
    def test_environment_overrides_defaults(self) -> None:
        roles = EntityRoles.from_env(
            {"RCA_INFRASTRUCTURE_ENTITIES": "collector, host", "RCA_WORKLOAD_ENTITIES": ""}
        )
        self.assertTrue(roles.is_infrastructure("host"))
        self.assertFalse(roles.is_infrastructure("otel-collector"))
        self.assertFalse(roles.is_workload("load-generator"))

    def test_load_generator_is_ranked_as_external_workload(self) -> None:
        self.assertEqual(
            canonical_node_semantics("load-generator", None), ("external", "workload_spike")
        )

    def test_infrastructure_and_workload_cannot_open_incidents(self) -> None:
        detector = IncidentDetector(min_consecutive=1)
        for entity in ("otelcol-contrib", "docker-desktop", "load-generator"):
            self.assertEqual(detector.update([anomaly(entity, 0, 15)]), [])
        self.assertFalse(detector.has_active_incident("run-1"))


class IncidentScopeTests(unittest.TestCase):
    def test_unrelated_single_bucket_blips_neither_join_nor_prevent_recovery(self) -> None:
        detector = IncidentDetector(min_consecutive=2, recovery_consecutive=2)
        detector.update([anomaly("payment", 0, 8)])
        opened = detector.update([anomaly("payment", 1, 8)])
        self.assertEqual(opened[0].action, "opened")
        # payment recovers while an unrelated service blips for one bucket.
        self.assertEqual(
            detector.update([anomaly("payment", 2, 0), anomaly("quote", 2, 9)]), []
        )
        resolved = detector.update([anomaly("payment", 3, 0), anomaly("quote", 3, 0)])
        self.assertEqual([event.action for event in resolved], ["resolved"])
        self.assertEqual(resolved[0].incident.affected_entities, ["payment"])

    def test_sustained_new_anomaly_joins_with_its_true_onset(self) -> None:
        detector = IncidentDetector(min_consecutive=2)
        detector.update([anomaly("payment", 0, 8)])
        detector.update([anomaly("payment", 1, 8)])
        detector.update([anomaly("payment", 2, 8), anomaly("checkout", 2, 6)])
        events = detector.update([anomaly("payment", 3, 8), anomaly("checkout", 3, 6)])
        incident = events[0].incident
        self.assertEqual(incident.affected_entities, ["checkout", "payment"])
        self.assertEqual(
            incident.metadata["anomaly_times"]["checkout"],
            (START + timedelta(seconds=20)).isoformat(),
        )

    def test_restored_incident_can_recover(self) -> None:
        detector = IncidentDetector(recovery_consecutive=2)
        incident = Incident(START, START, "cart", ["cart"], 9, "warning", "live")
        detector.restore(incident)
        self.assertTrue(detector.has_active_incident("live"))
        detector.update([anomaly("cart", 1, 0, "live")])
        resolved = detector.update([anomaly("cart", 2, 0, "live")])
        self.assertEqual(resolved[0].incident.status, "resolved")


class RankerRoleTests(unittest.TestCase):
    def test_infrastructure_is_never_a_suspect_and_hub_does_not_win(self) -> None:
        callers = ["frontend", "cart", "checkout", "ad", "recommendation"]
        edges = [DependencyEdge(START, caller, "flagd", request_count=5) for caller in callers]
        edges += [
            DependencyEdge(START, "frontend", "checkout", request_count=5),
            DependencyEdge(START, "checkout", "payment", request_count=5, error_count=5),
            DependencyEdge(START, "otelcol-contrib", "frontend", request_count=5),
        ]
        incident = Incident(
            START,
            START + timedelta(seconds=10),
            "checkout",
            ["checkout", "frontend", "otelcol-contrib"],
            10,
            "critical",
            "run-1",
            metadata={
                "anomaly_times": {
                    "flagd": START.isoformat(),
                    "otelcol-contrib": START.isoformat(),
                    "checkout": START.isoformat(),
                    "frontend": (START + timedelta(seconds=10)).isoformat(),
                }
            },
        )
        decision = HeuristicRootCauseRanker(min_margin=0).rank(
            incident,
            [
                anomaly("flagd", 0, 6),
                anomaly("otelcol-contrib", 0, 15),
                anomaly("checkout", 0, 10),
                anomaly("frontend", 1, 7),
            ],
            DirectedDependencyGraph(edges),
        )
        ranked = [item.entity_id for item in decision.actionable_rankings]
        self.assertNotIn("otelcol-contrib", ranked)
        self.assertFalse(any("otelcol-contrib" in entity for entity in ranked))
        self.assertLess(ranked.index("checkout->payment"), ranked.index("flagd"))


class _Cursor:
    def __init__(self, connection: "_Connection") -> None:
        self.connection = connection
        self.next_row = None
        self.rows: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return False

    def execute(self, query, params=None):
        normalized = " ".join(query.split())
        self.connection.queries.append((normalized, params))
        if "FROM incidents WHERE status = 'open'" in normalized:
            self.rows = self.connection.open_rows
        elif "MAX(bucket_start) AS last_bucket" in normalized:
            self.next_row = {"last_bucket": self.connection.last_bucket}

    def fetchall(self):
        return self.rows

    def fetchone(self):
        return self.next_row


class _Connection:
    def __init__(self, open_rows=(), last_bucket=None) -> None:
        self.queries: list[tuple[str, object]] = []
        self.open_rows = list(open_rows)
        self.last_bucket = last_bucket

    def cursor(self):
        return _Cursor(self)


class WorkerLifecycleTests(unittest.TestCase):
    def worker(self) -> RCAWorker:
        return RCAWorker(
            WorkerSettings("postgresql://unused", min_history_buckets=1, reset_gap_seconds=60)
        )

    def test_telemetry_gap_resets_baseline_and_closes_open_incident(self) -> None:
        worker = self.worker()
        worker.scorer.score(bucket("cart", 0, {"error_rate": 0.0}, "live"))
        worker.detector.restore(Incident(START, START, "cart", ["cart"], 9, "warning", "live"))
        connection = _Connection()

        worker._handle_telemetry_gap(connection, "live", START + timedelta(seconds=10), START)
        self.assertTrue(worker.detector.has_active_incident("live"))

        resumed = START + timedelta(days=20)
        worker._handle_telemetry_gap(connection, "live", resumed, None)
        self.assertFalse(worker.detector.has_active_incident("live"))
        persisted = [params for query, params in connection.queries if "INSERT INTO incidents" in query]
        self.assertEqual(persisted[-1][2], "resolved")
        self.assertEqual(persisted[-1][5], START + timedelta(seconds=20))
        self.assertFalse(
            worker.scorer.score(bucket("cart", 2000, {"error_rate": 0.0}, "live")).sufficient_history
        )

    def test_restart_readopts_newest_open_incident_and_closes_older_ones(self) -> None:
        def row(started: datetime, trigger: str) -> dict:
            return {
                "id": f"id-{trigger}",
                "run_id": "live",
                "status": "open",
                "started_at": started,
                "detected_at": started,
                "ended_at": None,
                "severity": "critical",
                "summary": "",
                "trigger_entity": trigger,
                "max_anomaly": 18.0,
                "affected_entities": [trigger],
                "metadata": {},
            }

        old_start = START
        new_start = START + timedelta(days=20)
        connection = _Connection(
            [row(old_start, "image-provider"), row(new_start, "checkout")],
            last_bucket=old_start + timedelta(minutes=5),
        )
        worker = self.worker()
        worker._restore_open_incidents(connection)

        active = worker.detector.active_incidents
        self.assertEqual([incident.trigger_entity for incident in active], ["checkout"])
        self.assertEqual(active[0].id, "id-checkout")
        persisted = [params for query, params in connection.queries if "INSERT INTO incidents" in query]
        self.assertEqual(len(persisted), 1)
        self.assertEqual(persisted[0][0], "id-image-provider")
        self.assertEqual(persisted[0][2], "resolved")
        self.assertEqual(persisted[0][5], old_start + timedelta(minutes=5, seconds=10))

    def test_restored_incident_keeps_its_stored_id(self) -> None:
        # An id written by an older release, read back in a non-UTC session.
        started = START.astimezone(timezone(timedelta(hours=-5)))
        connection = _Connection([{
            "id": "legacy-id",
            "run_id": "live",
            "status": "open",
            "started_at": started,
            "detected_at": started,
            "ended_at": None,
            "severity": "warning",
            "summary": "",
            "trigger_entity": "cart",
            "max_anomaly": 9.0,
            "affected_entities": ["cart"],
            "metadata": {},
        }])
        worker = self.worker()
        worker._restore_open_incidents(connection)
        restored = worker.detector.active_incidents[0]

        worker._persist_incident(connection, IncidentEvent("updated", restored))
        resolved = worker.detector.resolve_active("live", START + timedelta(minutes=1))
        worker._persist_incident(connection, resolved)

        persisted = [params[0] for query, params in connection.queries if "INSERT INTO incidents" in query]
        self.assertEqual(persisted, ["legacy-id", "legacy-id"])

    def test_persisted_identity_is_stable_across_time_zones(self) -> None:
        worker = self.worker()
        connection = _Connection()
        utc = Incident(START, START, "cart", ["cart"], 9, "warning", "live")
        shifted = Incident(
            START.astimezone(timezone(timedelta(hours=-5))), START, "cart", ["cart"], 9, "warning", "live"
        )
        first = worker._persist_incident(connection, IncidentEvent("opened", utc)).id
        second = worker._persist_incident(connection, IncidentEvent("opened", shifted)).id
        self.assertEqual(first, second)


if __name__ == "__main__":
    unittest.main()
