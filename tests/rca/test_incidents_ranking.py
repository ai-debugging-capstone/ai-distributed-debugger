from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from rca.domain import AnomalyResult, DependencyEdge, FeatureBucket, Incident
from rca.graph import DirectedDependencyGraph
from rca.incidents import IncidentDetector
from rca.ranking import (
    HeuristicRootCauseRanker,
    canonical_edge_fault_class,
    canonical_node_semantics,
)


class IncidentAndRankingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.start = datetime(2026, 1, 1, tzinfo=timezone.utc)

    def anomaly(self, entity: str, offset: int, score: float, missing: float = 0.0) -> AnomalyResult:
        when = self.start + timedelta(seconds=10 * offset)
        bucket = FeatureBucket(
            when,
            when + timedelta(seconds=10),
            "service",
            entity,
            "run-1",
            {"latency_ms_p95": 500 if score else 10, "error_rate": 0.5 if score else 0},
            missingness={"span": missing, "metric": missing, "log": missing},
        )
        return AnomalyResult(
            bucket,
            score,
            score,
            {"latency_ms_p95": score},
            {"latency_ms_p95": 10},
            {"latency_ms_p95": 1},
            True,
        )

    def test_detector_requires_sustained_anomaly_and_resolves(self) -> None:
        detector = IncidentDetector(min_consecutive=2, recovery_consecutive=2)
        self.assertEqual(detector.update([self.anomaly("db", 0, 8)]), [])
        opened = detector.update([self.anomaly("db", 1, 8)])
        self.assertEqual(opened[0].action, "opened")
        self.assertEqual(opened[0].incident.trigger_entity, "db")
        self.assertEqual(opened[0].incident.severity, "warning")
        refreshed = detector.update([self.anomaly("db", 2, 8)])
        self.assertEqual([event.action for event in refreshed], ["updated"])
        self.assertEqual(detector.update([self.anomaly("db", 3, 0)]), [])
        resolved = detector.update([self.anomaly("db", 4, 0)])
        self.assertEqual(resolved[0].action, "resolved")
        self.assertEqual(resolved[0].incident.status, "resolved")

    def test_ranker_prefers_early_upstream_candidate_with_evidence(self) -> None:
        graph = DirectedDependencyGraph(
            [
                DependencyEdge(self.start, "frontend", "catalog", request_count=10),
                DependencyEdge(self.start, "catalog", "db", request_count=10),
            ]
        )
        incident = Incident(
            self.start,
            self.start + timedelta(seconds=10),
            "db",
            ["db", "catalog", "frontend"],
            10,
            "critical",
            "run-1",
            metadata={
                "anomaly_times": {
                    "db": self.start.isoformat(),
                    "catalog": (self.start + timedelta(seconds=10)).isoformat(),
                    "frontend": (self.start + timedelta(seconds=20)).isoformat(),
                }
            },
        )
        anomalies = [self.anomaly("db", 0, 10), self.anomaly("catalog", 1, 7), self.anomaly("frontend", 2, 5)]
        decision = HeuristicRootCauseRanker(min_margin=0).rank(incident, anomalies, graph)
        candidates = [item for item in decision.rankings if item.entity_id != "inconclusive"]
        self.assertEqual(candidates[0].entity_id, "db")
        self.assertTrue(candidates[0].evidence)
        self.assertIn("propagation", candidates[0].evidence_summary)

    def test_ranker_abstains_on_missing_telemetry(self) -> None:
        incident = Incident(self.start, self.start, "x", ["x"], 4, "medium", "run-1")
        decision = HeuristicRootCauseRanker().rank(
            incident, [self.anomaly("x", 0, 4, missing=1)], DirectedDependencyGraph()
        )
        self.assertTrue(decision.abstained)
        self.assertIn("inconclusive", [item.entity_id for item in decision.rankings])
        self.assertEqual(decision.actionable_rankings[0].entity_id, "x")

    def test_failed_client_edge_ranks_unreachable_unsampled_target(self) -> None:
        graph = DirectedDependencyGraph(
            [
                DependencyEdge(
                    self.start,
                    "frontend",
                    "checkout",
                    request_count=8,
                    error_count=4,
                ),
                DependencyEdge(
                    self.start,
                    "checkout",
                    "payment",
                    protocol="grpc",
                    request_count=4,
                    error_count=4,
                ),
            ]
        )
        incident = Incident(
            self.start,
            self.start + timedelta(seconds=10),
            "checkout",
            ["checkout", "frontend"],
            10,
            "critical",
            "run-1",
            metadata={
                "anomaly_times": {
                    "checkout": self.start.isoformat(),
                    "frontend": (self.start + timedelta(seconds=10)).isoformat(),
                }
            },
        )
        decision = HeuristicRootCauseRanker(min_margin=0).rank(
            incident,
            [
                self.anomaly("checkout", 0, 10),
                self.anomaly("frontend", 1, 7),
                # Resource metrics may still produce a target bucket even
                # though no payment server request span was observed.
                self.anomaly("payment", 0, 0),
            ],
            graph,
            top_k=10,
        )
        candidates = list(decision.actionable_rankings)
        causal = next(item for item in candidates if item.entity_id == "checkout->payment")

        self.assertLessEqual(causal.rank, 3)
        self.assertEqual(causal.fault_class, "dependency_unreachable")
        self.assertEqual(causal.evidence[-1].signal_name, "edge_error_rate")
        self.assertEqual(causal.evidence[-1].value, 1.0)

    def test_demo_database_names_map_to_database_lock(self) -> None:
        anomaly = self.anomaly("astronomy-db", 0, 8)
        self.assertEqual(
            canonical_node_semantics("astronomy-db", anomaly),
            ("database", "database_lock"),
        )
        self.assertEqual(
            canonical_edge_fault_class("product-catalog->astronomy-db", anomaly),
            "database_lock",
        )


if __name__ == "__main__":
    unittest.main()
