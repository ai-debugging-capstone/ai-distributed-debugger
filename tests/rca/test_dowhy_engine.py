from __future__ import annotations

import importlib.util
import math
import random
import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

from rca.domain import AnomalyResult, DependencyEdge, FeatureBucket, Incident
from rca.dowhy_engine import DoWhyRootCauseRanker
from rca.graph import DirectedDependencyGraph


class DoWhyAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.started = datetime(2026, 1, 1, tzinfo=UTC)
        self.engine = DoWhyRootCauseRanker(min_baseline_samples=10)
        self.incident = Incident(
            self.started + timedelta(seconds=120),
            self.started + timedelta(seconds=130),
            "payment", ["payment", "checkout", "frontend"], 8, "warning", "run-a",
        )
        self.graph = DirectedDependencyGraph([
            DependencyEdge(self.started, "frontend", "checkout", "run-a", "http", 20),
            DependencyEdge(self.started, "checkout", "payment", "run-a", "grpc", 20),
        ])
        rng = random.Random(101)
        self.healthy = []
        for offset in range(12):
            payment = rng.uniform(8, 18)
            checkout = 20 + 0.9 * payment + rng.uniform(-1, 1)
            frontend = 5 + 1.1 * checkout + rng.uniform(-1, 1)
            self.healthy.extend([
                self.result("payment", offset, payment),
                self.result("checkout", offset, checkout),
                self.result("frontend", offset, frontend),
            ])
        self.current = [
            self.result("payment", 13, 200, 8),
            self.result("checkout", 13, 200, 8),
            self.result("frontend", 13, 225, 8),
        ]

    def result(
        self, entity: str, offset: int, latency: float, score: float = 0,
        *, run_id: str = "run-a", observed: bool = True,
    ) -> AnomalyResult:
        timestamp = self.started + timedelta(seconds=offset * 10)
        bucket = FeatureBucket(
            timestamp, timestamp + timedelta(seconds=10), "service", entity, run_id,
            {"latency_ms_p95": latency, "request_count": 20 if observed else 0},
            missingness={"span": 0 if observed else 1, "metric": 0, "log": 0},
        )
        return AnomalyResult(
            bucket, score, score, {"latency_ms_p95": score},
            {"latency_ms_p95": 20}, {"latency_ms_p95": 1}, True,
        )

    def test_signed_attribution_is_preserved_without_probability(self) -> None:
        self.engine.observe(self.healthy)
        with patch.object(
            self.engine, "_attribute", return_value={"payment": 4.2, "checkout": -0.3, "frontend": 0.1},
        ) as provider:
            decision = self.engine.rank(self.incident, self.current, self.graph)
        self.assertEqual(decision.rankings[0].entity_id, "payment")
        self.assertTrue(all(item.probability is None for item in decision.rankings))
        checkout = next(item for item in decision.rankings if item.entity_id == "checkout")
        self.assertEqual(checkout.score, -0.3)
        self.assertEqual(checkout.evidence[0].contribution, -0.3)
        prepared = provider.call_args.args[0]
        self.assertEqual(prepared.target, "frontend")
        self.assertEqual(prepared.causal_edges, (("checkout", "frontend"), ("payment", "checkout")))
        self.assertTrue(all(timestamp < self.incident.started_at for timestamp in prepared.baseline_times))
        metadata = self.incident.metadata["analysis"]
        self.assertEqual(metadata["status"], "analyzed")
        self.assertEqual(metadata["score_kind"], "causal_attribution")
        self.assertEqual(metadata["baseline_samples"], 12)

    def test_missing_provider_falls_back_with_original_engine_identity(self) -> None:
        self.engine.observe(self.healthy)
        with patch.object(self.engine, "_attribute", side_effect=ImportError("dowhy unavailable")):
            decision = self.engine.rank(self.incident, self.current, self.graph)
        self.assertEqual(self.incident.metadata["analysis"]["status"], "fallback")
        self.assertIn("ImportError", self.incident.metadata["analysis"]["reason"])
        self.assertTrue(all(item.model_version == "heuristic-v1" for item in decision.rankings))

    def test_database_latency_does_not_claim_a_database_lock_mechanism(self) -> None:
        for item in self.healthy:
            bucket = item.bucket
            offset = int((bucket.bucket_start - self.started).total_seconds() / 10)
            self.engine.observe([self.result(
                "postgres" if bucket.entity_id == "payment" else bucket.entity_id,
                offset, bucket.value("latency_ms_p95"),
            )])
        current = [
            self.result("postgres", 13, 200, 8),
            self.result("checkout", 13, 200, 8),
            self.result("frontend", 13, 225, 8),
        ]
        self.incident.affected_entities = ["postgres", "checkout", "frontend"]
        graph = DirectedDependencyGraph([
            DependencyEdge(self.started, "frontend", "checkout", "run-a", "http", 20),
            DependencyEdge(self.started, "checkout", "postgres", "run-a", "postgresql", 20),
        ])
        with patch.object(
            self.engine, "_attribute", return_value={"postgres": 4.2, "checkout": -0.3, "frontend": 0.1},
        ):
            decision = self.engine.rank(self.incident, current, graph)
        self.assertEqual(decision.rankings[0].entity_type, "database")
        self.assertEqual(decision.rankings[0].fault_class, "service_error")

    def test_cold_start_does_not_call_provider(self) -> None:
        with patch.object(self.engine, "_attribute") as provider:
            self.engine.rank(self.incident, self.current, self.graph)
        provider.assert_not_called()
        self.assertIn("only 0 aligned healthy baseline rows", self.incident.metadata["analysis"]["reason"])

    def test_baseline_never_uses_other_runs_or_post_incident_data(self) -> None:
        # Nine valid rows are insufficient; unrelated runs and future/incident
        # observations must not manufacture the missing tenth healthy sample.
        self.engine.observe(self.healthy[:27])
        self.engine.observe([
            self.result(node, offset, 10 + offset, run_id="run-b")
            for offset in range(12) for node in ("payment", "checkout", "frontend")
        ])
        self.engine.observe([
            self.result(node, offset, 10 + offset)
            for offset in range(12, 40) for node in ("payment", "checkout", "frontend")
        ])
        with patch.object(self.engine, "_attribute") as provider:
            self.engine.rank(self.incident, self.current, self.graph)
        provider.assert_not_called()
        self.assertIn("only 9 aligned", self.incident.metadata["analysis"]["reason"])

    def test_current_row_requires_one_common_observation_time(self) -> None:
        self.engine.observe(self.healthy)
        unaligned = self.current[:2] + [self.result("frontend", 14, 250, 8)]
        with patch.object(self.engine, "_attribute") as provider:
            self.engine.rank(self.incident, unaligned, self.graph)
        provider.assert_not_called()
        self.assertIn("current latency row is incomplete", self.incident.metadata["analysis"]["reason"])

    def test_other_run_and_future_dependency_edges_do_not_enter_the_model(self) -> None:
        self.engine.observe(self.healthy)
        graph = DirectedDependencyGraph([
            *self.graph.edges.values(),
            DependencyEdge(self.started, "frontend", "foreign", "run-b", "grpc", 20),
            DependencyEdge(
                self.started + timedelta(seconds=140),
                "frontend", "future", "run-a", "grpc", 20,
            ),
        ])
        with patch.object(
            self.engine, "_attribute", return_value={"payment": 4.2, "checkout": -0.3, "frontend": 0.1},
        ) as provider:
            self.engine.rank(self.incident, self.current, graph)
        self.assertEqual(provider.call_args.args[0].nodes, ("checkout", "frontend", "payment"))
        self.assertEqual(self.incident.metadata["analysis"]["status"], "analyzed")

    def test_missing_spans_are_not_zero_filled(self) -> None:
        self.engine.observe(self.healthy)
        current = self.current[1:] + [self.result("payment", 13, 200, 8, observed=False)]
        with patch.object(self.engine, "_attribute") as provider:
            self.engine.rank(self.incident, current, self.graph)
        provider.assert_not_called()
        self.assertEqual(self.incident.metadata["analysis"]["status"], "fallback")

    def test_cycle_and_async_paths_are_rejected_without_edge_removal(self) -> None:
        self.engine.observe(self.healthy)
        graphs = [
            (DirectedDependencyGraph([
                *self.graph.edges.values(),
                DependencyEdge(self.started, "payment", "frontend", "run-a", "grpc", 20),
            ]), "contains a cycle"),
            (DirectedDependencyGraph([
                DependencyEdge(self.started, "frontend", "checkout", "run-a", "http", 20),
                DependencyEdge(self.started, "checkout", "payment", "run-a", "messaging", 20),
            ]), "asynchronous dependency"),
        ]
        for graph, reason in graphs:
            with self.subTest(reason=reason), patch.object(self.engine, "_attribute") as provider:
                self.engine.rank(self.incident, self.current, graph)
                provider.assert_not_called()
                self.assertIn(reason, self.incident.metadata["analysis"]["reason"])

    def test_constant_baseline_and_resource_only_incidents_use_fallback(self) -> None:
        self.engine.observe([
            self.result(node, offset, 10)
            for offset in range(12) for node in ("payment", "checkout", "frontend")
        ])
        self.engine.rank(self.incident, self.current, self.graph)
        self.assertIn("baseline is constant", self.incident.metadata["analysis"]["reason"])
        resource_current = [
            AnomalyResult(item.bucket, 8, 8, {"cpu_percent": 8}, {}, {}, True)
            for item in self.current
        ]
        self.engine.rank(self.incident, resource_current, self.graph)
        self.assertIn("no observed p95 latency anomaly", self.incident.metadata["analysis"]["reason"])

    def test_no_positive_contribution_abstains_and_nonfinite_provider_falls_back(self) -> None:
        self.engine.observe(self.healthy)
        with patch.object(
            self.engine, "_attribute", return_value={node: -0.1 for node in ("payment", "checkout", "frontend")},
        ):
            decision = self.engine.rank(self.incident, self.current, self.graph)
        self.assertTrue(decision.abstained)
        self.assertEqual(decision.rankings[-1].entity_id, "inconclusive")
        with patch.object(
            self.engine, "_attribute", return_value={"payment": math.nan, "checkout": 0.1, "frontend": 0.1},
        ):
            self.engine.rank(self.incident, self.current, self.graph)
        self.assertEqual(self.incident.metadata["analysis"]["status"], "fallback")

    def test_history_is_bounded_and_duplicate_observations_do_not_add_samples(self) -> None:
        engine = DoWhyRootCauseRanker(min_baseline_samples=3, max_history_buckets=5)
        engine.observe(self.healthy)
        engine.observe(self.healthy)
        self.assertEqual(len(engine._history["run-a"]), 5)
        self.assertEqual(sum(len(row) for row in engine._history["run-a"].values()), 15)

    @unittest.skipUnless(importlib.util.find_spec("dowhy"), "install rca/requirements.txt")
    def test_real_dowhy_attributes_dependency_slowdown_and_reuses_fitted_baseline(self) -> None:
        # Actual package integration: the fault is injected only in payment's
        # intrinsic latency; callers inherit it through healthy fitted equations.
        engine = DoWhyRootCauseRanker(min_baseline_samples=30, num_samples=300)
        rng = random.Random(101)
        healthy = []
        for offset in range(100):
            payment = rng.uniform(8, 18)
            checkout = 20 + 0.9 * payment + rng.uniform(-0.5, 0.5)
            frontend = 5 + 1.1 * checkout + rng.uniform(-0.5, 0.5)
            healthy.extend([
                self.result("payment", offset, payment),
                self.result("checkout", offset, checkout),
                self.result("frontend", offset, frontend),
            ])
        engine.observe(healthy)
        self.incident.started_at = self.started + timedelta(seconds=1000)
        self.incident.detected_at = self.started + timedelta(seconds=1010)
        current = [
            self.result("payment", 101, 100, 8),
            self.result("checkout", 101, 110, 8),
            self.result("frontend", 101, 126, 8),
        ]
        decision = engine.rank(self.incident, current, self.graph)
        self.assertEqual(self.incident.metadata["analysis"]["status"], "analyzed")
        self.assertEqual(decision.rankings[0].entity_id, "payment")
        self.assertGreater(decision.rankings[0].score, 0)
        self.assertIsNone(decision.rankings[0].probability)
        self.assertTrue(decision.rankings[0].model_version.startswith("dowhy-gcm-"))
        self.assertEqual(len(engine._models), 1)
        again = engine.rank(self.incident, current, self.graph)
        self.assertEqual(len(engine._models), 1)
        self.assertEqual(again.rankings, decision.rankings)


if __name__ == "__main__":
    unittest.main()
