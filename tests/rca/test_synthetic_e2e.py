from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone
from itertools import groupby

from app.telemetry_api.models import NormalizedBatch
from rca.features import StreamingAnomalyScorer, aggregate_feature_buckets
from rca.graph import DirectedDependencyGraph, build_dependency_edges
from rca.incidents import IncidentDetector
from rca.ranking import HeuristicRootCauseRanker
from rca.synthetic import SCENARIOS, generate_run


class SyntheticEndToEndTests(unittest.TestCase):
    def test_repeated_seed_namespaces_span_identity_by_run(self) -> None:
        first_start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        first = generate_run(
            "payment_failure",
            seed=202,
            start=first_start,
            warmup_buckets=1,
            fault_buckets=1,
            recovery_buckets=1,
            requests_per_route=1,
        )
        replay = generate_run(
            "payment_failure",
            seed=202,
            start=first_start + timedelta(hours=1),
            warmup_buckets=1,
            fault_buckets=1,
            recovery_buckets=1,
            requests_per_route=1,
        )
        first_ids = {
            (event["trace_id"], event["span_id"])
            for event in first.events
            if event["signal"] == "span"
        }
        replay_ids = {
            (event["trace_id"], event["span_id"])
            for event in replay.events
            if event["signal"] == "span"
        }
        self.assertTrue(first_ids)
        self.assertTrue(first_ids.isdisjoint(replay_ids))

    def test_all_scenarios_generate_backend_contract(self) -> None:
        for scenario in SCENARIOS:
            run = generate_run(
                scenario,
                seed=2,
                warmup_buckets=2,
                fault_buckets=2,
                recovery_buckets=1,
                requests_per_route=1,
            )
            self.assertTrue(run.events)
            self.assertTrue(all(event["signal"] in {"span", "metric", "log"} for event in run.events))
            batch = next(iter(run.batches(20)))
            self.assertEqual(batch["run_id"], run.run_id)
            self.assertLessEqual(len(batch["events"]), 20)
            # This is the real strict Pydantic API contract (extra fields forbidden).
            NormalizedBatch.model_validate(batch)

    def test_database_lock_replay_detects_and_ranks_causal_path(self) -> None:
        run = generate_run("database_lock", seed=4, requests_per_route=2)
        normalized = [{**event, "run_id": run.run_id} for event in run.events]
        spans = [event for event in normalized if event["signal"] == "span"]
        metrics = [event for event in normalized if event["signal"] == "metric"]
        logs = [event for event in normalized if event["signal"] == "log"]
        buckets = aggregate_feature_buckets(spans, metrics, logs)
        edges = build_dependency_edges(spans)
        graph = DirectedDependencyGraph(edges)
        scorer = StreamingAnomalyScorer(lookback=20, min_history=10)
        detector = IncidentDetector(min_consecutive=3, recovery_consecutive=2)
        all_results = []
        opened = []
        ordered = sorted(buckets, key=lambda item: (item.bucket_start, item.entity_id))
        for _, group in groupby(ordered, key=lambda item: item.bucket_start):
            results = [scorer.score(bucket) for bucket in group]
            all_results.extend(results)
            opened.extend(event.incident for event in detector.update(results) if event.action == "opened")
        self.assertTrue(opened, "the replay should create an incident")
        decision = HeuristicRootCauseRanker(min_margin=0).rank(opened[0], all_results, graph, top_k=10)
        ids = [ranking.entity_id for ranking in decision.rankings]
        self.assertIn("productcatalog->postgres", ids[:3], ids)
        self.assertTrue({"postgres", "productcatalog"} & set(ids[:3]), ids)

    def test_payment_failure_is_hit_at_three(self) -> None:
        for seed in (7, 202):
            with self.subTest(seed=seed):
                ids = self._rank_scenario("payment_failure", seed=seed)
                self.assertIn("payment", ids[:3], ids)
                self.assertIn("checkout->payment", ids[:3], ids)

    @staticmethod
    def _rank_scenario(scenario: str, seed: int) -> list[str]:
        run = generate_run(scenario, seed=seed, requests_per_route=2)
        normalized = [{**event, "run_id": run.run_id} for event in run.events]
        spans = [event for event in normalized if event["signal"] == "span"]
        metrics = [event for event in normalized if event["signal"] == "metric"]
        logs = [event for event in normalized if event["signal"] == "log"]
        buckets = aggregate_feature_buckets(spans, metrics, logs)
        graph = DirectedDependencyGraph(build_dependency_edges(spans))
        scorer = StreamingAnomalyScorer(lookback=20, min_history=10)
        detector = IncidentDetector(min_consecutive=3, recovery_consecutive=2)
        results, opened = [], []
        for _, group in groupby(sorted(buckets, key=lambda item: (item.bucket_start, item.entity_id)), key=lambda item: item.bucket_start):
            current = [scorer.score(bucket) for bucket in group]
            results.extend(current)
            opened.extend(event.incident for event in detector.update(current) if event.action == "opened")
        decision = HeuristicRootCauseRanker(min_margin=0).rank(opened[0], results, graph, top_k=10)
        return [ranking.entity_id for ranking in decision.rankings]


if __name__ == "__main__":
    unittest.main()
