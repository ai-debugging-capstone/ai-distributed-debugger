from __future__ import annotations

import logging
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

from rca.candidate_features import candidate_feature_map
from rca.domain import CandidateRanking, EvidenceItem, Incident
from rca.graph import DirectedDependencyGraph
from rca.models import ChampionRanker
from rca.online_ranking import (
    ChampionRerankingRootCauseRanker,
    LoadedChampionArtifact,
    load_champion_artifact,
)
from rca.ranking import RankerDecision


class _CapturingModel:
    def __init__(self, scores):
        self.scores = scores
        self.records = []

    def predict_scores(self, records):
        self.records = list(records)
        if isinstance(self.scores, Exception):
            raise self.scores
        return list(self.scores)


class _FixedBaseline:
    def __init__(self, decision):
        self.decision = decision
        self.calls = 0

    def rank(self, incident, anomalies, graph, *, top_k=10):
        self.calls += 1
        return self.decision


class OnlineRankingTests(unittest.TestCase):
    def _decision(self) -> RankerDecision:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        evidence_a = EvidenceItem(
            "signal_anomaly", "checkout", now, "error_rate", 0.8, 0.02, 0.5,
            {"robust_z": 8.0},
        )
        evidence_b = EvidenceItem(
            "dependency_health", "checkout->postgres", now, "edge_error_rate",
            0.2, 0.0, 0.4, {"request_count": 40},
        )
        return RankerDecision(
            (
                CandidateRanking(
                    "service", "checkout", 1, 0.8, 0.7, "high", "service_error",
                    "anomaly=0.8", (evidence_a,), "heuristic-v1",
                ),
                CandidateRanking(
                    "dependency_edge", "checkout->postgres", 2, 0.6, 0.2, "medium",
                    "database_lock", "anomaly=0.6", (evidence_b,), "heuristic-v1",
                ),
            ),
            False,
            None,
            0.9,
        )

    def _incident(self) -> Incident:
        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        return Incident(now, now, "checkout", ["checkout"], 8.0, "critical", run_id="run-1")

    def test_valid_portable_artifact_loads_with_checksum_version(self) -> None:
        rows = [
            {"features": {"baseline_score": 0.9}, "label": 1},
            {"features": {"baseline_score": 0.1}, "label": 0},
        ]
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "model.json"
            ChampionRanker(prefer_sklearn=False).fit(rows).save(target)
            loaded = load_champion_artifact(target)
        self.assertIsNotNone(loaded)
        assert loaded is not None
        self.assertTrue(loaded.model_version.startswith("champion-portable-linear-sha256:"))
        self.assertEqual(len(loaded.sha256), 64)

    def test_missing_or_leaky_artifact_falls_back_without_raising(self) -> None:
        with self.assertLogs("rca.online_ranking", logging.WARNING) as captured:
            self.assertIsNone(load_champion_artifact("definitely-missing-model.json"))
        self.assertIn("using heuristic-v1", " ".join(captured.output))

        rows = [
            {"features": {"label": 1.0}, "label": 1},
            {"features": {"label": 0.0}, "label": 0},
        ]
        with tempfile.TemporaryDirectory() as folder:
            target = Path(folder) / "leaky.json"
            ChampionRanker(prefer_sklearn=False).fit(rows).save(target)
            with self.assertLogs("rca.online_ranking", logging.WARNING) as captured:
                loaded = load_champion_artifact(target)
        self.assertIsNone(loaded)
        self.assertIn("offline-only", " ".join(captured.output))

    def test_champion_reranks_while_preserving_evidence_and_fault_class(self) -> None:
        baseline_decision = self._decision()
        baseline = _FixedBaseline(baseline_decision)
        model = _CapturingModel([0.1, 0.9])
        artifact = LoadedChampionArtifact(
            model, Path("model.json"), "champion-test-sha256:abc", "abc"  # type: ignore[arg-type]
        )
        ranker = ChampionRerankingRootCauseRanker(
            artifact, baseline=baseline  # type: ignore[arg-type]
        )
        decision = ranker.rank(self._incident(), [], DirectedDependencyGraph())

        self.assertEqual([item.entity_id for item in decision.rankings], [
            "checkout->postgres", "checkout"
        ])
        promoted = decision.rankings[0]
        original = baseline_decision.rankings[1]
        self.assertEqual(promoted.fault_class, "database_lock")
        self.assertEqual(promoted.evidence, original.evidence)
        self.assertEqual(promoted.model_version, "champion-test-sha256:abc")
        self.assertIn("heuristic_rank=2", promoted.evidence_summary)
        self.assertEqual(promoted.online_features["baseline_score"], 0.6)
        self.assertEqual(promoted.online_features["baseline_probability"], 0.2)
        self.assertEqual(promoted.online_features["baseline_inverse_rank"], 0.5)
        self.assertNotIn("label", model.records[0])
        self.assertNotIn("fault_type", model.records[0])

    def test_ties_are_deterministic_and_keep_baseline_order(self) -> None:
        baseline_decision = self._decision()
        model = _CapturingModel([0.5, 0.5])
        artifact = LoadedChampionArtifact(
            model, Path("model.json"), "champion-test", "abc"  # type: ignore[arg-type]
        )
        ranker = ChampionRerankingRootCauseRanker(
            artifact, baseline=_FixedBaseline(baseline_decision)  # type: ignore[arg-type]
        )
        decision = ranker.rank(self._incident(), [], DirectedDependencyGraph())
        self.assertEqual([item.entity_id for item in decision.rankings], [
            "checkout", "checkout->postgres"
        ])

    def test_runtime_model_failure_disables_champion_and_returns_baseline(self) -> None:
        baseline_decision = self._decision()
        baseline = _FixedBaseline(baseline_decision)
        model = _CapturingModel(RuntimeError("bad artifact"))
        artifact = LoadedChampionArtifact(
            model, Path("model.json"), "champion-test", "abc"  # type: ignore[arg-type]
        )
        ranker = ChampionRerankingRootCauseRanker(
            artifact, baseline=baseline  # type: ignore[arg-type]
        )
        with self.assertLogs("rca.online_ranking", logging.WARNING) as captured:
            first = ranker.rank(self._incident(), [], DirectedDependencyGraph())
        second = ranker.rank(self._incident(), [], DirectedDependencyGraph())
        self.assertEqual(first, baseline_decision)
        self.assertEqual(second, baseline_decision)
        self.assertEqual(baseline.calls, 2)
        self.assertIn("disabling it", " ".join(captured.output))

    def test_shared_candidate_builder_has_stable_online_shape(self) -> None:
        features = candidate_feature_map(
            {
                "entity_type": "dependency-edge",
                "rank": 2,
                "score": 0.6,
                "probability": 0.2,
                "confidence": "medium",
            },
            [{
                "evidence_type": "signal anomaly",
                "signal_name": "latency_ms_p95",
                "value": 120,
                "baseline": 20,
                "contribution": 0.4,
                "details": {"robust_z": 7},
            }],
        )
        self.assertEqual(features["candidate_is_dependency_edge"], 1.0)
        self.assertEqual(features["baseline_inverse_rank"], 0.5)
        self.assertEqual(features["evidence_abs_deviation_max"], 100.0)
        self.assertEqual(features["evidence_max_abs_robust_z"], 7.0)

    def test_stored_online_snapshot_wins_over_post_rerank_fields(self) -> None:
        features = candidate_feature_map(
            {
                "entity_type": "service",
                "rank": 1,
                "score": 0.99,
                "probability": 0.98,
                "confidence": "high",
            },
            [],
            stored_features={
                "baseline_score": 0.31,
                "baseline_probability": 0.22,
                "baseline_inverse_rank": 0.25,
            },
        )
        self.assertEqual(
            features,
            {
                "baseline_score": 0.31,
                "baseline_probability": 0.22,
                "baseline_inverse_rank": 0.25,
            },
        )
        with self.assertRaises(ValueError):
            candidate_feature_map({}, [], stored_features={"ground_truth_label": 1})


if __name__ == "__main__":
    unittest.main()
