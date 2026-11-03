from __future__ import annotations

import hashlib
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from rca.dataset import split_by_run
from rca.evaluation import (
    RankingExample,
    brier_score,
    evaluate_rankings,
    expected_calibration_error,
)
from rca.models import ChampionRanker, PortableLinearRanker
from rca.train import (
    apply_feature_profile,
    evaluate_model,
    main,
    partition_records,
    registered_model_matches,
    train_pipeline,
    verify_dataset_checksum,
)


class DatasetEvaluationModelTests(unittest.TestCase):
    def records(self):
        rows = []
        for run in range(12):
            for candidate in range(3):
                label = candidate == 0
                rows.append(
                    {
                        "run_id": f"run-{run}",
                        "incident_id": f"incident-{run}",
                        "candidate_id": f"service-{candidate}",
                        "fault_type": "latency" if run % 2 else "error",
                        "label": label,
                        "features": {"anomaly": 10 - candidate * 4, "upstream": 1 if label else 0},
                    }
                )
        return rows

    def test_run_split_has_no_leakage(self) -> None:
        split = split_by_run(self.records(), seed=7)
        split.assert_disjoint()
        self.assertEqual(len(split.train_runs | split.validation_runs | split.test_runs), 12)
        for partition in (split.train, split.validation, split.test):
            run_ids = {row["run_id"] for row in partition}
            self.assertTrue(all(row["run_id"] in run_ids for row in partition))

    def test_metrics_known_example(self) -> None:
        examples = [
            RankingExample(frozenset({"b"}), ("a", "b", "c")),
            RankingExample(frozenset({"x"}), ("x", "y")),
        ]
        metrics = evaluate_rankings(examples)
        self.assertEqual(metrics["hit_at_1"], 0.5)
        self.assertAlmostEqual(metrics["mrr"], 0.75)
        self.assertAlmostEqual(brier_score([1, 0], [1, 0]), 0)
        self.assertAlmostEqual(expected_calibration_error([0.8, 0.2], [1, 0], bins=2), 0.2)

    def test_portable_champion_learns_and_round_trips(self) -> None:
        records = self.records()
        model = ChampionRanker(prefer_sklearn=False).fit(records)
        scores = model.predict_scores(records[:3])
        self.assertGreater(scores[0], scores[1])
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "model.json"
            model.save(path)
            loaded = ChampionRanker.load(path)
            self.assertEqual(loaded.backend, "portable-linear")
            self.assertEqual(len(loaded.predict_scores(records[:3])), 3)

    def test_linear_ranker_rejects_single_class(self) -> None:
        with self.assertRaises(ValueError):
            PortableLinearRanker().fit([{"features": {"x": 1}, "label": True}])

    def test_declared_test_and_holdout_runs_never_enter_training(self) -> None:
        records = self.records()
        for row in records:
            run = row["run_id"]
            row["split"] = (
                "test" if run == "run-10" else "holdout" if run == "run-11" else "train"
            )
        split, holdout = partition_records(records, seed=7)
        self.assertNotIn("run-10", split.train_runs)
        self.assertNotIn("run-11", split.train_runs)
        self.assertEqual({row["run_id"] for row in split.test}, {"run-10"})
        self.assertEqual({row["run_id"] for row in holdout}, {"run-11"})

    def test_candidate_identity_includes_entity_type(self) -> None:
        class FixedScores:
            @staticmethod
            def predict_scores(records):
                return [0.9, 0.1]

        records = [
            {
                "run_id": "run-1",
                "incident_id": "incident-1",
                "candidate_id": "postgres",
                "candidate_type": "service",
                "label": 0,
                "features": {"x": 1},
            },
            {
                "run_id": "run-1",
                "incident_id": "incident-1",
                "candidate_id": "postgres",
                "candidate_type": "database",
                "label": 1,
                "features": {"x": 0},
            },
        ]
        report = evaluate_model(FixedScores(), records)  # type: ignore[arg-type]
        self.assertEqual(report["hit_at_1"], 0.0)
        self.assertLessEqual(report["ndcg_at_3"], 1.0)
        self.assertLessEqual(report["map_at_5"], 1.0)

    def test_split_membership_and_seed_are_recorded(self) -> None:
        _, split, report = train_pipeline(
            self.records(), prefer_sklearn=False, seed=19
        )
        self.assertEqual(report["seed"], 19)
        self.assertEqual(report["run_membership"]["train"], sorted(split.train_runs))
        self.assertEqual(
            set().union(*map(set, report["run_membership"].values())),
            {f"run-{index}" for index in range(12)},
        )

    def test_compact_feature_profile_is_explicit_and_leakage_safe(self) -> None:
        row = {
            "run_id": "run-1",
            "label": 1,
            "features": {
                "baseline_inverse_rank": 1.0,
                "baseline_probability": 0.7,
                "baseline_score": 0.9,
                "candidate_is_dependency_edge": 1.0,
                "candidate_is_service": 0.0,
                "evidence_abs_deviation_max": 123456.0,
            },
        }
        projected = apply_feature_profile([row], "compact")

        self.assertEqual(projected[0]["run_id"], "run-1")
        self.assertEqual(projected[0]["label"], 1)
        self.assertEqual(
            tuple(projected[0]["features"]),
            (
                "baseline_inverse_rank",
                "baseline_probability",
                "baseline_score",
                "candidate_is_dependency_edge",
                "candidate_is_service",
            ),
        )
        self.assertNotIn("evidence_abs_deviation_max", projected[0]["features"])
        with self.assertRaisesRegex(ValueError, "unsupported feature profile"):
            apply_feature_profile([row], "unknown")

    def test_training_rejects_dataset_changed_after_sidecar(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            dataset = Path(folder) / "records.jsonl"
            body = b'{"run_id":"run-1"}\n'
            dataset.write_bytes(body)
            metadata = dataset.with_suffix(dataset.suffix + ".metadata.json")
            metadata.write_text(
                json.dumps({"sha256": hashlib.sha256(body).hexdigest()}),
                encoding="utf-8",
            )
            self.assertEqual(verify_dataset_checksum(dataset), hashlib.sha256(body).hexdigest())
            dataset.write_bytes(body + b"corrupt\n")
            with self.assertRaises(ValueError):
                verify_dataset_checksum(dataset)

    def test_training_writes_challenger_without_publishing_stable_alias(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            dataset = root / "records.json"
            output = root / "champion"
            dataset.write_text(json.dumps(self.records()), encoding="utf-8")
            stdout = io.StringIO()
            with redirect_stdout(stdout):
                self.assertEqual(
                    main(
                        [
                            str(dataset),
                            "--output-dir",
                            str(output),
                            "--portable",
                        ]
                    ),
                    0,
                )
            result = json.loads(stdout.getvalue())
            immutable = Path(result["artifact"])
            self.assertIsNone(result["deployment_alias"])
            self.assertTrue(result["promotion_required"])
            self.assertFalse((output / "model.json").exists())
            self.assertIn(result["artifact_sha256"][:16], immutable.name)

    def test_model_registration_replay_ignores_lifecycle_status(self) -> None:
        for status in ("challenger", "champion", "retired"):
            self.assertTrue(
                registered_model_matches(
                    {
                        "status": status,
                        "artifact_checksum": "artifact-sha",
                        "dataset_snapshot_id": "snapshot-id",
                    },
                    artifact_checksum="artifact-sha",
                    dataset_snapshot_id="snapshot-id",
                )
            )
        self.assertFalse(
            registered_model_matches(
                {
                    "status": "champion",
                    "artifact_checksum": "different",
                    "dataset_snapshot_id": "snapshot-id",
                },
                artifact_checksum="artifact-sha",
                dataset_snapshot_id="snapshot-id",
            )
        )


if __name__ == "__main__":
    unittest.main()
