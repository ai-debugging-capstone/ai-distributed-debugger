from __future__ import annotations

import argparse
import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[2]
MODULE_PATH = PROJECT_ROOT / "scripts" / "demo_experiment.py"
SPEC = importlib.util.spec_from_file_location("demo_experiment_under_test", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
demo_experiment = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(demo_experiment)


class DemoExperimentProvenanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.manifest_path = Path(self.temporary_directory.name) / "run.json"

    def start_arguments(self, *, seed: int = 31415) -> argparse.Namespace:
        return argparse.Namespace(
            file=self.manifest_path,
            experiment_id=str(uuid4()),
            commit="dedc0178918e260823323b8d95005a8cb924b007",
            profile="full",
            seed=seed,
            split="train",
            scenario="paymentFailure",
            variant="50%",
            original_variant="off",
            warmup=60,
            duration=120,
            recovery=60,
        )

    def test_manifest_records_truthful_workload_and_capture_seed(self) -> None:
        demo_experiment.start_manifest(self.start_arguments())

        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(manifest["seed"], 31415)
        self.assertEqual(manifest["split"], "train")
        self.assertEqual(manifest["seed_semantics"], "capture_partition_seed")
        self.assertEqual(manifest["workload"]["actual"], "mixed-commerce")
        self.assertEqual(
            manifest["workload"]["intended_scenario_exercise"],
            "repeated-checkout",
        )
        self.assertFalse(manifest["upstream_workload_rng"]["controlled"])
        self.assertTrue(manifest["reset_between_runs"])
        self.assertIsNone(manifest["reset_completed_at"])
        self.assertIsNone(manifest["started_at"])
        self.assertEqual(manifest["status"], "reset_pending")

    def test_sync_refuses_registration_until_reset_is_recorded(self) -> None:
        demo_experiment.start_manifest(self.start_arguments())

        with (
            mock.patch.object(demo_experiment, "request_json") as request,
            self.assertRaisesRegex(SystemExit, "before the Demo reset"),
        ):
            demo_experiment.sync_api(
                argparse.Namespace(
                    file=self.manifest_path, api_url="http://example.invalid"
                )
            )
        request.assert_not_called()

    def test_api_payload_separates_actual_workload_from_intended_exercise(self) -> None:
        demo_experiment.start_manifest(self.start_arguments(seed=2718))
        with mock.patch.object(
            demo_experiment, "utc_now", return_value="2026-10-04T12:00:00Z"
        ):
            demo_experiment.mark_reset(argparse.Namespace(file=self.manifest_path))

        with mock.patch.object(
            demo_experiment, "request_json", return_value={}
        ) as request:
            demo_experiment.sync_api(
                argparse.Namespace(
                    file=self.manifest_path, api_url="http://example.invalid/"
                )
            )

        request.assert_called_once()
        method, url, payload = request.call_args.args
        self.assertEqual(method, "POST")
        self.assertTrue(url.endswith("/api/v1/experiments"))
        self.assertEqual(payload["seed"], 2718)
        self.assertEqual(payload["split"], "train")
        self.assertEqual(payload["workload"], "mixed-commerce")
        self.assertEqual(payload["configuration"]["capture_seed"], 2718)
        self.assertEqual(
            payload["configuration"]["intended_scenario_exercise"],
            "repeated-checkout",
        )
        self.assertFalse(payload["configuration"]["upstream_workload_rng_controlled"])
        self.assertEqual(payload["environment"]["capture_seed"], 2718)
        self.assertEqual(
            payload["environment"]["upstream_workload_rng"], "uncontrolled"
        )
        self.assertEqual(
            payload["configuration"]["reset_completed_at"],
            "2026-10-04T12:00:00Z",
        )
        self.assertEqual(payload["started_at"], "2026-10-04T12:00:00Z")

    def test_invalid_capture_seed_is_rejected(self) -> None:
        with self.assertRaisesRegex(SystemExit, "Capture seed"):
            demo_experiment.start_manifest(self.start_arguments(seed=-1))
        self.assertFalse(self.manifest_path.exists())

    def move_to_recovery(self) -> None:
        demo_experiment.start_manifest(self.start_arguments())
        timestamps = iter(
            (
                "2026-10-04T12:00:00Z",
                "2026-10-04T12:01:00Z",
                "2026-10-04T12:03:00Z",
            )
        )
        with mock.patch.object(
            demo_experiment, "utc_now", side_effect=lambda: next(timestamps)
        ):
            demo_experiment.mark_reset(argparse.Namespace(file=self.manifest_path))
            demo_experiment.mark_manifest(
                argparse.Namespace(file=self.manifest_path, status="fault_active")
            )
            demo_experiment.mark_manifest(
                argparse.Namespace(file=self.manifest_path, status="recovering")
            )

    @staticmethod
    def accepted_summary(*, rank: int = 2, logs: int = 8) -> dict:
        return {
            "run_id": str(uuid4()),
            "telemetry": {
                "spans": 120,
                "metric_points": 55,
                "log_records": logs,
            },
            "derived": {"feature_buckets": 20, "dependency_edges": 9},
            "incidents": [{"id": str(uuid4()), "status": "resolved"}],
            "candidates": [
                {
                    "incident_id": str(uuid4()),
                    "rank": rank,
                    "entity_type": "service",
                    "entity_id": "payment",
                    "score": 0.81,
                }
            ],
        }

    def test_capture_acceptance_precedes_completion_and_freezes_evidence(self) -> None:
        self.move_to_recovery()
        summary = self.accepted_summary()
        with (
            mock.patch.object(
                demo_experiment, "request_json", return_value=summary
            ) as request,
            mock.patch.object(
                demo_experiment, "utc_now", return_value="2026-10-04T12:04:00Z"
            ),
        ):
            demo_experiment.accept_capture(
                argparse.Namespace(
                    file=self.manifest_path,
                    api_url="http://example.invalid/",
                    top_k=3,
                )
            )

        request.assert_called_once_with(
            "GET",
            mock.ANY,
        )
        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertTrue(manifest["acceptance"]["passed"])
        self.assertEqual(manifest["acceptance"]["top_k"], 3)
        self.assertEqual(manifest["acceptance"]["telemetry"], summary["telemetry"])
        self.assertTrue(manifest["training_eligible"])
        demo_experiment.mark_manifest(
            argparse.Namespace(file=self.manifest_path, status="completed")
        )
        completed = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertEqual(completed["status"], "completed")
        self.assertTrue(completed["capture_valid"])

    def test_capture_acceptance_fails_closed_for_missing_signal(self) -> None:
        self.move_to_recovery()
        with (
            mock.patch.object(
                demo_experiment,
                "request_json",
                return_value=self.accepted_summary(logs=0),
            ),
            self.assertRaisesRegex(SystemExit, "no persisted log_records"),
        ):
            demo_experiment.accept_capture(
                argparse.Namespace(
                    file=self.manifest_path,
                    api_url="http://example.invalid",
                    top_k=3,
                )
            )

        rejected = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertFalse(rejected["acceptance"]["passed"])
        self.assertFalse(rejected["training_eligible"])
        self.assertFalse(rejected["capture_valid"])
        with self.assertRaisesRegex(SystemExit, "has not passed acceptance"):
            demo_experiment.mark_manifest(
                argparse.Namespace(file=self.manifest_path, status="completed")
            )

        demo_experiment.mark_manifest(
            argparse.Namespace(file=self.manifest_path, status="aborted")
        )
        with mock.patch.object(
            demo_experiment, "request_json", return_value={}
        ) as request:
            demo_experiment.sync_api(
                argparse.Namespace(
                    file=self.manifest_path, api_url="http://example.invalid"
                )
            )
        terminal_payload = request.call_args_list[0].args[2]
        self.assertEqual(terminal_payload["status"], "failed")
        self.assertFalse(terminal_payload["configuration"]["training_eligible"])
        self.assertFalse(terminal_payload["configuration"]["capture_valid"])
        self.assertIn(
            "no persisted log_records",
            terminal_payload["configuration"]["failure_reason"],
        )

    def test_capture_accepts_declared_causal_dependency_edge(self) -> None:
        self.move_to_recovery()
        summary = self.accepted_summary()
        summary["candidates"] = [
            {
                "incident_id": str(uuid4()),
                "rank": 1,
                "entity_type": "dependency_edge",
                "entity_id": "checkout->payment",
                "score": 0.91,
            }
        ]
        with mock.patch.object(
            demo_experiment, "request_json", return_value=summary
        ):
            demo_experiment.accept_capture(
                argparse.Namespace(
                    file=self.manifest_path,
                    api_url="http://example.invalid",
                    top_k=3,
                )
            )

        manifest = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertTrue(manifest["acceptance"]["passed"])
        self.assertEqual(
            manifest["acceptance"]["matched_candidate"]["entity_id"],
            "checkout->payment",
        )
        self.assertIn(
            {
                "entity_type": "dependency_edge",
                "entity_id": "checkout->payment",
            },
            manifest["acceptance"]["expected_candidates"],
        )

    def test_capture_acceptance_rank_threshold_is_configurable(self) -> None:
        self.move_to_recovery()
        with (
            mock.patch.object(
                demo_experiment,
                "request_json",
                return_value=self.accepted_summary(rank=4),
            ),
            self.assertRaisesRegex(SystemExit, "not ranked in top 3"),
        ):
            demo_experiment.accept_capture(
                argparse.Namespace(
                    file=self.manifest_path,
                    api_url="http://example.invalid",
                    top_k=3,
                )
            )

        # Rechecking the same still-recovering run with an explicitly wider
        # threshold is allowed and replaces the failed pre-terminal verdict.
        with mock.patch.object(
            demo_experiment,
            "request_json",
            return_value=self.accepted_summary(rank=4),
        ):
            demo_experiment.accept_capture(
                argparse.Namespace(
                    file=self.manifest_path,
                    api_url="http://example.invalid",
                    top_k=5,
                )
            )
        accepted = json.loads(self.manifest_path.read_text(encoding="utf-8"))
        self.assertTrue(accepted["acceptance"]["passed"])
        self.assertEqual(accepted["acceptance"]["top_k"], 5)

    def test_runners_reset_and_mark_before_first_runtime_registration(self) -> None:
        powershell = (PROJECT_ROOT / "scripts" / "run-demo-experiment.ps1").read_text(
            encoding="utf-8"
        )
        ps_runtime = powershell.split("try {", maxsplit=1)[1]
        self.assertLess(
            ps_runtime.index('@("restart")'), ps_runtime.index("mark-reset")
        )
        self.assertLess(ps_runtime.index("mark-reset"), ps_runtime.index("sync-api"))
        self.assertIn("--seed $Seed", powershell)
        self.assertIn("--split $Split", powershell)
        self.assertIn('[string]$Split = "unassigned"', powershell)
        ps_fault = powershell.index("--status fault_active")
        ps_enable = powershell.index(
            "set-variant --file $flagFile --flag $Scenario --variant $Variant"
        )
        ps_recover = powershell.index("--status recovering")
        ps_restore = powershell.index(
            "set-variant --file $flagFile --flag $Scenario --variant $originalVariant"
        )
        ps_accept = powershell.index("accept-capture")
        ps_complete = powershell.index("--status completed")
        self.assertLess(ps_fault, ps_enable)
        self.assertLess(ps_recover, ps_restore)
        self.assertLess(ps_accept, ps_complete)
        self.assertNotIn('Invoke-Docker -Arguments ($compose + @("restart", "flagd"))', powershell)
        self.assertIn("[int]$AcceptanceTopK = 3", powershell)
        # PowerShell unwraps a one-item pipeline into a scalar. The outer @(...)
        # must cover the complete filtering pipeline because containerIds is
        # normally exactly one ID and Wait-DemoReady reads its Count property.
        self.assertIn("$requiredServices = @(\n        & docker", powershell)
        self.assertIn("$containerIds = @(\n                & docker", powershell)
        self.assertNotIn("$containerIds = @(& docker", powershell)

        bash = (PROJECT_ROOT / "scripts" / "run-demo-experiment.sh").read_text(
            encoding="utf-8"
        )
        bash_runtime = bash.split("trap 'exit 143' TERM", maxsplit=1)[1]
        self.assertLess(bash_runtime.index("restart"), bash_runtime.index("mark-reset"))
        self.assertLess(
            bash_runtime.index("mark-reset"), bash_runtime.index("sync-api")
        )
        self.assertIn('--seed "${seed}"', bash)
        self.assertIn('--split "${split}"', bash)
        bash_fault = bash.index("--status fault_active")
        bash_enable = bash.index(
            'set-variant --file "${flag_file}" --flag "${scenario}" --variant "${variant}"'
        )
        bash_recover = bash.index("--status recovering")
        bash_restore = bash.index(
            'set-variant --file "${flag_file}" --flag "${scenario}" --variant "${original_variant}"'
        )
        bash_accept = bash.index("accept-capture")
        bash_complete = bash.index("--status completed")
        self.assertLess(bash_fault, bash_enable)
        self.assertLess(bash_recover, bash_restore)
        self.assertLess(bash_accept, bash_complete)
        bash_capture = bash[bash_fault:bash_accept]
        self.assertNotIn("restart flagd", bash_capture)
        self.assertIn("--acceptance-top-k", bash)


if __name__ == "__main__":
    unittest.main()
