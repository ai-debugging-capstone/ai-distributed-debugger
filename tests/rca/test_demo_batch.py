from __future__ import annotations

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock
from uuid import uuid4

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = PROJECT_ROOT / "scripts"


def _load(name: str, filename: str):
    if str(SCRIPTS) not in sys.path:
        sys.path.insert(0, str(SCRIPTS))
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    # Dataclasses resolve annotations through sys.modules, and the batch
    # imports demo_experiment by name, so both must share this registration.
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


demo_experiment = _load("demo_experiment", "demo_experiment.py")
batch = _load("run_demo_batch_under_test", "run_demo_batch.py")

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is optional on the host
    yaml = None


class ScenarioMirrorTests(unittest.TestCase):
    @unittest.skipIf(yaml is None, "PyYAML is not installed")
    def test_runtime_mirror_matches_experiment_definition(self) -> None:
        document = yaml.safe_load(
            (PROJECT_ROOT / "experiments" / "otel-demo-v3.1.0.yaml").read_text(encoding="utf-8")
        )
        for scenario in document["scenarios"]:
            mirror = demo_experiment.SCENARIOS[scenario["flag"]]
            self.assertEqual(list(mirror["variants"]), [str(v) for v in scenario["variants"]])
            self.assertEqual(mirror["hold_seconds"], scenario["hold_seconds"])
            self.assertEqual(mirror["entity_id"], scenario["causal_entity"]["id"])
            self.assertEqual(mirror["mechanism"], scenario["mechanism"])
        for control in document["healthy_controls"]:
            mirror = demo_experiment.HEALTHY_CONTROLS[control["key"]]
            for field in ("workload", "hold_seconds", "users_multiplier"):
                self.assertEqual(mirror[field], control[field])


class HealthyControlProtocolTests(unittest.TestCase):
    def setUp(self) -> None:
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.manifest = Path(directory.name) / "control.json"

    def start(self, **overrides) -> None:
        values = dict(
            file=self.manifest,
            experiment_id=str(uuid4()),
            commit="dedc0178918e260823323b8d95005a8cb924b007",
            profile="full",
            seed=7,
            split="unassigned",
            scenario="healthy-high-load",
            variant="none",
            original_variant="none",
            warmup=180,
            duration=600,
            recovery=120,
        )
        values.update(overrides)
        demo_experiment.start_manifest(argparse.Namespace(**values))

    def to_recovery(self) -> None:
        self.start()
        demo_experiment.mark_reset(argparse.Namespace(file=self.manifest))
        for status in ("fault_active", "recovering"):
            demo_experiment.mark_manifest(argparse.Namespace(file=self.manifest, status=status))

    def test_control_rejects_a_flag_variant(self) -> None:
        with self.assertRaisesRegex(SystemExit, "changes no flag"):
            self.start(variant="on")

    def test_fault_window_is_only_ever_added_to_configuration(self) -> None:
        # The API rejects changing an existing configuration key, so a sync
        # must never send a null boundary that a later sync would overwrite.
        self.start()
        demo_experiment.mark_reset(argparse.Namespace(file=self.manifest))
        sent = []
        with mock.patch.object(
            demo_experiment, "request_json", side_effect=lambda *a: sent.append(a[2]) or {}
        ):
            for status in (None, "fault_active", "recovering"):
                if status:
                    demo_experiment.mark_manifest(
                        argparse.Namespace(file=self.manifest, status=status)
                    )
                demo_experiment.sync_api(
                    argparse.Namespace(file=self.manifest, api_url="http://example.invalid")
                )
        configurations = [payload["configuration"] for payload in sent]
        for earlier, later in zip(configurations, configurations[1:]):
            for key, value in earlier.items():
                self.assertEqual(later.get(key), value, key)
        self.assertNotIn("fault_started_at", configurations[0])
        self.assertIn("fault_ended_at", configurations[-1])

    def test_control_registers_run_without_fault_or_labels(self) -> None:
        self.start()
        demo_experiment.mark_reset(argparse.Namespace(file=self.manifest))
        demo_experiment.mark_manifest(argparse.Namespace(file=self.manifest, status="fault_active"))
        demo_experiment.mark_manifest(argparse.Namespace(file=self.manifest, status="recovering"))
        with mock.patch.object(demo_experiment, "request_json", return_value={}) as request:
            demo_experiment.sync_api(
                argparse.Namespace(file=self.manifest, api_url="http://example.invalid")
            )
        self.assertEqual(len(request.call_args_list), 1)
        payload = request.call_args.args[2]
        self.assertEqual(payload["workload"], "sustained-high-volume")
        self.assertEqual(payload["configuration"]["kind"], "control")
        self.assertIsNone(payload["configuration"]["flag"])
        self.assertFalse(payload["configuration"]["training_eligible"])
        # The worker gates detection on this window, by bucket time.
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        self.assertEqual(
            payload["configuration"]["fault_started_at"], manifest["fault_started_at"]
        )
        self.assertEqual(payload["configuration"]["fault_ended_at"], manifest["fault_ended_at"])

    def test_control_acceptance_counts_incidents_as_false_positives(self) -> None:
        self.to_recovery()
        summary = {
            "telemetry": {"spans": 10, "metric_points": 10, "log_records": 10},
            "incidents": [{"id": "a"}, {"id": "b"}],
            "candidates": [{"rank": 1, "entity_type": "service", "entity_id": "flagd"}],
        }
        with mock.patch.object(demo_experiment, "request_json", return_value=summary):
            demo_experiment.accept_capture(
                argparse.Namespace(file=self.manifest, api_url="http://example.invalid", top_k=3)
            )
        manifest = json.loads(self.manifest.read_text(encoding="utf-8"))
        self.assertTrue(manifest["acceptance"]["passed"])
        self.assertEqual(manifest["acceptance"]["false_positive_incidents"], 2)
        self.assertFalse(manifest["training_eligible"])
        demo_experiment.mark_manifest(argparse.Namespace(file=self.manifest, status="completed"))
        completed = json.loads(self.manifest.read_text(encoding="utf-8"))
        self.assertTrue(completed["capture_valid"])
        self.assertFalse(completed["training_eligible"])

    def test_bursty_hold_always_restores_the_baseline(self) -> None:
        calls: list[int] = []
        with (
            mock.patch.object(demo_experiment, "locust_user_count", return_value=10),
            mock.patch.object(
                demo_experiment, "set_locust_users", side_effect=lambda _url, users: calls.append(users)
            ),
            mock.patch.object(demo_experiment.time, "sleep", side_effect=KeyboardInterrupt),
            self.assertRaises(KeyboardInterrupt),
        ):
            demo_experiment.hold_workload(
                argparse.Namespace(
                    scenario="healthy-bursty-load",
                    seconds=600,
                    loadgen_url="http://example.invalid",
                    baseline_users=None,
                )
            )
        self.assertEqual(calls, [60, 10])

    def test_runners_hold_controls_instead_of_editing_flags(self) -> None:
        bash = (SCRIPTS / "run-demo-experiment.sh").read_text(encoding="utf-8")
        powershell = (SCRIPTS / "run-demo-experiment.ps1").read_text(encoding="utf-8")
        for text in (bash, powershell):
            self.assertIn("scenario-kind", text)
            self.assertIn("hold-workload", text)
            self.assertLess(text.index("--status fault_active"), text.index("hold-workload"))
            self.assertLess(text.index("hold-workload"), text.index("--status recovering"))


class BatchPlanTests(unittest.TestCase):
    def test_split_shares_keep_every_group_evaluable(self) -> None:
        self.assertEqual(batch.assign_splits(1), ["train"])
        self.assertEqual(batch.assign_splits(2), ["train", "test"])
        self.assertEqual(batch.assign_splits(3), ["train", "validation", "test"])
        five = batch.assign_splits(5)
        self.assertEqual((five.count("train"), five.count("validation"), five.count("test")), (3, 1, 1))

    def test_plan_is_deterministic_run_level_and_round_robin(self) -> None:
        arguments = dict(
            scenarios=["paymentFailure", "cartFailure"],
            controls=["healthy-normal-load"],
            variants="strongest",
            seeds=3,
            base_seed=100,
        )
        first = batch.build_plan(**arguments)
        self.assertEqual(first, batch.build_plan(**arguments))
        self.assertEqual(len({run.key for run in first}), len(first))
        self.assertEqual([run.seed for run in first[:3]], [100, 100, 100])
        self.assertEqual(first[0].kind, "control")
        self.assertEqual({run.variant for run in first if run.scenario == "cartFailure"}, {"100%"})
        self.assertEqual({run.split for run in first if run.kind == "control"}, {"unassigned"})
        payment = sorted(run.split for run in first if run.scenario == "paymentFailure")
        self.assertEqual(payment, ["test", "train", "validation"])

    def test_all_variants_and_whole_scenario_holdout(self) -> None:
        plan = batch.build_plan(
            scenarios=["paymentFailure", "paymentUnreachable"],
            controls=[],
            variants="all",
            seeds=1,
            base_seed=1,
            holdout_scenarios=["paymentUnreachable"],
        )
        self.assertEqual(len([run for run in plan if run.scenario == "paymentFailure"]), 6)
        self.assertEqual(
            {run.split for run in plan if run.scenario == "paymentUnreachable"}, {"holdout"}
        )

    def test_profile_and_platform_filtering(self) -> None:
        core = batch.runnable_scenarios("core")
        self.assertNotIn("kafkaQueueProblems", core)
        self.assertNotIn("failedReadinessProbe", core)
        self.assertIn("kafkaQueueProblems", batch.runnable_scenarios("full"))

    def test_resume_state_uses_latest_attempt(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "state.jsonl"
            batch.append_state(path, {"key": "a", "status": "failed"})
            batch.append_state(path, {"key": "a", "status": "completed"})
            batch.append_state(path, {"key": "b", "status": "failed"})
            state = batch.read_state(path)
        self.assertEqual(state["a"]["status"], "completed")
        self.assertEqual(state["b"]["status"], "failed")

    def test_short_warmup_is_refused(self) -> None:
        with self.assertRaisesRegex(SystemExit, "baseline before the fault"):
            batch.main(["--dry-run", "--warmup", "60"])

    def test_runner_commands_forward_the_planned_run(self) -> None:
        run = batch.PlannedRun("k", "paymentFailure", "fault", "100%", 9, "train", 300)
        bash = batch.runner_command("bash", run, warmup=180, recovery=120, top_k=3)
        self.assertIn("--duration", bash)
        self.assertEqual(bash[bash.index("--variant") + 1], "100%")
        powershell = batch.runner_command("powershell", run, warmup=180, recovery=120, top_k=3)
        self.assertEqual(powershell[powershell.index("-Split") + 1], "train")
        self.assertEqual(powershell[powershell.index("-DurationSeconds") + 1], "300")

    def test_summary_separates_hits_from_false_positives(self) -> None:
        summary = batch.summarize(
            [
                {"kind": "fault", "scenario": "paymentFailure", "outcome": {"accepted": True, "matched_rank": 1}},
                {"kind": "fault", "scenario": "paymentFailure", "outcome": {"accepted": False}},
                {
                    "kind": "control",
                    "scenario": "healthy-normal-load",
                    "outcome": {"capture_valid": True, "false_positive_incidents": 2},
                },
            ]
        )
        self.assertEqual(summary["faults"]["paymentFailure"], {"runs": 2, "accepted": 1, "hit_at_1": 1})
        self.assertEqual(summary["controls"]["healthy-normal-load"]["false_positive_incidents"], 2)


if __name__ == "__main__":
    unittest.main()
