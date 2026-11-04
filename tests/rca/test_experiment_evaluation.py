"""Offline scoring contracts; fixtures are diagnoses, not measured model accuracy."""

import copy
import json
import tempfile
from pathlib import Path
from uuid import UUID

import pytest

from rca import experiment_evaluation as evaluation
from scripts.demo_experiment import SCENARIOS, expected_causal_candidates


@pytest.fixture
def evaluation_dir():
    # Match existing tests: avoid a shared pytest temp root owned by another Windows identity.
    with tempfile.TemporaryDirectory(prefix="m6-evaluation-") as folder:
        yield Path(folder)


def manifest(scenario="paymentFailure", number=1, status="completed"):
    return {
        "experiment_id": str(UUID(int=number)), "scenario": scenario, "profile": "full",
        "variant": "on" if scenario == "adFailure" else "100%", "seed": 42,
        "split": "holdout", "status": status,
        "fault_started_at": "2026-01-01T00:01:00Z", "fault_ended_at": "2026-01-01T00:03:00Z",
        "capture_valid": False, "training_eligible": False,
    }


def summary(m, ids=("payment", "checkout", "frontend", "cart")):
    return {
        "run_id": m["experiment_id"],
        "telemetry": {"spans": 10, "metric_points": 10, "log_records": 10},
        "incidents": [{"id": "incident-1"}],
        "candidates": [
            {"incident_id": "incident-1", "rank": rank, "entity_type": "service",
             "entity_id": entity, "model_version": "fixture-only"}
            for rank, entity in enumerate(ids, 1)
        ],
    }


@pytest.mark.parametrize("scenario,ids,rank,hit1,hit3,status", [
    ("paymentFailure", ("payment", "cart", "ad"), 1, 1, 1, "passed"),
    ("cartFailure", ("payment", "ad", "cart"), 3, 0, 1, "passed"),
    ("adFailure", ("payment", "cart", "frontend", "ad"), 4, 0, 0, "failed"),
    ("paymentFailure", ("cart", "ad"), None, 0, 0, "failed"),
])
def test_known_failures_and_rank_cutoffs(scenario, ids, rank, hit1, hit3, status):
    m = manifest(scenario, status="aborted")  # Never select only accepted captures.
    result = evaluation.evaluate_capture(m, summary(m, ids))
    row = result["diagnoses"][0]
    assert (row["expected_rank"], row["hit_at_1"], row["hit_at_3"], row["status"]) == (rank, hit1, hit3, status)
    assert result["expected_root_cause"] == expected_causal_candidates(SCENARIOS[scenario])[0]


@pytest.mark.parametrize("scenario", ["paymentFailure", "paymentUnreachable"])
def test_declared_edge_is_exact_alternative_not_arbitrary_upstream_service(scenario):
    m = manifest(scenario)
    s = summary(m, ("checkout->payment",))
    s["candidates"][0]["entity_type"] = "dependency_edge"
    assert evaluation.evaluate_capture(m, s)["hit_at_1"] == 1
    s["candidates"][0]["entity_type"] = "service"
    assert evaluation.evaluate_capture(m, s)["hit_at_3"] == 0


@pytest.mark.parametrize("change", [
    lambda s: s.update(run_id=str(UUID(int=9))),
    lambda s: s["telemetry"].update(log_records=0),
    lambda s: s["telemetry"].update(spans=True),
    lambda s: s.update(incidents=[]),
    lambda s: s.update(candidates=[]),
    lambda s: s["candidates"][0].update(incident_id="other"),
    lambda s: s["candidates"][0].update(rank=0),
    lambda s: s["candidates"][0].update(rank=-1),
    lambda s: s["candidates"][0].update(rank=True),
    lambda s: s["candidates"][0].update(rank="1"),
    lambda s: s["candidates"][0].update(rank=1.5),
    lambda s: s["candidates"][1].update(rank=1),
    lambda s: s["candidates"][1].update(entity_id="payment"),
    lambda s: s["candidates"][0].pop("entity_id"),
    lambda s: s["incidents"].append({"id": "incident-1"}),
    lambda s: s.update(incidents=[{"id": []}]),
    lambda s: s.update(candidates=[None]),
])
def test_missing_or_malformed_output_is_inconclusive_and_counts_as_zero(change):
    m = manifest()
    s = summary(m)
    change(s)
    result = evaluation.evaluate_capture(m, s)
    assert result["hit_at_1"] == result["hit_at_3"] == 0
    assert all(row["status"] == "inconclusive" and row["reasons"] for row in result["diagnoses"])


@pytest.mark.parametrize("update", [{"status": "recovering"}, {"fault_ended_at": None}])
def test_incomplete_capture_cannot_pass(update):
    m = {**manifest(), **update}
    assert evaluation.evaluate_capture(m, summary(m))["diagnoses"][0]["status"] == "inconclusive"


def test_incidents_are_not_flattened_and_runs_have_equal_weight():
    m = manifest()
    s = summary(m, ("payment",))
    s["incidents"].append({"id": "incident-2"})  # One correct, one unavailable.
    original = copy.deepcopy(s)
    first = evaluation.evaluate_capture(m, s)
    assert first["hit_at_3"] == 0.5
    assert s == original
    second_manifest = manifest("cartFailure", number=2)
    second = evaluation.evaluate_capture(second_manifest, summary(second_manifest, ("cart",)))
    report = evaluation.build_report([first, second])
    assert report["run_count"] == 2 and report["diagnosis_count"] == 3
    assert report["hit_at_1"] == report["hit_at_3"] == 0.75
    assert report["passed"] == 2 and report["inconclusive"] == 1
    assert report["by_scenario"]["paymentFailure"]["hit_at_3"] == 0.5
    with pytest.raises(ValueError, match="duplicate"):
        evaluation.build_report([first, first])
    with pytest.raises(ValueError, match="at least one"):
        evaluation.build_report([])


def write_manifest(evaluation_dir, m):
    path = evaluation_dir / f"manifest-{m['experiment_id']}.json"
    path.write_text(json.dumps(m), encoding="utf-8")
    return path


def test_cli_offline_is_repeatable_keeps_missing_runs_and_never_uses_network(evaluation_dir, monkeypatch, capsys):
    m = manifest()
    other = manifest("cartFailure", number=2)
    paths = [write_manifest(evaluation_dir, item) for item in (m, other)]
    (evaluation_dir / f"{m['experiment_id']}.json").write_text(json.dumps(summary(m)), encoding="utf-8")
    monkeypatch.setattr(evaluation, "request_json", lambda *args: pytest.fail("offline network call"))
    args = [*map(str, paths), "--summary-dir", str(evaluation_dir)]
    before = [path.read_bytes() for path in paths]
    for _ in range(2):
        assert evaluation.main(args) == 1
        report = json.loads(capsys.readouterr().out)
        assert report["run_count"] == 2 and report["inconclusive"] == 1
        assert report["hit_at_1"] == 0.5
    assert [path.read_bytes() for path in paths] == before


@pytest.mark.parametrize("unavailable", [False, True])
def test_cli_api_only_gets_existing_run_and_handles_unavailability(evaluation_dir, monkeypatch, capsys, unavailable):
    m = manifest()
    path = write_manifest(evaluation_dir, m)
    calls = []

    def get(method, url):
        calls.append((method, url))
        if unavailable:
            raise SystemExit("API unavailable")
        return summary(m)

    monkeypatch.setattr(evaluation, "request_json", get)
    assert evaluation.main([str(path), "--api-url", "http://example.invalid/"]) == int(unavailable)
    report = json.loads(capsys.readouterr().out)
    assert report["inconclusive"] == int(unavailable)
    assert calls == [("GET", f"http://example.invalid/api/v1/experiments/{m['experiment_id']}/capture-summary")]


def test_cli_rejects_duplicate_runs_before_api_and_refuses_overwrite(evaluation_dir, monkeypatch, capsys):
    m = manifest()
    path = write_manifest(evaluation_dir, m)
    monkeypatch.setattr(evaluation, "request_json", lambda *args: pytest.fail("unexpected API call"))
    with pytest.raises(SystemExit) as exc:
        evaluation.main([str(path), str(path), "--api-url", "http://example.invalid"])
    assert exc.value.code == 2
    output = evaluation_dir / "report.json"
    output.write_text("preserve me", encoding="utf-8")
    with pytest.raises(SystemExit) as exc:
        evaluation.main([str(path), "--summary-dir", str(evaluation_dir), "--output", str(output)])
    assert exc.value.code == 2
    assert output.read_text(encoding="utf-8") == "preserve me"


@pytest.mark.parametrize("body", ["{", "[]"])
def test_cli_bad_snapshot_remains_in_report(evaluation_dir, capsys, body):
    m = manifest()
    path = write_manifest(evaluation_dir, m)
    (evaluation_dir / f"{m['experiment_id']}.json").write_text(body, encoding="utf-8")
    assert evaluation.main([str(path), "--summary-dir", str(evaluation_dir)]) == 1
    report = json.loads(capsys.readouterr().out)
    assert "capture_summary_unavailable" in report["runs"][0]["diagnoses"][0]["reasons"][0]


def test_documented_three_scenario_example(capsys):
    fixtures = Path(__file__).resolve().parents[1] / "fixtures" / "rca_evaluation"
    args = [str(fixtures / f"{name}.json") for name in ("payment", "cart", "ad")]
    assert evaluation.main([*args, "--summary-dir", str(fixtures / "summaries")]) == 1
    report = json.loads(capsys.readouterr().out)
    assert (report["passed"], report["failed"], report["inconclusive"]) == (1, 1, 1)
    assert report["hit_at_1"] == report["hit_at_3"] == 1 / 3


def test_invalid_scenario_is_configuration_error_before_api(evaluation_dir, monkeypatch):
    path = write_manifest(evaluation_dir, manifest("unknownFault"))
    monkeypatch.setattr(evaluation, "request_json", lambda *args: pytest.fail("unexpected API call"))
    with pytest.raises(SystemExit) as exc:
        evaluation.main([str(path), "--api-url", "http://example.invalid"])
    assert exc.value.code == 2
