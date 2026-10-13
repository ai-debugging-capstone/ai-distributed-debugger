"""Test the shipped model's input/execution contract, never RCA accuracy or training."""

import hashlib
import math
from datetime import UTC, datetime
from itertools import groupby
from pathlib import Path

import pytest

from rca.candidate_features import candidate_feature_map
from rca.dataset import feature_matrix
from rca.domain import CandidateRanking, Incident
from rca.features import StreamingAnomalyScorer, aggregate_feature_buckets
from rca.graph import DirectedDependencyGraph, build_dependency_edges
from rca.incidents import IncidentDetector
from rca.online_ranking import (
    ChampionRerankingRootCauseRanker, load_champion_artifact, online_candidate_record,
)
from rca.ranking import HeuristicRootCauseRanker
from rca.synthetic import generate_run
from telemetry_api.models import NormalizedBatch
from telemetry_api.normalization import normalized_batch_to_records
from tests.telemetry_samples import NANOS, make_sample

MODEL_PATH = Path(__file__).resolve().parents[2] / "artifacts" / "champion" / "model.json"
SCHEMA = (
    "baseline_inverse_rank", "baseline_probability", "baseline_score",
    "candidate_is_dependency_edge", "candidate_is_service",
)


@pytest.fixture
def artifact():
    original = MODEL_PATH.read_bytes()
    loaded = load_champion_artifact(MODEL_PATH)
    assert loaded is not None
    assert loaded.ranker.backend == "portable-linear"
    assert loaded.sha256 == hashlib.sha256(original).hexdigest()
    yield loaded
    assert MODEL_PATH.read_bytes() == original  # Inference must not rewrite the artifact.


def candidate_records():
    now = datetime(2026, 1, 1, tzinfo=UTC)
    incident = Incident(now, now, "payment", ["payment"], 8.0, "critical", run_id="fixture")
    candidates = [
        CandidateRanking("service", "payment", 1, 0.8, 0.7, "high", "service_error", "fixture"),
        CandidateRanking("dependency_edge", "checkout->payment", 2, 0.6, 0.2,
                         "medium", "dependency_error", "fixture"),
        CandidateRanking("service", "checkout", 3, 0.1, 0.05, "low", "unknown", "fixture"),
    ]
    return [online_candidate_record(incident, item) for item in candidates]


def test_deployed_feature_order_dimensions_and_numeric_types(artifact):
    records = candidate_records()
    assert artifact.ranker.schema == SCHEMA
    assert all(set(SCHEMA) <= set(row["features"]) for row in records)
    assert all("candidate_id" in row and "incident_id" in row for row in records)
    matrix = feature_matrix(records, artifact.ranker.schema)
    assert matrix == [[1.0, 0.7, 0.8, 0.0, 1.0],
                      [0.5, 0.2, 0.6, 1.0, 0.0],
                      [1.0 / 3, 0.05, 0.1, 0.0, 1.0]]
    assert all(len(row) == 5 for row in matrix)
    assert all(type(value) is float and math.isfinite(value) for row in matrix for value in row)
    reordered = [{**record, "features": dict(reversed(list(record["features"].items())))}
                 for record in records]
    assert feature_matrix(reordered, SCHEMA) == matrix


def test_missing_features_default_to_zero_and_extra_fields_do_not_enter_model(artifact):
    record = {"features": {"baseline_score": 2, "unrelated": 999}, "label": 1}
    assert feature_matrix([record], SCHEMA) == [[0.0, 0.0, 2.0, 0.0, 0.0]]
    scores = artifact.ranker.predict_scores([record, {}])
    assert len(scores) == 2
    assert all(math.isfinite(value) and 0 <= value <= 1 for value in scores)


@pytest.mark.parametrize("invalid", [None, "not-a-number"])
def test_invalid_raw_feature_fails_explicitly_instead_of_silently_scoring(artifact, invalid):
    with pytest.raises((TypeError, ValueError)):
        artifact.ranker.predict_scores([{"features": {"baseline_score": invalid}}])


@pytest.mark.parametrize("invalid", [None, "bad", float("nan"), float("inf")])
def test_online_builder_sanitizes_invalid_numeric_values_before_inference(artifact, invalid):
    features = candidate_feature_map(
        {"entity_type": "service", "score": invalid, "probability": invalid, "rank": invalid},
        [{"contribution": invalid, "value": invalid, "baseline": invalid,
          "details": {"robust_z": invalid}}],
    )
    assert features["baseline_score"] == features["baseline_probability"] == 0.0
    assert features["baseline_inverse_rank"] == 1.0
    assert all(type(value) is float and math.isfinite(value) for value in features.values())
    score = artifact.ranker.predict_scores([{"features": features}])[0]
    assert math.isfinite(score) and 0 <= score <= 1


def test_real_inference_outputs_are_associated_with_candidate_ids(artifact):
    records = candidate_records()
    scores = artifact.ranker.predict_scores(records)
    assert len(scores) == len(records) == 3
    assert len(set(scores)) > 1  # The checked-in parameters produce genuine distinct outputs.
    assert all(type(value) is float and math.isfinite(value) and 0 <= value <= 1
               for value in scores)
    expected = {row["candidate_id"]: score for row, score in zip(records, scores)}
    ranked = artifact.ranker.rank_by_incident(list(reversed(records)))
    assert set(ranked) == {records[0]["incident_id"]}
    assert dict(ranked[records[0]["incident_id"]]) == expected
    assert artifact.ranker.predict_scores([]) == []


def normalized_records(source):
    if source == "normalized_json":
        run = generate_run("payment_failure", seed=4995, start=datetime(2026, 1, 1, tzinfo=UTC),
                           warmup_buckets=14, fault_buckets=6, recovery_buckets=2,
                           requests_per_route=1)
        batch = NormalizedBatch.model_validate({"run_id": run.run_id, "events": run.events})
        rows, rejected = normalized_batch_to_records(batch)
        assert not rejected
        return rows
    rows = {"span": [], "metric": [], "log": []}
    for bucket in range(20):
        fault = bucket >= 14
        timestamp = NANOS + bucket * 10_000_000_000
        for signal in rows:
            sample = make_sample(signal)
            if signal == "span":
                sample.item.span_id = (bucket + 1).to_bytes(8, "big")
                sample.item.start_time_unix_nano = timestamp
                duration_nanos = 400_000_000 if fault else 10_000_000
                sample.item.end_time_unix_nano = timestamp + duration_nanos
                sample.item.status.code = 2 if fault else 1
            else:
                sample.item.time_unix_nano = timestamp
                if signal == "log":
                    sample.item.observed_time_unix_nano = timestamp
                    sample.item.severity_number = 17 if fault else 9
                    sample.item.severity_text = "ERROR" if fault else "INFO"
                else:
                    sample.item.as_double = 0.99 if fault else 0.2
            normalized, rejected = sample.normalize(sample.request)
            assert not rejected
            rows[signal].extend(normalized)
    return rows


@pytest.mark.component
@pytest.mark.parametrize("source", ["otlp", "normalized_json"])
def test_normalized_telemetry_reaches_real_live_inference(artifact, monkeypatch, source):
    rows = normalized_records(source)
    assert all(rows[signal] for signal in ("span", "metric", "log"))
    buckets = aggregate_feature_buckets(rows["span"], rows["metric"], rows["log"])
    scorer = StreamingAnomalyScorer(lookback=20, min_history=10)
    detector = IncidentDetector(min_consecutive=2)
    anomalies, opened = [], []
    ordered = sorted(buckets, key=lambda item: (item.bucket_start, item.entity_id))
    for _, group in groupby(ordered, key=lambda item: item.bucket_start):
        current = [scorer.score(bucket) for bucket in group]
        anomalies.extend(current)
        opened.extend(event.incident for event in detector.update(current) if event.action == "opened")
    assert opened, "fixture must exercise live candidate creation, not only model loading"
    incident = opened[0]
    graph = DirectedDependencyGraph(build_dependency_edges(rows["span"]))
    baseline = HeuristicRootCauseRanker(min_margin=0)
    decision = baseline.rank(incident, anomalies, graph)
    candidates = decision.actionable_rankings
    assert candidates
    records = [online_candidate_record(incident, candidate) for candidate in candidates]
    expected_scores = artifact.ranker.predict_scores(records)
    expected = {(candidate.entity_type, candidate.entity_id): score
                for candidate, score in zip(candidates, expected_scores)}
    calls = []
    actual_predict = artifact.ranker.predict_scores

    def observe_actual_inference(inputs):
        calls.append(inputs)
        return actual_predict(inputs)  # Observing spy; no canned outputs or fake model.

    monkeypatch.setattr(artifact.ranker, "predict_scores", observe_actual_inference)
    reranked = ChampionRerankingRootCauseRanker(artifact, baseline=baseline).rank(
        incident, anomalies, graph,
    )
    assert calls == [records]
    assert len(expected_scores) == len(records)
    assert all(len(row) == 5 for row in feature_matrix(calls[0], artifact.ranker.schema))
    for candidate in reranked.actionable_rankings:
        assert candidate.model_version == artifact.model_version
        assert candidate.probability == expected[(candidate.entity_type, candidate.entity_id)]
        assert candidate.online_features
    # No assertions about which candidate is the true cause or causal confidence.
