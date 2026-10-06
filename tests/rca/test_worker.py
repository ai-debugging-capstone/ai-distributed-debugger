from __future__ import annotations

import json
import os
import re
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from rca.domain import (
    AnomalyResult,
    CandidateRanking,
    DependencyEdge,
    FeatureBucket,
    Incident,
)
from rca.worker import (
    RCAWorker,
    RunLifecycle,
    WorkerSettings,
    incident_processing_mode,
    is_unprocessed_bucket,
)


class WorkerConfigurationTests(unittest.TestCase):
    @staticmethod
    def anomaly(run_id: str, offset: int, entity: str = "payment") -> AnomalyResult:
        start = datetime(2026, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=offset * 10)
        bucket = FeatureBucket(
            bucket_start=start,
            bucket_end=start + timedelta(seconds=10),
            entity_type="service",
            entity_id=entity,
            run_id=run_id,
            features={"error_rate": 1.0},
        )
        return AnomalyResult(
            bucket=bucket,
            score=8.0,
            onset_score=8.0,
            feature_scores={"error_rate": 8.0},
            baselines={"error_rate": 0.0},
            contributions={"error_rate": 1.0},
            sufficient_history=True,
        )

    def test_environment_configuration(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DATABASE_URL": "postgresql://example/db",
                "RCA_POLL_SECONDS": "2.5",
                "RCA_BUCKET_SECONDS": "20",
                "RCA_LOOKBACK_BUCKETS": "40",
                "RCA_MIN_HISTORY_BUCKETS": "8",
                "RCA_MIN_INCIDENT_BUCKETS": "3",
                "RCA_MODEL_PATH": "artifacts/champion/model.json",
            },
            clear=False,
        ):
            settings = WorkerSettings.from_env()
        self.assertEqual(settings.poll_seconds, 2.5)
        self.assertEqual(settings.bucket_seconds, 20)
        self.assertEqual(settings.lookback_buckets, 40)
        self.assertEqual(settings.min_incident_buckets, 3)
        self.assertEqual(settings.model_path, "artifacts/champion/model.json")

    def test_deployed_incident_threshold_matches_code_default(self) -> None:
        with patch.dict(
            os.environ,
            {"DATABASE_URL": "postgresql://example/db"},
            clear=True,
        ):
            default_threshold = WorkerSettings.from_env().min_incident_buckets

        project_root = Path(__file__).resolve().parents[2]
        compose = (project_root / "docker-compose.yml").read_text(encoding="utf-8")
        example_env = (project_root / ".env.example").read_text(encoding="utf-8")
        compose_match = re.search(
            r"RCA_MIN_INCIDENT_BUCKETS:\s+\$\{RCA_MIN_INCIDENT_BUCKETS:-(\d+)\}",
            compose,
        )
        env_match = re.search(r"^RCA_MIN_INCIDENT_BUCKETS=(\d+)$", example_env, re.MULTILINE)

        self.assertIsNotNone(compose_match)
        self.assertIsNotNone(env_match)
        self.assertEqual(int(compose_match.group(1)), default_threshold)
        self.assertEqual(int(env_match.group(1)), default_threshold)

    def test_worker_validates_database_url_and_lookback(self) -> None:
        with self.assertRaises(ValueError):
            RCAWorker(WorkerSettings(""))
        with self.assertRaises(ValueError):
            RCAWorker(WorkerSettings("postgresql://x", lookback_buckets=2, min_history_buckets=6))

    def test_per_run_watermark_accepts_overlapping_historical_new_run(self) -> None:
        old = datetime(2026, 1, 1, tzinfo=timezone.utc)
        newer = old + timedelta(days=3)
        watermarks = {"run-a": newer, None: newer}
        self.assertFalse(is_unprocessed_bucket("run-a", old, watermarks))
        self.assertTrue(is_unprocessed_bucket("run-b", old, watermarks))
        self.assertFalse(is_unprocessed_bucket(None, newer, watermarks))
        self.assertTrue(is_unprocessed_bucket(None, newer + timedelta(seconds=10), watermarks))

    def test_lifecycle_policy_keeps_unscoped_live_detection_and_fails_unknown_run_closed(self) -> None:
        self.assertEqual(
            incident_processing_mode(None, None, has_active_incident=False), "detect"
        )
        self.assertEqual(
            incident_processing_mode("missing-run", None, has_active_incident=False),
            "observe",
        )
        self.assertEqual(
            incident_processing_mode(
                "run-a", RunLifecycle("warming"), has_active_incident=False
            ),
            "observe",
        )
        self.assertEqual(
            incident_processing_mode(
                "run-a", RunLifecycle("running"), has_active_incident=False
            ),
            "detect",
        )

    def test_anomalous_warmup_is_baseline_only_and_running_anomaly_opens_incident(self) -> None:
        worker = RCAWorker(
            WorkerSettings(
                "postgresql://unused",
                min_history_buckets=1,
            )
        )
        warm_one = self.anomaly("run-a", 0)
        warm_two = self.anomaly("run-a", 1)
        for result in (warm_one, warm_two):
            # In run_once the scorer and _write_feature calls happen before
            # this lifecycle gate. The gate owns only incident state.
            mode, events = worker._incident_events_for_lifecycle(
                [result], RunLifecycle("warming")
            )
            self.assertEqual(mode, "observe")
            self.assertEqual(events, [])
        self.assertFalse(worker.detector.has_active_incident("run-a"))
        self.assertEqual(worker._recent_anomalies["run-a"], [])

        mode, first = worker._incident_events_for_lifecycle(
            [self.anomaly("run-a", 2)], RunLifecycle("running")
        )
        self.assertEqual(mode, "detect")
        self.assertEqual(first, [])
        _, second = worker._incident_events_for_lifecycle(
            [self.anomaly("run-a", 3)], RunLifecycle("running")
        )
        self.assertEqual([event.action for event in second], ["opened"])
        self.assertEqual(second[0].incident.started_at, self.anomaly("run-a", 2).bucket.bucket_start)
        self.assertEqual(len(worker._recent_anomalies["run-a"]), 2)

    def test_recovery_cannot_grow_or_open_incident(self) -> None:
        worker = RCAWorker(
            WorkerSettings(
                "postgresql://unused",
                min_incident_buckets=1,
                min_history_buckets=1,
            )
        )
        _, opened = worker._incident_events_for_lifecycle(
            [self.anomaly("run-a", 0)], RunLifecycle("running")
        )
        self.assertEqual([event.action for event in opened], ["opened"])
        mode, events = worker._incident_events_for_lifecycle(
            [self.anomaly("run-a", 1, entity="unrelated")],
            RunLifecycle("recovering"),
        )
        self.assertEqual(mode, "recover")
        self.assertEqual(events, [])
        active = worker.detector.active_incidents[0]
        self.assertEqual(active.affected_entities, ["payment"])

    def test_dependency_graph_is_rolling_and_cannot_see_future_edges(self) -> None:
        worker = RCAWorker(
            WorkerSettings(
                "postgresql://unused",
                bucket_seconds=10,
                lookback_buckets=2,
                min_history_buckets=1,
            )
        )
        start = datetime(2026, 1, 1, tzinfo=timezone.utc)
        failing = DependencyEdge(
            start,
            "checkout",
            "payment",
            run_id="run-a",
            request_count=2,
            error_count=2,
        )
        future_success = DependencyEdge(
            start + timedelta(seconds=10),
            "checkout",
            "payment",
            run_id="run-a",
            request_count=2,
        )

        first = worker._dependency_graph_at("run-a", start, [failing, future_success])
        self.assertEqual(first.edges[("checkout", "payment")].error_rate, 1.0)
        second = worker._dependency_graph_at(
            "run-a", start + timedelta(seconds=10), [future_success]
        )
        self.assertEqual(second.edges[("checkout", "payment")].error_rate, 0.5)
        expired = worker._dependency_graph_at(
            "run-a", start + timedelta(seconds=31), []
        )
        self.assertEqual(expired.edges, {})

    def test_recommendation_selects_seeded_runbook_for_top_actionable_candidate(self) -> None:
        class Cursor:
            def __init__(self):
                self.queries = []

            def execute(self, query, params):
                self.queries.append((" ".join(query.split()), params))

            def fetchone(self):
                return {"id": "runbook-id", "title": "Investigate a service error", "entity_type": "service"}

        cursor = Cursor()
        ranking = CandidateRanking(
            "service", "checkout", 1, 0.9, 0.8, "high", "service_error", "error_rate=0.9"
        )
        incident = Incident(
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            datetime(2026, 1, 1, tzinfo=timezone.utc),
            "checkout",
            ["checkout"],
            8,
            "warning",
        )
        RCAWorker._write_recommendation(cursor, incident, [(ranking, 123)])
        self.assertIn("fault_class=%s", cursor.queries[0][0])
        self.assertEqual(cursor.queries[0][1][0], "service_error")
        self.assertIn("INSERT INTO recommendations", cursor.queries[1][0])

    def test_persisted_candidate_contains_exact_online_feature_snapshot(self) -> None:
        class Cursor:
            def __init__(self):
                self.queries = []
                self.next_row = None

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query, params):
                normalized = " ".join(query.split())
                self.queries.append((normalized, params))
                if "INSERT INTO candidate_rankings" in normalized:
                    self.next_row = {"id": 123}
                elif "SELECT id, title, entity_type FROM runbooks" in normalized:
                    self.next_row = None

            def fetchone(self):
                return self.next_row

        class Connection:
            def __init__(self):
                self.the_cursor = Cursor()

            def cursor(self):
                return self.the_cursor

        now = datetime(2026, 1, 1, tzinfo=timezone.utc)
        incident = Incident(now, now, "checkout", ["checkout"], 8, "warning")
        ranking = CandidateRanking(
            "service", "checkout", 1, 0.9, 0.8, "high", "service_error", "error_rate=0.9"
        )
        connection = Connection()
        worker = RCAWorker.__new__(RCAWorker)
        worker._replace_rankings(connection, incident, [ranking])

        insert = next(
            item for item in connection.the_cursor.queries
            if "INSERT INTO candidate_rankings" in item[0]
        )
        self.assertIn("online_features", insert[0])
        snapshot = json.loads(insert[1][-1])
        self.assertEqual(snapshot["baseline_score"], 0.9)
        self.assertEqual(snapshot["baseline_probability"], 0.8)
        self.assertEqual(snapshot["baseline_inverse_rank"], 1.0)


if __name__ == "__main__":
    unittest.main()
