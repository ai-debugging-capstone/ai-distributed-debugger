from __future__ import annotations

import json
import os
import re
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

from rca.domain import (
    AnomalyResult,
    CandidateRanking,
    DependencyEdge,
    FeatureBucket,
    Incident,
)
from rca.incidents import IncidentEvent
from rca.ranking import RankerDecision
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
                "RCA_ANALYSIS_ENGINE": "heuristic",
            },
            clear=False,
        ):
            settings = WorkerSettings.from_env()
        self.assertEqual(settings.poll_seconds, 2.5)
        self.assertEqual(settings.bucket_seconds, 20)
        self.assertEqual(settings.lookback_buckets, 40)
        self.assertEqual(settings.min_incident_buckets, 3)
        self.assertEqual(settings.model_path, "artifacts/champion/model.json")
        self.assertEqual(settings.analysis_engine, "heuristic")

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
        with self.assertRaises(ValueError):
            RCAWorker(WorkerSettings("postgresql://x", analysis_engine="typo"))

    def test_dowhy_selection_does_not_load_legacy_champion(self) -> None:
        with (
            patch("rca.dowhy_engine.DoWhyRootCauseRanker") as provider,
            patch("rca.worker.load_champion_artifact") as load,
        ):
            worker = RCAWorker(WorkerSettings("postgresql://unused"))
        self.assertIs(worker.ranker, provider.return_value)
        load.assert_not_called()

    def test_explicit_heuristic_and_champion_remain_available(self) -> None:
        with patch("rca.worker.load_champion_artifact", return_value=None) as load:
            heuristic = RCAWorker(
                WorkerSettings("postgresql://unused", analysis_engine="heuristic")
            )
            load.assert_not_called()
            champion = RCAWorker(
                WorkerSettings("postgresql://unused", analysis_engine="champion")
            )
            load.assert_called_once()
        self.assertEqual(type(heuristic.ranker).__name__, "HeuristicRootCauseRanker")
        self.assertEqual(type(champion.ranker).__name__, "HeuristicRootCauseRanker")

    def run_bucket(self, worker, result, lifecycle, events=None):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchone.return_value = {"acquired": True}
        with (
            patch.object(worker, "_connect", return_value=connection),
            patch.object(worker, "_watermarks", return_value={}),
            patch.object(worker, "_prime_history"),
            patch.object(worker, "_resolve_terminal_active_incidents"),
            patch.object(worker, "_raw_start", return_value=result.bucket.bucket_start),
            patch.object(worker, "_fetch_raw", return_value=([], [], [])),
            patch("rca.worker.aggregate_feature_buckets", return_value=[result.bucket]),
            patch("rca.worker.build_dependency_edges", return_value=[]),
            patch.object(worker.scorer, "score", return_value=result),
            patch.object(worker, "_write_feature"),
            patch.object(worker, "_run_lifecycles", return_value={"run-a": lifecycle}),
        ):
            if events is None:
                return worker.run_once()
            with patch.object(
                worker, "_incident_events_for_lifecycle", return_value=("detect", events)
            ):
                return worker.run_once()

    def test_warmup_reaches_provider_without_opening_an_incident(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )
        provider = MagicMock()
        worker.ranker = provider
        result = self.anomaly("run-a", 0)
        with patch.object(worker, "_persist_incident") as persist:
            self.run_bucket(worker, result, RunLifecycle("warming"))
        provider.observe.assert_called_once_with([result])
        provider.rank.assert_not_called()
        persist.assert_not_called()

    def test_diagnosis_metadata_is_persisted_after_provider_runs(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )
        result = self.anomaly("run-a", 0)
        incident = Incident(
            result.bucket.bucket_start, result.bucket.bucket_start, "payment",
            ["payment"], 8, "warning", "run-a",
        )
        ranking = CandidateRanking(
            "service", "payment", 1, 12.5, None, "low", "service_error",
            "latency attribution", model_version="dowhy-gcm-0.14-latency-v1",
        )
        seen = []

        def diagnose(actual, *_):
            self.assertIs(actual, incident)
            self.assertNotEqual(actual.id, original_id)
            self.assertEqual(seen, ["observe"])
            actual.metadata["analysis"] = {"provider": "dowhy-gcm", "status": "analyzed"}
            seen.append("rank")
            return RankerDecision((ranking,), False, None, 1.0)

        def persist(_connection, event):
            self.assertEqual(seen, ["observe", "rank"])
            self.assertEqual(event.incident.metadata["analysis"]["provider"], "dowhy-gcm")
            self.assertEqual(
                event.incident.metadata["analysis_model_version"], ranking.model_version
            )
            seen.append("persist")
            return event.incident

        original_id = incident.id
        provider = MagicMock()
        provider.observe.side_effect = lambda _: seen.append("observe")
        provider.rank.side_effect = diagnose
        worker.ranker = provider
        with (
            patch.object(worker, "_persist_incident", side_effect=persist),
            patch.object(worker, "_replace_rankings") as write,
        ):
            self.run_bucket(
                worker, result, RunLifecycle("running"), [IncidentEvent("opened", incident)]
            )
        self.assertEqual(seen, ["observe", "rank", "persist"])
        write.assert_called_once()

    def test_restart_primes_provider_with_persisted_anomaly_scores(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )
        provider = MagicMock()
        worker.ranker = provider
        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        when = datetime(2026, 1, 1, tzinfo=timezone.utc)
        cursor.fetchall.return_value = [{
            "bucket_start": when, "entity_type": "service", "entity_id": "payment",
            "run_id": "run-a", "features": {"latency_ms_p95": 400.0},
            "anomaly_score": 8.0, "onset_score": 5.0,
        }]
        worker._prime_history(connection)
        observed = provider.observe.call_args[0][0][0]
        self.assertEqual(observed.bucket.run_id, "run-a")
        self.assertEqual(observed.bucket.value("latency_ms_p95"), 400.0)
        self.assertEqual(observed.score, 8.0)
        self.assertEqual(observed.onset_score, 5.0)

    def test_restart_restores_open_incident_from_database(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        incident = Incident(
            id="11111111-1111-1111-1111-111111111111",
            run_id="22222222-2222-2222-2222-222222222222",
            started_at=started,
            detected_at=started + timedelta(seconds=10),
            trigger_entity="payment",
            affected_entities=["payment"],
            max_anomaly=8.0,
            severity="warning",
            status="open",
            summary="Sustained anomaly affecting payment",
            metadata={"detector": "robust-baseline-v1"},
        )

        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [{
            "id": incident.id,
            "run_id": incident.run_id,
            "status": incident.status,
            "started_at": incident.started_at,
            "detected_at": incident.detected_at,
            "ended_at": incident.ended_at,
            "trigger_entity": incident.trigger_entity,
            "affected_entities": incident.affected_entities,
            "max_anomaly": incident.max_anomaly,
            "severity": incident.severity,
            "summary": incident.summary,
            "metadata": incident.metadata,
        }]

        worker._restore_active_incidents(connection)

        self.assertTrue(worker.detector.has_active_incident(incident.run_id))
        restored = worker.detector.active_incidents[0]

        self.assertEqual(restored.id, incident.id)
        self.assertEqual(restored.started_at, incident.started_at)
        self.assertEqual(restored.affected_entities, ["payment"])
        self.assertEqual(restored.status, "open")

    def test_restart_does_not_restore_resolved_incidents(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        worker._restore_active_incidents(connection)

        self.assertEqual(worker.detector.active_incidents, ())

        query = cursor.execute.call_args[0][0]
        self.assertIn("status = 'open'", query)
        self.assertIn("ended_at IS NULL", query)


    def test_restart_rejects_duplicate_open_incidents_for_same_run(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        run_id = "22222222-2222-2222-2222-222222222222"

        def stored_incident(incident_id):
            return {
                "id": incident_id,
                "run_id": run_id,
                "status": "open",
                "started_at": started,
                "detected_at": started + timedelta(seconds=10),
                "ended_at": None,
                "trigger_entity": "payment",
                "affected_entities": ["payment"],
                "max_anomaly": 8.0,
                "severity": "warning",
                "summary": "Sustained anomaly affecting payment",
                "metadata": {},
            }

        connection = MagicMock()
        cursor = connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [
            stored_incident("11111111-1111-1111-1111-111111111111"),
            stored_incident("33333333-3333-3333-3333-333333333333"),
        ]

        with self.assertRaisesRegex(RuntimeError, "Multiple open incidents"):
            worker._restore_active_incidents(connection)

        self.assertEqual(worker.detector.active_incidents, ())


    def test_restored_incident_keeps_original_id_when_persisted(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        started = datetime(2026, 1, 1, tzinfo=timezone.utc)

        incident = Incident(
            id="11111111-1111-1111-1111-111111111111",
            run_id="22222222-2222-2222-2222-222222222222",
            started_at=started,
            detected_at=started + timedelta(seconds=10),
            trigger_entity="payment",
            affected_entities=["payment"],
            max_anomaly=8.0,
            severity="warning",
            status="open",
        )

        worker._identify_incident(incident)
        original_id = incident.id

        connection = MagicMock()

        worker._persist_incident(
            connection,
            IncidentEvent("updated", incident),
        )

        self.assertEqual(incident.id, original_id)

    def test_worker_restores_incidents_only_on_first_cycle(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        result = self.anomaly("run-a", 0)

        with patch.object(worker, "_restore_active_incidents") as restore:
            self.run_bucket(worker, result, RunLifecycle("running"))
            self.run_bucket(worker, result, RunLifecycle("running"))

        restore.assert_called_once()

    def test_restored_incident_resolves_after_healthy_buckets(self) -> None:
        worker = RCAWorker(
            WorkerSettings("postgresql://unused", analysis_engine="heuristic")
        )

        # Simulate an incident loaded from PostgreSQL.
        started = datetime(2026, 1, 1, tzinfo=timezone.utc)
        incident = Incident(
            run_id="run-a",
            started_at=started,
            detected_at=started + timedelta(seconds=10),
            trigger_entity="payment",
            affected_entities=["payment"],
            max_anomaly=8.0,
            severity="warning",
            status="open",
        )

        worker._identify_incident(incident)
        original_id = incident.id
        worker.detector._active["run-a"] = incident

        # Healthy results from three consecutive 10-second buckets.
        for offset in (2, 3, 4):
            result = self.anomaly("run-a", offset)
            result = result.__class__(
                bucket=result.bucket,
                score=0.0,
                onset_score=0.0,
                feature_scores={},
                baselines={},
                contributions={},
                sufficient_history=True,
            )

            mode, events = worker._incident_events_for_lifecycle(
                [result], RunLifecycle("recovering")
            )

            self.assertEqual(mode, "recover")

        self.assertEqual(len(events), 1)
        self.assertEqual(events[0].action, "resolved")
        self.assertEqual(events[0].incident.id, original_id)
        self.assertEqual(events[0].incident.status, "resolved")
        self.assertFalse(worker.detector.has_active_incident("run-a"))

    def test_per_run_watermark_accepts_overlapping_historical_new_run(self) -> None:
        old = datetime(2026, 1, 1, tzinfo=timezone.utc)
        newer = old + timedelta(days=3)
        watermarks = {"run-a": newer, None: newer}
        self.assertFalse(is_unprocessed_bucket("run-a", old, watermarks))
        self.assertTrue(is_unprocessed_bucket("run-b", old, watermarks))
        self.assertFalse(is_unprocessed_bucket(None, newer, watermarks))
        self.assertTrue(is_unprocessed_bucket(None, newer + timedelta(seconds=10), watermarks))

    def test_worker_establishes_repeatable_read_before_advisory_lock(self) -> None:
        statements: list[str] = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query, params=None):
                statements.append(" ".join(query.split()))

            def fetchone(self):
                return {"acquired": False}

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def execute(self, query):
                statements.append(" ".join(query.split()))

            def cursor(self):
                return Cursor()

        worker = RCAWorker(WorkerSettings("postgresql://unused"))
        with patch.object(worker, "_connect", return_value=Connection()):
            processed = worker.run_once()

        self.assertEqual(processed, 0)
        self.assertEqual(
            statements,
            [
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                "SELECT pg_try_advisory_xact_lock(%s) AS acquired",
            ],
        )

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

    #CK changes
    def test_abstained_decision_does_not_attach_runbook(self) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "id": "runbook-id",
            "title": "Investigate a service error",
            "entity_type": "service",
        }

        when = datetime(2026, 1, 1, tzinfo=timezone.utc)
        incident = Incident(
            when, when, "checkout", ["checkout"], 8, "warning"
        )

        ranking = CandidateRanking(
            "service",
            "checkout",
            1,
            0.9,
            0.8,
            "high",
            "service_error",
            "error_rate=0.9",
        )

        RCAWorker._write_recommendation(
            cursor,
            incident,
            [(ranking, 123)],
            abstained=True,
        )

        cursor.execute.assert_not_called()

    def test_attribution_recommendation_preserves_unknown_probability(self) -> None:
        cursor = MagicMock()
        cursor.fetchone.return_value = {
            "id": "runbook-id", "title": "Investigate a service error", "entity_type": "service"
        }
        when = datetime(2026, 1, 1, tzinfo=timezone.utc)
        incident = Incident(when, when, "checkout", ["checkout"], 8, "warning")
        ranking = CandidateRanking(
            "service", "checkout", 1, 12.5, None, "low", "service_error",
            "latency attribution", model_version="dowhy-gcm-0.14-latency-v1",
        )
        RCAWorker._write_recommendation(cursor, incident, [(ranking, 123)])
        params = cursor.execute.call_args[0][1]
        self.assertIsNone(params[3])
        self.assertIn("attribution score=12.500 (not a probability)", params[4])

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

        connection.the_cursor.queries.clear()
        attribution = CandidateRanking(
            "service", "checkout", 1, 12.5, None, "low", "service_error",
            "latency attribution", model_version="dowhy-gcm-0.14-latency-v1",
        )
        worker._replace_rankings(connection, incident, [attribution])
        external_insert = next(
            item for item in connection.the_cursor.queries
            if "INSERT INTO candidate_rankings" in item[0]
        )
        self.assertIsNone(external_insert[1][5])
        self.assertEqual(json.loads(external_insert[1][-1]), {})

    def test_abstained_ranking_replacement_does_not_create_recommendation(self) -> None:
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
                        self.next_row = {
                            "id": "runbook-id",
                            "title": "Investigate a service error",
                            "entity_type": "service",
                        }

                def fetchone(self):
                    return self.next_row

            class Connection:
                def __init__(self):
                    self.the_cursor = Cursor()

                def cursor(self):
                    return self.the_cursor

            when = datetime(2026, 1, 1, tzinfo=timezone.utc)
            incident = Incident(
                when, when, "checkout", ["checkout"], 8, "warning"
            )

            ranking = CandidateRanking(
                "service", "checkout", 1, 0.9, 0.8,
                "high", "service_error", "error_rate=0.9",
            )

            connection = Connection()
            worker = RCAWorker.__new__(RCAWorker)

            worker._replace_rankings(
                connection,
                incident,
                [ranking],
                abstained=True,
            )

            queries = [query for query, _ in connection.the_cursor.queries]

            self.assertTrue(
                any("DELETE FROM candidate_rankings" in query for query in queries)
            )
            self.assertTrue(
                any("INSERT INTO candidate_rankings" in query for query in queries)
            )
            self.assertFalse(
                any("INSERT INTO recommendations" in query for query in queries)
            )
            self.assertFalse(
                any("SELECT id, title, entity_type FROM runbooks" in query for query in queries)
            )



if __name__ == "__main__":
    unittest.main()