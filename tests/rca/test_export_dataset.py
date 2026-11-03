from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rca.export_dataset import (
    DatasetExportError,
    build_candidate_records,
    fetch_dataset_inputs,
    freeze_run_provenance,
    main,
    normalize_include_run_ids,
    provenance_sha256,
    selection_manifest,
    write_dataset,
)


class DatasetExporterTests(unittest.TestCase):
    def candidate(self, row_id: int, entity_type: str, entity_id: str) -> dict:
        return {
            "candidate_row_id": row_id,
            "incident_id": "incident-1",
            "run_id": "run-1",
            "entity_type": entity_type,
            "entity_id": entity_id,
            "rank": row_id,
            "score": 0.9 / row_id,
            "probability": 0.7 / row_id,
            "confidence": "high" if row_id == 1 else "medium",
            "fault_class": "latency",
            "model_version": "heuristic-v1",
        }

    def test_exact_ground_truth_match_and_evidence_only_features(self) -> None:
        candidates = [
            self.candidate(1, "service", "frontend"),
            self.candidate(2, "dependency_edge", "productcatalog->postgres"),
        ]
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "edge",
                "entity_id": "productcatalog→postgres",
                "fault_class": "database_lock",
                "relevance": 3,
                "is_primary": True,
            }
        ]
        evidence = [
            {
                "candidate_id": 2,
                "evidence_type": "signal_anomaly",
                "signal_name": "latency_ms_p95",
                "value": 1200.0,
                "baseline": 20.0,
                "contribution": 0.4,
                "details": {"robust_z": 12.0},
            },
            {
                "candidate_id": 2,
                "evidence_type": "topology_propagation",
                "signal_name": "upstream_coverage",
                "value": 0.8,
                "baseline": 0.0,
                "contribution": 0.2,
                "details": {},
            },
        ]

        records, summary = build_candidate_records(candidates, labels, evidence)

        self.assertEqual(summary["positive_records"], 1)
        positive = next(record for record in records if record["label"])
        self.assertEqual(
            positive["candidate_id"], "dependency_edge:productcatalog->postgres"
        )
        self.assertEqual(positive["candidate_entity_id"], "productcatalog->postgres")
        self.assertEqual(positive["fault_type"], "database_lock")
        self.assertEqual(positive["features"]["evidence_count"], 2.0)
        self.assertEqual(positive["features"]["evidence_max_abs_robust_z"], 12.0)
        self.assertNotIn("ground_truth", positive["features"])
        self.assertNotIn("fault_type", positive["features"])

    def test_missing_true_candidate_fails_closed(self) -> None:
        candidates = [self.candidate(1, "service", "frontend")]
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "service",
                "entity_id": "payment",
                "fault_class": "service_error",
                "relevance": 3,
                "is_primary": True,
            }
        ]
        with self.assertRaises(DatasetExportError):
            build_candidate_records(candidates, labels, [])

        records, summary = build_candidate_records(
            candidates, labels, [], require_positive_per_incident=False
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(summary["unmatched_incidents"], ["incident-1"])

    def test_labeled_incident_with_zero_candidates_fails_closed(self) -> None:
        candidates = [self.candidate(1, "service", "frontend")]
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "service",
                "entity_id": "frontend",
                "fault_class": "service_error",
                "relevance": 3,
                "is_primary": True,
            },
            {
                "run_id": "run-2",
                "incident_id": "incident-2",
                "entity_type": "service",
                "entity_id": "payment",
                "fault_class": "service_error",
                "relevance": 3,
                "is_primary": True,
            },
        ]
        eligible = [
            {"run_id": "run-1", "incident_id": "incident-1"},
            {"run_id": "run-2", "incident_id": "incident-2"},
        ]
        with self.assertRaises(DatasetExportError):
            build_candidate_records(
                candidates, labels, [], eligible_incidents=eligible
            )

        records, summary = build_candidate_records(
            candidates,
            labels,
            [],
            eligible_incidents=eligible,
            require_positive_per_incident=False,
        )
        self.assertEqual(len(records), 1)
        self.assertEqual(summary["eligible_incidents"], 2)
        self.assertEqual(summary["unmatched_incidents"], ["incident-2"])

    def test_declared_split_is_preserved(self) -> None:
        candidate = self.candidate(1, "service", "frontend")
        candidate["dataset_split"] = "holdout"
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "service",
                "entity_id": "frontend",
                "fault_class": "service_error",
                "relevance": 3,
                "is_primary": True,
            }
        ]
        records, _ = build_candidate_records([candidate], labels, [])
        self.assertEqual(records[0]["split"], "holdout")

    def test_abstention_row_is_not_a_supervised_candidate(self) -> None:
        candidates = [
            self.candidate(1, "service", "frontend"),
            self.candidate(2, "system", "inconclusive"),
        ]
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "service",
                "entity_id": "frontend",
                "fault_class": "service_error",
                "relevance": 3,
                "is_primary": True,
            }
        ]
        records, summary = build_candidate_records(candidates, labels, [])
        self.assertEqual([row["candidate_id"] for row in records], ["service:frontend"])
        self.assertEqual(summary["skipped_abstention_candidates"], 1)
        self.assertEqual(records[0]["schema_version"], 2)

    def test_learned_legacy_row_without_online_snapshot_fails_closed(self) -> None:
        candidate = self.candidate(1, "service", "frontend")
        candidate["model_version"] = "champion-portable-linear-sha256:abc"
        labels = [
            {
                "run_id": "run-1",
                "incident_id": None,
                "entity_type": "service",
                "entity_id": "frontend",
                "fault_class": "latency",
                "relevance": 3,
                "is_primary": True,
            }
        ]
        with self.assertRaisesRegex(DatasetExportError, "missing its captured online feature"):
            build_candidate_records([candidate], labels, [])

        candidate["online_features"] = {"baseline_score": 0.75}
        records, _ = build_candidate_records([candidate], labels, [])
        self.assertEqual(records[0]["features"], {"baseline_score": 0.75})

    def test_run_provenance_is_sorted_and_content_addressed(self) -> None:
        frozen = freeze_run_provenance(
            [
                {"run_id": "run-2", "source_sha": "b", "environment": {"z": 1}},
                {"run_id": "run-1", "source_sha": "a", "environment": {"z": 2}},
            ]
        )
        self.assertEqual([row["run_id"] for row in frozen], ["run-1", "run-2"])
        self.assertEqual(provenance_sha256(frozen), provenance_sha256(list(frozen)))

    def test_explicit_selection_is_normalized_and_frozen(self) -> None:
        first = "11111111-1111-4111-8111-111111111111"
        second = "22222222-2222-4222-8222-222222222222"
        self.assertEqual(
            normalize_include_run_ids([second.upper(), first, second]),
            (first, second),
        )
        manifest = selection_manifest(
            include_run_ids=[second, first],
            source_repository="https://example.test/repository.git",
            source_version="v1",
            source_sha="abc123",
        )
        self.assertEqual(manifest["include_run_ids"], [first, second])
        self.assertEqual(manifest["source_repository"], "https://example.test/repository.git")
        self.assertTrue(manifest["training_eligible_only"])
        self.assertTrue(manifest["reserved_run_excluded"])

        with self.assertRaisesRegex(DatasetExportError, "invalid --include-run UUID"):
            normalize_include_run_ids(["not-a-uuid"])
        with self.assertRaisesRegex(DatasetExportError, "reserved live-telemetry"):
            normalize_include_run_ids(["00000000-0000-0000-0000-000000000000"])

    def test_atomic_output_has_reproducible_checksum_and_metadata(self) -> None:
        records = [
            {
                "schema_version": 1,
                "run_id": "run-1",
                "incident_id": "incident-1",
                "candidate_id": "payment",
                "candidate_type": "service",
                "label": 1,
                "label_relevance": 3,
                "fault_type": "service_error",
                "model_version": "heuristic-v1",
                "features": {"baseline_score": 0.9},
            }
        ]
        summary = {"records": 1, "runs": 1, "incidents": 1, "positive_records": 1}
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "dataset.jsonl"
            artifact, metadata, checksum = write_dataset(records, output, summary)
            body = artifact.read_bytes()
            self.assertEqual(checksum, hashlib.sha256(body).hexdigest())
            payload = json.loads(metadata.read_text(encoding="utf-8"))
            self.assertEqual(payload["sha256"], checksum)
            self.assertEqual(payload["records"], 1)

            with self.assertRaises(DatasetExportError):
                write_dataset([{**records[0], "label": 0}], output, summary)
            self.assertEqual(artifact.read_bytes(), body)

            manifest_body = metadata.read_bytes()
            with self.assertRaisesRegex(DatasetExportError, "immutable dataset manifest"):
                write_dataset(records, output, {**summary, "runs": 2})
            self.assertEqual(metadata.read_bytes(), manifest_body)


class _FakeTransaction:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False


class _FakeCursor:
    def __init__(self, result_sets):
        self.result_sets = iter(result_sets)
        self.current = []
        self.queries: list[str] = []
        self.parameters: list[object] = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, parameters=None):
        rendered = str(query)
        self.queries.append(rendered)
        self.parameters.append(parameters)
        if not rendered.startswith("SET TRANSACTION"):
            self.current = next(self.result_sets)

    def fetchall(self):
        return self.current


class _FakeConnection:
    def __init__(self, result_sets):
        self.cursor_instance = _FakeCursor(result_sets)
        self.transaction_entered = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def transaction(self):
        self.transaction_entered = True
        return _FakeTransaction()

    def cursor(self):
        return self.cursor_instance


class DatasetDatabaseReadTests(unittest.TestCase):
    def test_export_reads_one_repeatable_read_snapshot_and_excludes_sentinel(self) -> None:
        connection = _FakeConnection([[], [], [], [], [], []])
        with patch("psycopg.connect", return_value=connection):
            result = fetch_dataset_inputs("postgresql://unused")
        self.assertEqual(result, ([], [], [], [], []))
        self.assertTrue(connection.transaction_entered)
        self.assertTrue(
            connection.cursor_instance.queries[0].startswith(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
            )
        )
        selected_sql = "\n".join(connection.cursor_instance.queries[1:])
        self.assertIn("00000000-0000-0000-0000-000000000000", selected_sql)
        self.assertIn("training_eligible", selected_sql)
        self.assertIn("ORDER BY g.run_id::text", connection.cursor_instance.queries[1])

    def test_exact_selectors_are_applied_to_every_database_read(self) -> None:
        run_id = "11111111-1111-4111-8111-111111111111"
        connection = _FakeConnection([[], [], [], [], [], []])
        with patch("psycopg.connect", return_value=connection):
            fetch_dataset_inputs(
                "postgresql://unused",
                include_run_ids=[run_id],
                source_repository="https://example.test/repository.git",
                source_version="v1",
                source_sha="abc123",
            )

        for query in connection.cursor_instance.queries[1:]:
            self.assertIn("er.id = ANY(%s::uuid[])", query)
            self.assertIn("er.source_repository = %s", query)
            self.assertIn("er.source_version = %s", query)
            self.assertIn("er.source_sha = %s", query)
        expected = [[run_id], "https://example.test/repository.git", "v1", "abc123"]
        for parameters in connection.cursor_instance.parameters[1:]:
            self.assertEqual(parameters, expected)

    def test_main_freezes_selection_in_immutable_metadata(self) -> None:
        run_id = "11111111-1111-4111-8111-111111111111"
        candidate = {
            "candidate_row_id": 1,
            "incident_id": "incident-1",
            "run_id": run_id,
            "entity_type": "service",
            "entity_id": "payment",
            "rank": 1,
            "score": 0.9,
            "probability": 0.8,
            "confidence": "high",
            "fault_class": "service_error",
            "model_version": "heuristic-v1",
            "online_features": {"baseline_score": 0.9},
            "dataset_split": None,
        }
        label = {
            "run_id": run_id,
            "incident_id": None,
            "entity_type": "service",
            "entity_id": "payment",
            "fault_class": "service_error",
            "relevance": 3,
            "is_primary": True,
        }
        provenance = {
            "run_id": run_id,
            "name": "real-run",
            "source_repository": "https://example.test/repository.git",
            "source_version": "v1",
            "source_sha": "abc123",
        }
        inputs = (
            [candidate],
            [label],
            [],
            [{"run_id": run_id, "incident_id": "incident-1"}],
            [provenance],
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "selected.jsonl"
            with patch("rca.export_dataset.fetch_dataset_inputs", return_value=inputs):
                self.assertEqual(
                    main(
                        [
                            "--database-url",
                            "postgresql://unused",
                            "--output",
                            str(output),
                            "--include-run",
                            run_id,
                            "--source-repository",
                            "https://example.test/repository.git",
                            "--source-version",
                            "v1",
                            "--source-sha",
                            "abc123",
                            "--no-register-snapshot",
                        ]
                    ),
                    0,
                )
            metadata = json.loads(
                output.with_suffix(".jsonl.metadata.json").read_text(encoding="utf-8")
            )
            self.assertEqual(metadata["selection"]["include_run_ids"], [run_id])
            self.assertEqual(metadata["selection"]["source_sha"], "abc123")
            self.assertEqual(metadata["run_provenance"][0]["run_id"], run_id)

    def test_main_fails_if_an_explicit_run_is_not_eligible(self) -> None:
        run_id = "11111111-1111-4111-8111-111111111111"
        with patch(
            "rca.export_dataset.fetch_dataset_inputs",
            return_value=([], [], [], [], []),
        ):
            with self.assertRaisesRegex(DatasetExportError, "not eligible"):
                main(
                    [
                        "--database-url",
                        "postgresql://unused",
                        "--include-run",
                        run_id,
                        "--no-register-snapshot",
                    ]
                )

    def test_positive_labeled_run_without_incident_fails_before_export(self) -> None:
        connection = _FakeConnection([[{"run_id": "run-without-incident"}]])
        with patch("psycopg.connect", return_value=connection):
            with self.assertRaisesRegex(DatasetExportError, "no matching incident"):
                fetch_dataset_inputs("postgresql://unused")


if __name__ == "__main__":
    unittest.main()
