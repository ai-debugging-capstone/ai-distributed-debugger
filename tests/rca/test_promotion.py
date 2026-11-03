from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from rca.promote import (
    PromotionError,
    activate_deployment_alias,
    promote_registered_model,
    validate_registry_artifact,
)
from rca.train import file_sha256


class _Transaction:
    def __init__(self, events):
        self.events = events

    def __enter__(self):
        self.events.append("begin")
        return self

    def __exit__(self, exc_type, exc, traceback):
        self.events.append("rollback" if exc_type else "commit")
        return False


class _Cursor:
    def __init__(self, events, model=None, previous=None):
        self.events = events
        self.model = model
        self.previous = previous
        self.current = None
        self.rowcount = -1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def execute(self, query, parameters=None):
        normalized = " ".join(str(query).split())
        self.events.append((normalized, parameters))
        self.rowcount = -1
        if normalized.startswith("SELECT id, name, version"):
            self.current = self.model
        elif normalized.startswith("SELECT id FROM model_versions"):
            self.current = self.previous
        elif normalized.startswith("UPDATE model_versions"):
            self.current = None
            self.rowcount = 1

    def fetchone(self):
        return self.current


class _Connection:
    def __init__(self, events, model=None, previous=None):
        self.events = events
        self.cursor_instance = _Cursor(events, model=model, previous=previous)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback):
        return False

    def transaction(self):
        return _Transaction(self.events)

    def cursor(self):
        return self.cursor_instance

    def commit(self):
        self.events.append("commit")


class PromotionTests(unittest.TestCase):
    def test_artifact_validation_rejects_non_challenger_lifecycle(self) -> None:
        with self.assertRaisesRegex(PromotionError, "not promotable"):
            validate_registry_artifact(
                {
                    "status": "retired",
                    "artifact_uri": "unused",
                    "artifact_checksum": "unused",
                }
            )

    def test_registry_commits_before_atomic_alias_activation(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            artifact = root / "model-immutable.json"
            artifact.write_bytes(b"validated-model")
            output = root / "champion"
            events: list[object] = []
            model = {
                "id": "new-id",
                "name": "candidate-ranker",
                "version": "v2",
                "status": "challenger",
                "artifact_uri": str(artifact),
                "artifact_checksum": file_sha256(artifact),
                "dataset_snapshot_id": "snapshot-id",
            }
            connection = _Connection(events, model=model, previous={"id": "old-id"})

            def activate(staged, alias):
                self.assertEqual(events[-1], "commit")
                events.append("activate")
                return activate_deployment_alias(staged, alias)

            with patch("psycopg.connect", return_value=connection), patch(
                "rca.promote.activate_deployment_alias", side_effect=activate
            ):
                result = promote_registered_model(
                    "postgresql://unused",
                    name="candidate-ranker",
                    version="v2",
                    output_dir=output,
                )

            self.assertEqual(result["status"], "champion")
            self.assertEqual(result["previous_champion_id"], "old-id")
            self.assertEqual((output / "model.json").read_bytes(), b"validated-model")
            activate_index = events.index("activate")
            unlock_index = next(
                index
                for index, event in enumerate(events)
                if isinstance(event, tuple) and "pg_advisory_unlock" in event[0]
            )
            self.assertLess(activate_index, unlock_index)

    def test_alias_failure_compensates_registry_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            artifact = root / "model-immutable.json"
            artifact.write_bytes(b"validated-model")
            events: list[object] = []
            model = {
                "id": "new-id",
                "name": "candidate-ranker",
                "version": "v2",
                "status": "challenger",
                "artifact_uri": str(artifact),
                "artifact_checksum": file_sha256(artifact),
                "dataset_snapshot_id": "snapshot-id",
            }
            connection = _Connection(events, model=model, previous={"id": "old-id"})
            with patch("psycopg.connect", return_value=connection), patch(
                "rca.promote.activate_deployment_alias",
                side_effect=OSError("disk full"),
            ):
                with self.assertRaisesRegex(PromotionError, "registry lifecycle was restored"):
                    promote_registered_model(
                        "postgresql://unused",
                        name="candidate-ranker",
                        version="v2",
                        output_dir=root / "champion",
                    )

            updates = [
                event
                for event in events
                if isinstance(event, tuple) and event[0].startswith("UPDATE model_versions")
            ]
            self.assertEqual(len(updates), 4)
            self.assertIn("status = 'challenger'", updates[-2][0])
            self.assertIn("status = 'champion'", updates[-1][0])


if __name__ == "__main__":
    unittest.main()
