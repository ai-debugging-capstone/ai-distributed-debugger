from __future__ import annotations

from datetime import UTC, datetime

import pytest

from telemetry_api.models import ExperimentRunUpdate
from telemetry_api.storage import PsycopgStorage

RUN_ID = "d507d090-d8cc-4a66-a483-2d677b77643f"


class RecordingCursor:
    def __init__(self, returned_row: dict[str, object]) -> None:
        self.returned_row = returned_row
        self.executions: list[tuple[str, object]] = []

    async def __aenter__(self) -> RecordingCursor:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def execute(self, statement: str, parameters: object) -> None:
        self.executions.append((statement, parameters))

    async def fetchone(self) -> dict[str, object]:
        return self.returned_row


class RecordingConnection:
    def __init__(self, cursor: RecordingCursor) -> None:
        self._cursor = cursor
        self.committed = False

    async def __aenter__(self) -> RecordingConnection:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    def cursor(self) -> RecordingCursor:
        return self._cursor

    async def commit(self) -> None:
        self.committed = True


class RecordingPool:
    def __init__(self, connection: RecordingConnection) -> None:
        self._connection = connection

    def connection(self) -> RecordingConnection:
        return self._connection


def make_storage(returned_row: dict[str, object]) -> tuple[
    PsycopgStorage, RecordingCursor, RecordingConnection
]:
    cursor = RecordingCursor(returned_row)
    connection = RecordingConnection(cursor)
    storage = object.__new__(PsycopgStorage)
    storage._pool = RecordingPool(connection)  # type: ignore[attr-defined]
    return storage, cursor, connection


@pytest.mark.asyncio
async def test_status_only_patch_binds_omitted_fields_with_explicit_types() -> None:
    storage, cursor, connection = make_storage({"id": RUN_ID, "status": "completed"})

    row = await storage.update_experiment(
        RUN_ID, ExperimentRunUpdate(status="completed")
    )

    assert row == {"id": RUN_ID, "status": "completed"}
    assert connection.committed
    assert len(cursor.executions) == 1
    statement, parameters = cursor.executions[0]
    assert isinstance(parameters, dict)
    assert parameters["status"] == "completed"
    assert parameters["split"] is None
    assert parameters["workload"] is None
    assert parameters["started_at"] is None
    assert parameters["ended_at"] is None
    assert parameters["configuration"].obj == {}
    assert parameters["environment"].obj == {}
    for typed_binding in (
        "%(status)s::text AS status",
        "%(split)s::text AS split",
        "%(workload)s::text AS workload",
        "%(started_at)s::timestamptz AS started_at",
        "%(ended_at)s::timestamptz AS ended_at",
        "%(configuration)s::jsonb AS configuration",
        "%(environment)s::jsonb AS environment",
    ):
        assert typed_binding in statement
    assert "%(split)s IS NULL" not in statement


@pytest.mark.asyncio
async def test_ended_at_only_patch_leaves_other_optional_fields_omitted() -> None:
    ended_at = datetime(2026, 9, 30, 16, 4, tzinfo=UTC)
    storage, cursor, connection = make_storage(
        {"id": RUN_ID, "status": "running", "ended_at": ended_at}
    )

    row = await storage.update_experiment(
        RUN_ID, ExperimentRunUpdate(ended_at=ended_at)
    )

    assert row == {"id": RUN_ID, "status": "running", "ended_at": ended_at}
    assert connection.committed
    assert len(cursor.executions) == 1
    _statement, parameters = cursor.executions[0]
    assert isinstance(parameters, dict)
    assert parameters["status"] is None
    assert parameters["split"] is None
    assert parameters["workload"] is None
    assert parameters["started_at"] is None
    assert parameters["ended_at"] == ended_at
    assert parameters["configuration"].obj == {}
    assert parameters["environment"].obj == {}
