"""Worker restart recovery against the real PostgreSQL schema."""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from rca.domain import Incident
from rca.incidents import IncidentEvent
from rca.worker import RCAWorker, WorkerSettings

pytestmark = pytest.mark.postgres

STARTED = datetime(2026, 1, 1, tzinfo=UTC)


def new_worker() -> RCAWorker:
    return RCAWorker(WorkerSettings("postgresql://unused", analysis_engine="heuristic"))


def start_run(connection) -> str:
    run_id = uuid4()
    connection.execute(
        """INSERT INTO experiment_runs (id, name, status, started_at)
           VALUES (%s, %s, 'running', %s)""",
        (run_id, "restart-recovery-test", STARTED),
    )
    return str(run_id)


def open_incident(connection, run_id: str, started_at: datetime, trigger: str) -> str:
    incident = Incident(
        started_at, started_at, trigger, [trigger], 8.0, "warning", run_id,
        summary=f"Sustained anomaly affecting {trigger}",
    )
    new_worker()._persist_incident(connection, IncidentEvent("opened", incident))
    return incident.id


def incidents_for(connection, run_id: str) -> dict[str, dict]:
    rows = connection.execute(
        "SELECT id::text AS id, status, ended_at, metadata FROM incidents WHERE run_id = %s",
        (run_id,),
    ).fetchall()
    return {row["id"]: row for row in rows}


def test_restarted_worker_readopts_and_resolves_the_same_incident(postgres_api):
    connection = postgres_api.connection
    run_id = start_run(connection)
    original_id = open_incident(connection, run_id, STARTED, "payment")

    restarted = new_worker()
    restarted._restore_open_incidents(connection)
    [restored] = restarted.detector.active_incidents
    assert restored.id == original_id
    assert restored.started_at == STARTED
    assert restored.affected_entities == ["payment"]

    ended = STARTED + timedelta(minutes=1)
    connection.execute(
        "UPDATE experiment_runs SET status = 'completed', ended_at = %s WHERE id = %s",
        (ended, run_id),
    )
    restarted._resolve_terminal_active_incidents(connection)

    stored = incidents_for(connection, run_id)
    assert list(stored) == [original_id]
    assert stored[original_id]["status"] == "resolved"
    assert stored[original_id]["ended_at"] == ended
    assert not restarted.detector.has_active_incident(run_id)


def test_restart_closes_superseded_open_incidents_instead_of_stalling(postgres_api):
    connection = postgres_api.connection
    run_id = start_run(connection)
    # An earlier worker orphaned its incident; the next one opened another.
    older = open_incident(connection, run_id, STARTED, "image-provider")
    newer = open_incident(connection, run_id, STARTED + timedelta(minutes=10), "checkout")

    restarted = new_worker()
    restarted._restore_open_incidents(connection)

    assert [incident.id for incident in restarted.detector.active_incidents] == [newer]
    stored = incidents_for(connection, run_id)
    assert set(stored) == {older, newer}
    assert stored[older]["status"] == "resolved"
    assert stored[older]["ended_at"] == STARTED + timedelta(minutes=10)
    assert stored[older]["metadata"]["resolution"] == "superseded_after_worker_restart"
    assert stored[newer]["status"] == "open"
