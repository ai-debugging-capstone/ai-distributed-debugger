
"""Integration tests for read-only log retention eligibility."""

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
import os

from rca.log_retention import count_eligible_logs, RESERVED_RUN_ID


pytestmark = pytest.mark.postgres


def test_log_retention_excludes_active_and_live_telemetry(postgres_api):
    connection = postgres_api.connection
    now = datetime.now(timezone.utc)
    old = now - timedelta(days=45)
    recent = now - timedelta(days=1)

    experiments = {}

    for status in ("completed", "failed", "running", "warming"):
        run_id = uuid4()
        experiments[status] = run_id

        connection.execute(
            """
            INSERT INTO experiment_runs (id, name, status)
            VALUES (%s, %s, %s)
            """,
            (run_id, f"retention-test-{status}", status),
        )

    records = [
        ("completed-old", experiments["completed"], old),
        ("failed-old", experiments["failed"], old),
        ("running-old", experiments["running"], old),
        ("warming-old", experiments["warming"], old),
        ("completed-recent", experiments["completed"], recent),
        ("live-old", RESERVED_RUN_ID, old),
        ("unassigned-old", None, old),
    ]

    for key, run_id, received_at in records:
        connection.execute(
            """
            INSERT INTO log_records (
                idempotency_key,
                service_name,
                time_unix,
                run_id,
                received_at
            )
            VALUES (%s, %s, %s, %s, %s)
            """,
            (key, "retention-test", old, run_id, received_at),
        )

    connection.commit()

    from urllib.parse import urlsplit, urlunsplit

    admin_url = urlsplit(os.environ["TEST_POSTGRES_ADMIN_URL"])

    database_url = urlunsplit(
        (
            admin_url.scheme,
            admin_url.netloc,
            f"/{postgres_api.database_name}",
            admin_url.query,
            admin_url.fragment,
        )
    )

    eligible = count_eligible_logs(database_url, retention_days=30)

    assert eligible == 2

    total = connection.execute(
        "SELECT COUNT(*) AS total FROM log_records"
    ).fetchone()["total"]

    assert total == 7
