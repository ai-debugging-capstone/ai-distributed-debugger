
"""Read-only preview of PostgreSQL log retention eligibility."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

import psycopg


RESERVED_RUN_ID = "00000000-0000-0000-0000-000000000000"


def count_eligible_logs(database_url: str, retention_days: int) -> int:
    """Count old logs belonging to terminal experiments."""

    if retention_days <= 0:
        raise ValueError("retention_days must be positive")

    cutoff = datetime.now(timezone.utc) - timedelta(days=retention_days)

    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM log_records AS logs
                JOIN experiment_runs AS runs ON runs.id = logs.run_id
                WHERE runs.status IN ('completed', 'failed')
                  AND runs.id <> %s::uuid
                  AND logs.received_at < %s
                """,
                (RESERVED_RUN_ID, cutoff),
            )
            row = cursor.fetchone()

    return int(row[0]) if row is not None else 0


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preview logs eligible for retention cleanup."
    )
    parser.add_argument(
        "--database-url",
        default=os.environ.get("DATABASE_URL"),
    )
    parser.add_argument(
        "--retention-days",
        type=int,
        default=int(os.environ.get("RCA_LOG_RETENTION_DAYS", "30")),
    )
    args = parser.parse_args()

    if not args.database_url:
        parser.error("DATABASE_URL or --database-url is required")

    eligible = count_eligible_logs(args.database_url, args.retention_days)

    print(f"Retention period: {args.retention_days} days")
    print(f"Eligible log records: {eligible}")
    print("Dry run only. No records were deleted.")


if __name__ == "__main__":
    main()
