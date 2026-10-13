"""Real database fixtures. Never migrate, truncate, or reset the admin database."""

import asyncio
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from uuid import uuid4

import psycopg
import pytest
from fastapi.testclient import TestClient
from psycopg import sql
from psycopg.rows import dict_row

from telemetry_api.config import Settings
from telemetry_api.main import create_app
from telemetry_api.storage import PsycopgStorage

ROOT = Path(__file__).resolve().parents[2]


@dataclass
class DatabaseHarness:
    client: TestClient
    connection: psycopg.Connection
    database_name: str


@pytest.fixture
def postgres_api():
    admin_url = os.environ["TEST_POSTGRES_ADMIN_URL"]
    # Settings accepts PostgreSQL URLs, not arbitrary libpq keyword strings.
    Settings(database_url=admin_url, _env_file=None)
    name = "m5_test_" + uuid4().hex
    parts = urlsplit(admin_url)
    query = urlencode([(key, value) for key, value in parse_qsl(parts.query)
                       if key != "dbname"])
    test_url = urlunsplit((parts.scheme, parts.netloc, "/" + name, query, parts.fragment))
    created = False
    with psycopg.connect(admin_url, autocommit=True, connect_timeout=3) as admin:
        try:
            admin.execute(sql.SQL("CREATE DATABASE {} TEMPLATE template0").format(
                sql.Identifier(name)
            ))
            created = True
            with psycopg.connect(test_url, autocommit=True, row_factory=dict_row,
                                 connect_timeout=3) as connection:
                assert connection.execute("SELECT current_database() AS name").fetchone()[
                    "name"
                ] == name
                for migration in sorted((ROOT / "db" / "init").glob("*.sql")):
                    connection.execute(migration.read_text(encoding="utf-8"))
                settings = Settings(
                    database_url=test_url, database_min_pool_size=1, database_max_pool_size=2,
                    database_connect_timeout_seconds=3, database_command_timeout_seconds=5,
                    _env_file=None,
                )
                backend_options = {}
                if sys.platform == "win32":
                    # Psycopg async needs a selector loop. Scope it to this TestClient;
                    # do not change the process/global event-loop policy.
                    backend_options["loop_factory"] = asyncio.SelectorEventLoop
                with TestClient(create_app(settings=settings, storage=PsycopgStorage(settings)),
                                backend_options=backend_options) as client:
                    response = client.get("/health/ready")
                    assert response.status_code == 200, response.text
                    yield DatabaseHarness(client, connection, name)
        finally:
            # Only this fixture's successfully CREATED database can be removed.
            # No DROP ... FORCE: a leaked connection must fail cleanup visibly.
            if created:
                assert re.fullmatch(r"m5_test_[0-9a-f]{32}", name)
                admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
