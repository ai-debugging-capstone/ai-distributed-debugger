"""Explicit opt-in for tests that create disposable PostgreSQL databases."""

import os

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-postgres", action="store_true", default=False,
        help="Run real PostgreSQL tests using TEST_POSTGRES_ADMIN_URL (requires CREATEDB)",
    )


def pytest_configure(config):
    if config.getoption("--run-postgres") and not os.environ.get("TEST_POSTGRES_ADMIN_URL"):
        raise pytest.UsageError("--run-postgres requires TEST_POSTGRES_ADMIN_URL")


def pytest_collection_modifyitems(config, items):
    skip = pytest.mark.skip(reason="real PostgreSQL requires --run-postgres and admin URL")
    for item in items:
        if item.path.name == "test_synthetic_e2e.py":
            item.add_marker(pytest.mark.component)
        if "postgres" in item.keywords and not config.getoption("--run-postgres"):
            item.add_marker(skip)
