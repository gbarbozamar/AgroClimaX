"""
Shared pytest fixtures for apps/backend tests.

Before this file, each test module duplicated env setup + SQLite boilerplate.
Now every file can rely on:
  - `asyncio_mode = "auto"` (see pyproject.toml / pytest.ini options)
  - Testing env overrides (APP_ENV=testing, bypass auth)
  - Clean SQLite-in-memory engine per test function

Tests that need a real Postgres/Copernicus setup should be decorated with
`@pytest.mark.integration` and run explicitly with `pytest -m integration`.
"""
from __future__ import annotations

import os

import pytest

# Force testing env before any app module is imported
os.environ.setdefault("APP_ENV", "testing")
os.environ.setdefault("AUTH_BYPASS_FOR_TESTS", "true")
os.environ.setdefault("SECRET_KEY", "pytest-dev-secret")
os.environ.setdefault("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
os.environ.setdefault("DATABASE_SYNC_URL", "sqlite:///:memory:")
os.environ.setdefault("DATABASE_USE_POSTGIS", "false")
os.environ.setdefault("PIPELINE_SCHEDULER_ENABLED", "false")
os.environ.setdefault("PIPELINE_STARTUP_WARMUP_ENABLED", "false")


def pytest_collection_modifyitems(config, items):
    """Skip integration tests by default unless explicitly requested."""
    if config.getoption("-m") and "integration" in config.getoption("-m"):
        return
    skip_integration = pytest.mark.skip(reason="integration test: run with -m integration")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture
def anyio_backend():
    """pytest-asyncio compat for any `anyio` backed tests."""
    return "asyncio"
