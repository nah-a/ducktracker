"""Migrator-specific test fixtures."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from ducktracker.config import DuckTrackerConfig
from ducktracker.history.ducklake import DuckLakeHistoryManager
from ducktracker.introspection.ducklake import DuckLakeIntrospector


@pytest.fixture
def mgr() -> DuckLakeHistoryManager:
    return DuckLakeHistoryManager()


@pytest.fixture
def introspector() -> DuckLakeIntrospector:
    return DuckLakeIntrospector()


@pytest.fixture
def cfg(migrations_dir: Path) -> DuckTrackerConfig:
    return DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(migrations_dir),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )


@pytest.fixture
def setup_conn(conn: duckdb.DuckDBPyConnection, mgr: DuckLakeHistoryManager):
    """Connection with history table created."""
    mgr.ensure_history_table(conn, "memory", "main", "ducktracker_schema_history")
    return conn
