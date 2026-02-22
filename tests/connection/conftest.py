"""Connection test helpers."""

from __future__ import annotations

from unittest.mock import MagicMock

from ducktracker.config import DuckTrackerConfig


def recorded_conn():
    """Returns (conn_mock, executed_list) for tracking SQL calls."""
    executed = []
    conn = MagicMock()
    conn.execute.side_effect = executed.append
    return conn, executed


def pg_cfg(**kwargs) -> DuckTrackerConfig:
    """Creates a pg_duckdb DuckTrackerConfig with sensible defaults."""
    defaults = dict(
        catalog_backend="pg_duckdb",
        catalog_name="pg_catalog_test",
        postgres_connection="dbname=mydb host=localhost",
        target_schema="public",
    )
    defaults.update(kwargs)
    return DuckTrackerConfig(**defaults)
