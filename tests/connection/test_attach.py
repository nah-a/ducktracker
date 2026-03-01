"""Tests for _attach_ducklake."""

from __future__ import annotations

from ducktracker.config import DuckTrackerConfig
from ducktracker.connection import _attach_ducklake

from .conftest import recorded_conn


def test_attach_ducklake_with_secret():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(secret_name="my_secret", catalog_name="lake")
    _attach_ducklake(conn, config)
    attach_sql = next(s for s in executed if s.startswith("ATTACH"))
    assert "ducklake:my_secret" in attach_sql
    assert "ducklake_metadata.db" not in attach_sql
    assert "postgres" not in attach_sql


def test_attach_ducklake_no_secret_uses_duckdb_path():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(duckdb_metadata_path="my_catalog.db", catalog_name="lake")
    _attach_ducklake(conn, config)
    attach_sql = next(s for s in executed if s.startswith("ATTACH"))
    assert "ducklake:my_catalog.db" in attach_sql
    assert "postgres" not in attach_sql


def test_attach_ducklake_secret_with_data_path():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(secret_name="my_secret", data_path="/data/lake", catalog_name="lake")
    _attach_ducklake(conn, config)
    attach_sql = next(s for s in executed if s.startswith("ATTACH"))
    assert "ducklake:my_secret" in attach_sql
    assert "DATA_PATH '/data/lake'" in attach_sql


def test_attach_ducklake_postgres_backend_uses_postgres_uri():
    """catalog_backend='postgres' produces a ducklake:postgres:... URI."""
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(
        catalog_backend="postgres",
        postgres_connection="dbname=mydb host=localhost",
        catalog_name="lake",
    )
    _attach_ducklake(conn, config)
    attach_sql = next(s for s in executed if s.startswith("ATTACH"))
    assert "ducklake:postgres:dbname=mydb host=localhost" in attach_sql


def test_attach_ducklake_read_only_adds_option():
    """read_only=True adds READ_ONLY to the ATTACH options."""
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(read_only=True, catalog_name="lake")
    _attach_ducklake(conn, config)
    attach_sql = next(s for s in executed if s.startswith("ATTACH"))
    assert "READ_ONLY" in attach_sql
