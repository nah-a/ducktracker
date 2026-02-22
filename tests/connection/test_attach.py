"""Tests for _attach_ducklake and _attach_postgres_direct."""

from __future__ import annotations

import logging

from ducktracker.config import DuckTrackerConfig
from ducktracker.connection import _attach_ducklake, _attach_postgres_direct

from .conftest import pg_cfg, recorded_conn


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


def test_attach_postgres_direct_connection_string():
    conn, executed = recorded_conn()
    _attach_postgres_direct(conn, pg_cfg(postgres_connection="dbname=mydb host=localhost"))
    attach_sql = next(s for s in executed if "ATTACH" in s)
    assert "postgres:dbname=mydb host=localhost" in attach_sql
    assert "TYPE postgres" in attach_sql


def test_attach_postgres_direct_secret_name():
    conn, executed = recorded_conn()
    _attach_postgres_direct(conn, pg_cfg(secret_name="my_pg_secret", postgres_connection=""))
    attach_sql = next(s for s in executed if "ATTACH" in s)
    assert 'SECRET "my_pg_secret"' in attach_sql
    assert "TYPE postgres" in attach_sql


def test_attach_postgres_direct_secret_takes_precedence():
    conn, executed = recorded_conn()
    _attach_postgres_direct(
        conn, pg_cfg(secret_name="my_secret", postgres_connection="dbname=ignored")
    )
    attach_sql = next(s for s in executed if "ATTACH" in s)
    assert 'SECRET "my_secret"' in attach_sql
    assert "ignored" not in attach_sql


def test_attach_postgres_direct_warns_on_data_path(caplog):
    conn, _ = recorded_conn()
    with caplog.at_level(logging.WARNING, logger="ducktracker.connection"):
        _attach_postgres_direct(conn, pg_cfg(data_path="/some/path"))
    assert any("data_path" in r.message.lower() for r in caplog.records)


def test_attach_postgres_direct_warns_on_read_only(caplog):
    conn, _ = recorded_conn()
    with caplog.at_level(logging.WARNING, logger="ducktracker.connection"):
        _attach_postgres_direct(conn, pg_cfg(read_only=True))
    assert any("read_only" in r.message.lower() for r in caplog.records)
