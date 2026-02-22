"""Tests for _setup_extensions."""

from __future__ import annotations

from ducktracker.config import DuckTrackerConfig
from ducktracker.connection import _setup_extensions

from .conftest import pg_cfg, recorded_conn


def test_setup_extensions_installs_from_repo_by_default():
    conn, executed = recorded_conn()
    _setup_extensions(conn, DuckTrackerConfig())
    assert "INSTALL ducklake" in executed
    assert "LOAD ducklake" in executed


def test_setup_extensions_uses_absolute_path_for_ducklake():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(extensions_path="/opt/extensions")
    _setup_extensions(conn, config)
    assert "INSTALL '/opt/extensions/ducklake.duckdb_extension'" in executed
    assert "LOAD ducklake" in executed
    assert "INSTALL ducklake" not in executed


def test_setup_extensions_no_postgres_by_default():
    conn, executed = recorded_conn()
    _setup_extensions(conn, DuckTrackerConfig())
    assert "INSTALL postgres" not in executed
    assert "LOAD postgres" not in executed


def test_setup_extensions_postgres_backend_installs_from_repo():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(catalog_backend="postgres")
    _setup_extensions(conn, config)
    assert "INSTALL postgres" in executed
    assert "LOAD postgres" in executed


def test_setup_extensions_postgres_backend_uses_absolute_path():
    conn, executed = recorded_conn()
    config = DuckTrackerConfig(catalog_backend="postgres", extensions_path="/opt/extensions")
    _setup_extensions(conn, config)
    assert "INSTALL '/opt/extensions/ducklake.duckdb_extension'" in executed
    assert "INSTALL '/opt/extensions/postgres.duckdb_extension'" in executed
    assert "LOAD ducklake" in executed
    assert "LOAD postgres" in executed
    assert "INSTALL ducklake" not in executed
    assert "INSTALL postgres" not in executed


def test_setup_extensions_pg_duckdb_loads_postgres_not_ducklake():
    conn, executed = recorded_conn()
    _setup_extensions(conn, pg_cfg())
    assert "LOAD postgres" in executed
    assert "LOAD ducklake" not in executed


def test_setup_extensions_pg_duckdb_does_not_install_ducklake():
    conn, executed = recorded_conn()
    _setup_extensions(conn, pg_cfg())
    assert "INSTALL ducklake" not in executed


def test_setup_extensions_pg_duckdb_uses_local_path():
    conn, executed = recorded_conn()
    _setup_extensions(conn, pg_cfg(extensions_path="/opt/ext"))
    assert "INSTALL '/opt/ext/postgres.duckdb_extension'" in executed
    assert "LOAD postgres" in executed
