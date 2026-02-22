"""Tests for backend factory functions."""
from __future__ import annotations

from ducktracker.backends import get_history_manager, get_introspector
from ducktracker.config import DuckTrackerConfig
from ducktracker.history import HistoryManagerBase
from ducktracker.history.ducklake import DuckLakeHistoryManager
from ducktracker.history.postgres import PostgresNativeHistoryManager
from ducktracker.introspection import IntrospectorBase
from ducktracker.introspection.ducklake import DuckLakeIntrospector
from ducktracker.introspection.postgres import PostgresNativeIntrospector


def _cfg(backend: str) -> DuckTrackerConfig:
    return DuckTrackerConfig(catalog_backend=backend)


def test_get_introspector_duckdb_returns_ducklake():
    assert isinstance(get_introspector(_cfg("duckdb")), DuckLakeIntrospector)


def test_get_introspector_postgres_returns_ducklake():
    assert isinstance(get_introspector(_cfg("postgres")), DuckLakeIntrospector)


def test_get_introspector_pg_duckdb_returns_postgres_native():
    assert isinstance(get_introspector(_cfg("pg_duckdb")), PostgresNativeIntrospector)


def test_get_history_manager_duckdb_returns_ducklake():
    assert isinstance(get_history_manager(_cfg("duckdb")), DuckLakeHistoryManager)


def test_get_history_manager_postgres_returns_ducklake():
    assert isinstance(get_history_manager(_cfg("postgres")), DuckLakeHistoryManager)


def test_get_history_manager_pg_duckdb_returns_postgres_native():
    assert isinstance(get_history_manager(_cfg("pg_duckdb")), PostgresNativeHistoryManager)


def test_get_introspector_returns_introspector_base():
    for backend in ("duckdb", "postgres", "pg_duckdb"):
        assert isinstance(get_introspector(_cfg(backend)), IntrospectorBase)


def test_get_history_manager_returns_history_manager_base():
    for backend in ("duckdb", "postgres", "pg_duckdb"):
        assert isinstance(get_history_manager(_cfg(backend)), HistoryManagerBase)
