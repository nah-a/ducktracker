"""Tests for DuckLakeHistoryManager using in-memory DuckDB."""
from __future__ import annotations

from datetime import datetime

import pytest

from ducktracker.history import HistoryManagerBase
from ducktracker.history.ducklake import DuckLakeHistoryManager
from ducktracker.models import SchemaSnapshot


@pytest.fixture
def mgr() -> DuckLakeHistoryManager:
    return DuckLakeHistoryManager()


@pytest.fixture
def history_table_ducklake(conn, mgr):
    table_name = "ducktracker_schema_history"
    mgr.ensure_history_table(conn, "memory", "main", table_name)
    return table_name


def test_ducklake_history_manager_is_base(mgr):
    assert isinstance(mgr, HistoryManagerBase)


def test_ensure_history_table_idempotent(conn, mgr):
    mgr.ensure_history_table(conn, "memory", "main", "test_history")
    mgr.ensure_history_table(conn, "memory", "main", "test_history")
    rows = conn.execute("SELECT COUNT(*) FROM memory.main.test_history").fetchone()
    assert rows[0] == 0


def test_record_and_retrieve(conn, mgr, history_table_ducklake):
    mgr.record_migration(
        conn=conn, catalog="memory", schema="main", table=history_table_ducklake,
        version=1, description="create_users", migration_type="V",
        script="V1__create_users.sql", checksum="abc123",
        execution_time_ms=42, success=True,
    )
    applied = mgr.get_applied_migrations(conn, "memory", "main", history_table_ducklake)
    assert len(applied) == 1
    assert applied[0].version == 1
    assert applied[0].checksum == "abc123"
    assert applied[0].success is True


def test_rank_increments(conn, mgr, history_table_ducklake):
    for i in range(1, 4):
        mgr.record_migration(
            conn=conn, catalog="memory", schema="main", table=history_table_ducklake,
            version=i, description=f"migration_{i}", migration_type="V",
            script=f"V{i}__migration_{i}.sql", checksum=f"hash{i}",
            execution_time_ms=10, success=True,
        )
    applied = mgr.get_applied_migrations(conn, "memory", "main", history_table_ducklake)
    assert [a.installed_rank for a in applied] == [1, 2, 3]


def test_get_applied_returns_empty_when_table_missing(conn, mgr):
    applied = mgr.get_applied_migrations(conn, "memory", "main", "nonexistent_table")
    assert applied == []


def test_snapshot_roundtrip(conn, mgr, history_table_ducklake):
    snapshot = SchemaSnapshot(
        catalog_name="test",
        captured_at=datetime(2026, 2, 21, 10, 0, 0),
        schemas=("main",),
        tables=(), views=(), indexes=(), sequences=(), macros=(),
    )
    mgr.record_migration(
        conn=conn, catalog="memory", schema="main", table=history_table_ducklake,
        version=1, description="test", migration_type="V",
        script="V1__test.sql", checksum="abc",
        execution_time_ms=10, success=True, snapshot_json=snapshot.to_json(),
    )
    retrieved = mgr.get_latest_snapshot(conn, "memory", "main", history_table_ducklake)
    assert retrieved is not None
    assert retrieved.catalog_name == "test"


def test_get_latest_snapshot_returns_none_when_empty(conn, mgr, history_table_ducklake):
    assert mgr.get_latest_snapshot(conn, "memory", "main", history_table_ducklake) is None


def test_record_baseline(conn, mgr, history_table_ducklake):
    mgr.record_baseline(conn, "memory", "main", history_table_ducklake,
                        version=5, description="baseline")
    applied = mgr.get_applied_migrations(conn, "memory", "main", history_table_ducklake)
    assert len(applied) == 1
    assert applied[0].script == "<< Baseline V5 >>"
    assert applied[0].checksum == "baseline"
