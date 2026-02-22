"""Tests for PostgresNativeHistoryManager. Skips if Postgres unavailable."""
from __future__ import annotations

from datetime import datetime

import duckdb
import pytest

from ducktracker.history import HistoryManagerBase
from ducktracker.history.postgres import PostgresNativeHistoryManager
from ducktracker.models import SchemaSnapshot

TABLE = "dt_history_test"
SCHEMA = "dt_hist_schema"


@pytest.fixture(scope="module")
def pg_conn():
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
        conn.execute(
            "ATTACH 'postgres:dbname=postgres host=localhost port=5432' "
            "AS pg_hist (TYPE postgres)"
        )
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS pg_hist.{SCHEMA}")
        conn.execute(f"USE pg_hist.{SCHEMA}")
    except Exception as e:
        conn.close()
        pytest.skip(f"Postgres unavailable: {e}")
    yield conn
    try:
        conn.execute(f"DROP SCHEMA IF EXISTS pg_hist.{SCHEMA} CASCADE")
    except Exception:
        pass
    conn.close()


@pytest.fixture(autouse=True)
def clean_table(pg_conn):
    try:
        pg_conn.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{TABLE}")
    except Exception:
        pass
    yield


@pytest.fixture
def mgr():
    return PostgresNativeHistoryManager()


def test_postgres_history_manager_is_base(mgr):
    assert isinstance(mgr, HistoryManagerBase)


def test_ensure_history_table_idempotent(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    rows = pg_conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{TABLE}").fetchone()
    assert rows[0] == 0


def test_record_and_retrieve(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    mgr.record_migration(
        conn=pg_conn, catalog="pg_hist", schema=SCHEMA, table=TABLE,
        version=1, description="create_orders", migration_type="V",
        script="V1__create_orders.sql", checksum="xyz789",
        execution_time_ms=55, success=True,
    )
    applied = mgr.get_applied_migrations(pg_conn, "pg_hist", SCHEMA, TABLE)
    assert len(applied) == 1
    assert applied[0].version == 1
    assert applied[0].checksum == "xyz789"
    assert applied[0].success is True


def test_get_applied_returns_empty_when_table_missing(pg_conn, mgr):
    applied = mgr.get_applied_migrations(pg_conn, "pg_hist", SCHEMA, "nonexistent_table")
    assert applied == []


def test_rank_increments(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    for i in range(1, 4):
        mgr.record_migration(
            conn=pg_conn, catalog="pg_hist", schema=SCHEMA, table=TABLE,
            version=i, description=f"m{i}", migration_type="V",
            script=f"V{i}__m{i}.sql", checksum=f"h{i}",
            execution_time_ms=5, success=True,
        )
    applied = mgr.get_applied_migrations(pg_conn, "pg_hist", SCHEMA, TABLE)
    assert [a.installed_rank for a in applied] == [1, 2, 3]


def test_snapshot_roundtrip(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    snapshot = SchemaSnapshot(
        catalog_name="pg_hist",
        captured_at=datetime(2026, 2, 21),
        schemas=(SCHEMA,), tables=(), views=(), indexes=(), sequences=(), macros=(),
    )
    mgr.record_migration(
        conn=pg_conn, catalog="pg_hist", schema=SCHEMA, table=TABLE,
        version=1, description="snap_test", migration_type="V",
        script="V1__snap.sql", checksum="snap_hash",
        execution_time_ms=1, success=True, snapshot_json=snapshot.to_json(),
    )
    retrieved = mgr.get_latest_snapshot(pg_conn, "pg_hist", SCHEMA, TABLE)
    assert retrieved is not None
    assert retrieved.catalog_name == "pg_hist"


def test_get_latest_snapshot_returns_none_when_empty(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    assert mgr.get_latest_snapshot(pg_conn, "pg_hist", SCHEMA, TABLE) is None


def test_record_baseline(pg_conn, mgr):
    mgr.ensure_history_table(pg_conn, "pg_hist", SCHEMA, TABLE)
    mgr.record_baseline(pg_conn, "pg_hist", SCHEMA, TABLE, version=3, description="initial")
    applied = mgr.get_applied_migrations(pg_conn, "pg_hist", SCHEMA, TABLE)
    assert len(applied) == 1
    assert applied[0].checksum == "baseline"
    assert applied[0].script == "<< Baseline V3 >>"
