"""Tests for DuckLakeIntrospector using in-memory DuckDB."""

from __future__ import annotations

import duckdb
import pytest

from ducktracker.introspection import IntrospectorBase
from ducktracker.introspection.ducklake import DuckLakeIntrospector


@pytest.fixture
def populated_conn(conn: duckdb.DuckDBPyConnection):
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR NOT NULL, email VARCHAR)")
    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total DECIMAL(10,2))")
    conn.execute("CREATE OR REPLACE VIEW active_users AS SELECT * FROM users WHERE name IS NOT NULL")
    return conn


def test_ducklake_introspector_is_introspector_base():
    assert issubclass(DuckLakeIntrospector, IntrospectorBase)


def test_introspect_tables(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory")
    table_names = {t.table_name for t in snap.tables}
    assert "users" in table_names
    assert "orders" in table_names


def test_introspect_columns(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory")
    users = next(t for t in snap.tables if t.table_name == "users")
    col_names = [c.name for c in users.columns]
    assert "id" in col_names
    assert "name" in col_names
    assert "email" in col_names


def test_introspect_column_types(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory")
    users = next(t for t in snap.tables if t.table_name == "users")
    name_col = next(c for c in users.columns if c.name == "name")
    assert "VARCHAR" in name_col.data_type.upper()


def test_introspect_views(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory")
    view_names = {v.view_name for v in snap.views}
    assert "active_users" in view_names


def test_introspect_excludes_tables(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory", exclude_tables=frozenset({"users"}))
    table_names = {t.table_name for t in snap.tables}
    assert "users" not in table_names
    assert "orders" in table_names


def test_introspect_empty_database(conn):
    snap = DuckLakeIntrospector().introspect(conn, "memory")
    assert len(snap.tables) == 0
    assert len(snap.views) == 0


def test_snapshot_serialization_roundtrip(populated_conn):
    snap = DuckLakeIntrospector().introspect(populated_conn, "memory")
    restored = type(snap).from_json(snap.to_json())
    assert restored.catalog_name == snap.catalog_name
    assert len(restored.tables) == len(snap.tables)
    assert len(restored.views) == len(snap.views)


def test_introspect_sequences(conn):
    """DuckLakeIntrospector captures sequences with correct metadata."""
    conn.execute("CREATE SEQUENCE main.my_seq START 10 INCREMENT 2")
    snap = DuckLakeIntrospector().introspect(conn, "memory")
    seq_names = {s.sequence_name for s in snap.sequences}
    assert "my_seq" in seq_names
    seq = next(s for s in snap.sequences if s.sequence_name == "my_seq")
    assert seq.start_value == 10
    assert seq.increment_by == 2


def test_introspect_macros(conn):
    """DuckLakeIntrospector captures macros defined in the catalog."""
    conn.execute("CREATE MACRO main.double_val(x) AS x * 2")
    snap = DuckLakeIntrospector().introspect(conn, "memory")
    macro_names = {m.macro_name for m in snap.macros}
    assert "double_val" in macro_names
    m = next(m for m in snap.macros if m.macro_name == "double_val")
    assert m.macro_type == "scalar"


def test_introspect_handles_query_errors_gracefully():
    """When catalog queries fail, all _get_* methods return empty tuples without raising."""
    from unittest.mock import MagicMock

    bad_conn = MagicMock()
    bad_conn.execute.side_effect = duckdb.Error("simulated catalog error")

    snap = DuckLakeIntrospector().introspect(bad_conn, "test_catalog")
    assert snap.tables == ()
    assert snap.views == ()
    assert snap.sequences == ()
    assert snap.macros == ()
    assert snap.indexes == ()
    assert snap.schemas == ()


def test_introspect_unique_constraints(conn):
    """UNIQUE constraints on a table are captured in unique_constraints."""
    conn.execute("CREATE TABLE main.items (id INTEGER PRIMARY KEY, code VARCHAR UNIQUE)")
    snap = DuckLakeIntrospector().introspect(conn, "memory")
    items = next(t for t in snap.tables if t.table_name == "items")
    assert len(items.unique_constraints) >= 1


def test_get_constraints_handles_query_error():
    """_get_constraints returns (None, ()) without raising when the query fails."""
    from unittest.mock import MagicMock

    introspector = DuckLakeIntrospector()
    bad_conn = MagicMock()
    bad_conn.execute.side_effect = duckdb.Error("constraint query failed")
    pk, ucs = introspector._get_constraints(bad_conn, "memory", "main", "items")
    assert pk is None
    assert ucs == ()
