"""Tests for PostgresNativeIntrospector. Skips if Postgres is unavailable."""
from __future__ import annotations

import duckdb
import pytest

from ducktracker.introspection import IntrospectorBase
from ducktracker.introspection.postgres import PostgresNativeIntrospector


@pytest.fixture(scope="module")
def pg_conn():
    """DuckDB connection with a local Postgres database attached. Skips if unavailable."""
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")
        conn.execute(
            "ATTACH 'postgres:dbname=postgres host=localhost port=5432' "
            "AS pg_test (TYPE postgres)"
        )
        conn.execute("CREATE SCHEMA IF NOT EXISTS pg_test.dt_introspect_test")
        conn.execute("USE pg_test.dt_introspect_test")
    except Exception as e:
        conn.close()
        pytest.skip(f"Postgres unavailable: {e}")
    yield conn
    try:
        conn.execute("DROP SCHEMA IF EXISTS pg_test.dt_introspect_test CASCADE")
    except Exception:
        pass
    conn.close()


@pytest.fixture(autouse=True)
def clean_schema(pg_conn):
    pg_conn.execute("DROP TABLE IF EXISTS dt_introspect_test.products CASCADE")
    pg_conn.execute("DROP TABLE IF EXISTS dt_introspect_test.orders CASCADE")
    pg_conn.execute("DROP VIEW IF EXISTS dt_introspect_test.recent_orders")
    pg_conn.execute("DROP FUNCTION IF EXISTS dt_introspect_test.get_count()")
    yield


def test_postgres_introspector_is_introspector_base():
    assert issubclass(PostgresNativeIntrospector, IntrospectorBase)


def test_introspect_tables(pg_conn):
    pg_conn.execute(
        "CREATE TABLE dt_introspect_test.products "
        "(id SERIAL PRIMARY KEY, name TEXT NOT NULL, price NUMERIC(10,2))"
    )
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    table_names = {t.table_name for t in snap.tables}
    assert "products" in table_names


def test_introspect_columns(pg_conn):
    pg_conn.execute(
        "CREATE TABLE dt_introspect_test.products "
        "(id SERIAL PRIMARY KEY, name TEXT NOT NULL)"
    )
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    products = next(t for t in snap.tables if t.table_name == "products")
    col_names = [c.name for c in products.columns]
    assert "id" in col_names
    assert "name" in col_names


def test_introspect_views(pg_conn):
    pg_conn.execute(
        "CREATE TABLE dt_introspect_test.orders "
        "(id SERIAL PRIMARY KEY, total NUMERIC)"
    )
    pg_conn.execute(
        "CREATE VIEW dt_introspect_test.recent_orders AS "
        "SELECT * FROM dt_introspect_test.orders WHERE total > 100"
    )
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    view_names = {v.view_name for v in snap.views}
    assert "recent_orders" in view_names


def test_introspect_stored_functions(pg_conn):
    pg_conn.execute("""
        CREATE OR REPLACE FUNCTION dt_introspect_test.get_count()
        RETURNS INTEGER LANGUAGE sql AS $$ SELECT 42 $$
    """)
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    fn_names = {p.name for p in snap.stored_procedures}
    assert "get_count" in fn_names


def test_introspect_triggers(pg_conn):
    pg_conn.execute(
        "CREATE TABLE dt_introspect_test.orders "
        "(id SERIAL PRIMARY KEY, total NUMERIC)"
    )
    pg_conn.execute("""
        CREATE OR REPLACE FUNCTION dt_introspect_test.audit_fn()
        RETURNS TRIGGER LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$
    """)
    pg_conn.execute("""
        CREATE TRIGGER audit_orders
        AFTER INSERT ON dt_introspect_test.orders
        FOR EACH ROW EXECUTE FUNCTION dt_introspect_test.audit_fn()
    """)
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    trigger_names = {t.trigger_name for t in snap.triggers}
    assert "audit_orders" in trigger_names


def test_introspect_excludes_tables(pg_conn):
    pg_conn.execute("CREATE TABLE dt_introspect_test.products (id SERIAL PRIMARY KEY)")
    pg_conn.execute("CREATE TABLE dt_introspect_test.orders (id SERIAL PRIMARY KEY)")
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test",
        schema_filter="dt_introspect_test",
        exclude_tables=frozenset({"products"}),
    )
    table_names = {t.table_name for t in snap.tables}
    assert "products" not in table_names
    assert "orders" in table_names


def test_stored_procedures_empty_when_none_exist(pg_conn):
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    assert snap.stored_procedures == ()


def test_triggers_empty_when_none_exist(pg_conn):
    snap = PostgresNativeIntrospector().introspect(
        pg_conn, "pg_test", schema_filter="dt_introspect_test"
    )
    assert snap.triggers == ()
