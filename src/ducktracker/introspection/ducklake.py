"""DuckLake catalog introspection using DuckDB catalog views."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime

import duckdb

from ducktracker.introspection import IntrospectorBase
from ducktracker.models import (
    ColumnInfo,
    IndexInfo,
    MacroInfo,
    SchemaSnapshot,
    SequenceInfo,
    TableInfo,
    ViewInfo,
)

logger = logging.getLogger(__name__)

_SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog"})


def _safe_query[T](
    conn: duckdb.DuckDBPyConnection,
    sql: str,
    params: list[str],
    transform: Callable[[list[tuple[object, ...]]], T],
    default: T,
    label: str,
) -> T:
    """Execute a catalog query, returning *default* on duckdb.Error."""
    try:
        rows = conn.execute(sql, params).fetchall()
    except duckdb.Error:
        logger.warning("Could not query %s", label)
        return default
    return transform(rows)


class DuckLakeIntrospector(IntrospectorBase):
    """Introspects a DuckLake catalog using duckdb_* catalog views."""

    def introspect(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        exclude_tables: frozenset[str] | None = None,
    ) -> SchemaSnapshot:
        exclude = exclude_tables or frozenset()
        return SchemaSnapshot(
            catalog_name=catalog,
            captured_at=datetime.now(tz=UTC),
            schemas=self._get_schemas(conn, catalog),
            tables=self._get_tables(conn, catalog, exclude),
            views=self._get_views(conn, catalog),
            indexes=self._get_indexes(conn, catalog),
            sequences=self._get_sequences(conn, catalog),
            macros=self._get_macros(conn, catalog),
        )

    def _get_schemas(self, conn: duckdb.DuckDBPyConnection, catalog: str) -> tuple[str, ...]:
        return _safe_query(
            conn,
            "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = ?",
            [catalog],
            lambda rows: tuple(row[0] for row in rows if row[0] not in _SYSTEM_SCHEMAS),
            (),
            f"schemata for catalog {catalog}",
        )

    def _get_tables(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        exclude_tables: frozenset[str],
    ) -> tuple[TableInfo, ...]:
        try:
            rows = conn.execute(
                "SELECT t.table_schema, t.table_name, "
                "c.column_name, c.data_type, c.is_nullable, "
                "c.column_default, c.ordinal_position "
                "FROM information_schema.tables t "
                "JOIN information_schema.columns c "
                "ON t.table_catalog = c.table_catalog "
                "AND t.table_schema = c.table_schema "
                "AND t.table_name = c.table_name "
                "WHERE t.table_catalog = ? "
                "AND t.table_type = 'BASE TABLE' "
                "ORDER BY t.table_schema, t.table_name, c.ordinal_position",
                [catalog],
            ).fetchall()
        except duckdb.Error:
            logger.warning("Could not query tables for catalog %s", catalog)
            return ()

        table_columns: dict[tuple[str, str], list[ColumnInfo]] = {}
        for row in rows:
            schema_name, table_name = row[0], row[1]
            if table_name in exclude_tables:
                continue
            if schema_name in _SYSTEM_SCHEMAS:
                continue
            key = (schema_name, table_name)
            if key not in table_columns:
                table_columns[key] = []
            table_columns[key].append(
                ColumnInfo(
                    name=row[2],
                    data_type=row[3],
                    is_nullable=row[4] == "YES",
                    column_default=row[5],
                    ordinal_position=row[6],
                )
            )

        tables = []
        for (schema_name, table_name), columns in table_columns.items():
            pk, ucs = self._get_constraints(conn, catalog, schema_name, table_name)
            tables.append(
                TableInfo(
                    schema_name=schema_name,
                    table_name=table_name,
                    columns=tuple(columns),
                    primary_key=pk,
                    unique_constraints=ucs,
                )
            )
        return tuple(tables)

    def _get_constraints(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema_name: str,
        table_name: str,
    ) -> tuple[tuple[str, ...] | None, tuple[tuple[str, ...], ...]]:
        pk: tuple[str, ...] | None = None
        ucs: list[tuple[str, ...]] = []
        try:
            rows = conn.execute(
                "SELECT constraint_type, constraint_column_names "
                "FROM duckdb_constraints() "
                "WHERE database_name = ? AND schema_name = ? AND table_name = ?",
                [catalog, schema_name, table_name],
            ).fetchall()
            for row in rows:
                ctype = row[0]
                col_names = tuple(row[1]) if row[1] else ()
                if ctype == "PRIMARY KEY":
                    pk = col_names
                elif ctype == "UNIQUE":
                    ucs.append(col_names)
        except duckdb.Error:
            logger.debug("Could not query constraints for %s.%s.%s", catalog, schema_name, table_name)
        return pk, tuple(ucs)

    def _get_views(self, conn: duckdb.DuckDBPyConnection, catalog: str) -> tuple[ViewInfo, ...]:
        return _safe_query(
            conn,
            "SELECT table_schema, table_name, view_definition FROM information_schema.views WHERE table_catalog = ?",
            [catalog],
            lambda rows: tuple(
                ViewInfo(schema_name=row[0], view_name=row[1], sql_definition=row[2] or "")
                for row in rows
                if row[0] not in _SYSTEM_SCHEMAS
            ),
            (),
            f"views for catalog {catalog}",
        )

    def _get_indexes(self, conn: duckdb.DuckDBPyConnection, catalog: str) -> tuple[IndexInfo, ...]:
        return _safe_query(
            conn,
            "SELECT schema_name, table_name, index_name, is_unique, sql FROM duckdb_indexes() WHERE database_name = ?",
            [catalog],
            lambda rows: tuple(
                IndexInfo(
                    schema_name=row[0],
                    table_name=row[1],
                    index_name=row[2],
                    is_unique=row[3],
                    sql_definition=row[4] or "",
                )
                for row in rows
            ),
            (),
            f"indexes for catalog {catalog}",
        )

    def _get_sequences(self, conn: duckdb.DuckDBPyConnection, catalog: str) -> tuple[SequenceInfo, ...]:
        return _safe_query(
            conn,
            "SELECT schema_name, sequence_name, start_value, increment_by, min_value, max_value "
            "FROM duckdb_sequences() "
            "WHERE database_name = ?",
            [catalog],
            lambda rows: tuple(
                SequenceInfo(
                    schema_name=row[0],
                    sequence_name=row[1],
                    start_value=row[2],
                    increment_by=row[3],
                    min_value=row[4],
                    max_value=row[5],
                )
                for row in rows
            ),
            (),
            f"sequences for catalog {catalog}",
        )

    def _get_macros(self, conn: duckdb.DuckDBPyConnection, catalog: str) -> tuple[MacroInfo, ...]:
        return _safe_query(
            conn,
            "SELECT schema_name, function_name, function_type, parameters, macro_definition "
            "FROM duckdb_functions() "
            "WHERE database_name = ? "
            "AND function_type IN ('macro', 'table_macro')",
            [catalog],
            lambda rows: tuple(
                MacroInfo(
                    schema_name=row[0],
                    macro_name=row[1],
                    macro_type="table" if row[2] == "table_macro" else "scalar",
                    parameters=str(row[3]) if row[3] else "",
                    definition=row[4] or "",
                )
                for row in rows
            ),
            (),
            f"macros for catalog {catalog}",
        )
