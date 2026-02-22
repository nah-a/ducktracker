"""Postgres-native catalog introspection via pg_catalog and information_schema."""
from __future__ import annotations

import logging
from datetime import UTC, datetime

import duckdb

from ducktracker.introspection import IntrospectorBase
from ducktracker.models import (
    ColumnInfo,
    IndexInfo,
    SchemaSnapshot,
    SequenceInfo,
    StoredProcedureInfo,
    TableInfo,
    TriggerInfo,
    ViewInfo,
)

logger = logging.getLogger(__name__)

_SYSTEM_SCHEMAS = frozenset({"information_schema", "pg_catalog", "pg_toast"})


class PostgresNativeIntrospector(IntrospectorBase):
    """Introspects a Postgres database attached via DuckDB's postgres extension."""

    def introspect(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        exclude_tables: frozenset[str] | None = None,
        schema_filter: str | None = None,
        **kwargs,
    ) -> SchemaSnapshot:
        exclude = exclude_tables or frozenset()
        pg_db = self._current_database(conn)
        return SchemaSnapshot(
            catalog_name=catalog,
            captured_at=datetime.now(tz=UTC),
            schemas=self._get_schemas(conn, pg_db, schema_filter),
            tables=self._get_tables(conn, pg_db, exclude, schema_filter),
            views=self._get_views(conn, pg_db, schema_filter),
            indexes=self._get_indexes(conn, schema_filter),
            sequences=self._get_sequences(conn, pg_db, schema_filter),
            macros=(),
            stored_procedures=self._get_stored_procedures(conn, schema_filter),
            triggers=self._get_triggers(conn, schema_filter),
            columnar_tables=self._get_columnar_tables(conn, schema_filter),
        )

    def _current_database(self, conn: duckdb.DuckDBPyConnection) -> str:
        try:
            row = conn.execute("SELECT current_database()").fetchone()
            return row[0] if row else ""
        except duckdb.Error:
            return ""

    def _get_schemas(
        self, conn: duckdb.DuckDBPyConnection, pg_db: str, schema_filter: str | None
    ) -> tuple[str, ...]:
        try:
            if schema_filter:
                rows = conn.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE catalog_name = ? AND schema_name = ?",
                    [pg_db, schema_filter],
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE catalog_name = ?",
                    [pg_db],
                ).fetchall()
        except duckdb.Error:
            logger.warning("Could not query schemata")
            return ()
        return tuple(r[0] for r in rows if r[0] not in _SYSTEM_SCHEMAS)

    def _get_tables(
        self,
        conn: duckdb.DuckDBPyConnection,
        pg_db: str,
        exclude_tables: frozenset[str],
        schema_filter: str | None,
    ) -> tuple[TableInfo, ...]:
        try:
            params: list = [pg_db]
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND t.table_schema = ? "
                params.append(schema_filter)
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
                + schema_clause
                + "ORDER BY t.table_schema, t.table_name, c.ordinal_position",
                params,
            ).fetchall()
        except duckdb.Error:
            logger.warning("Could not query tables")
            return ()

        table_columns: dict[tuple[str, str], list[ColumnInfo]] = {}
        for row in rows:
            schema_name, table_name = row[0], row[1]
            if table_name in exclude_tables or schema_name in _SYSTEM_SCHEMAS:
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
            pk, ucs = self._get_constraints(conn, schema_name, table_name)
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
        schema_name: str,
        table_name: str,
    ) -> tuple[tuple[str, ...] | None, tuple[tuple[str, ...], ...]]:
        pk: tuple[str, ...] | None = None
        ucs: list[tuple[str, ...]] = []
        try:
            rows = conn.execute(
                "SELECT con.contype, "
                "ARRAY(SELECT a.attname FROM pg_catalog.pg_attribute a "
                "      WHERE a.attrelid = con.conrelid "
                "      AND a.attnum = ANY(con.conkey) "
                "      ORDER BY a.attnum) AS col_names "
                "FROM pg_catalog.pg_constraint con "
                "JOIN pg_catalog.pg_class cl ON cl.oid = con.conrelid "
                "JOIN pg_catalog.pg_namespace ns ON ns.oid = cl.relnamespace "
                "WHERE ns.nspname = ? AND cl.relname = ? "
                "AND con.contype IN ('p', 'u')",
                [schema_name, table_name],
            ).fetchall()
            for row in rows:
                col_names = tuple(row[1]) if row[1] else ()
                if row[0] == "p":
                    pk = col_names
                elif row[0] == "u":
                    ucs.append(col_names)
        except duckdb.Error:
            logger.debug("Could not query constraints for %s.%s", schema_name, table_name)
        return pk, tuple(ucs)

    def _get_views(
        self, conn: duckdb.DuckDBPyConnection, pg_db: str, schema_filter: str | None
    ) -> tuple[ViewInfo, ...]:
        try:
            params: list = [pg_db]
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND table_schema = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT table_schema, table_name, view_definition "
                "FROM information_schema.views "
                "WHERE table_catalog = ? " + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.warning("Could not query views")
            return ()
        return tuple(
            ViewInfo(schema_name=r[0], view_name=r[1], sql_definition=r[2] or "")
            for r in rows
            if r[0] not in _SYSTEM_SCHEMAS
        )

    def _get_indexes(
        self, conn: duckdb.DuckDBPyConnection, schema_filter: str | None
    ) -> tuple[IndexInfo, ...]:
        try:
            params: list = []
            schema_clause = ""
            if schema_filter:
                schema_clause = "WHERE schemaname = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT schemaname, tablename, indexname, indexdef "
                "FROM pg_catalog.pg_indexes " + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.debug("Could not query indexes")
            return ()
        return tuple(
            IndexInfo(
                schema_name=r[0],
                table_name=r[1],
                index_name=r[2],
                is_unique="UNIQUE" in (r[3] or "").upper(),
                sql_definition=r[3] or "",
            )
            for r in rows
        )

    def _get_sequences(
        self, conn: duckdb.DuckDBPyConnection, pg_db: str, schema_filter: str | None
    ) -> tuple[SequenceInfo, ...]:
        try:
            params: list = [pg_db]
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND sequence_schema = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT sequence_schema, sequence_name, "
                "start_value, increment, minimum_value, maximum_value "
                "FROM information_schema.sequences "
                "WHERE sequence_catalog = ? " + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.debug("Could not query sequences")
            return ()
        return tuple(
            SequenceInfo(
                schema_name=r[0],
                sequence_name=r[1],
                start_value=int(r[2]),
                increment_by=int(r[3]),
                min_value=int(r[4]),
                max_value=int(r[5]),
            )
            for r in rows
        )

    def _get_stored_procedures(
        self, conn: duckdb.DuckDBPyConnection, schema_filter: str | None
    ) -> tuple[StoredProcedureInfo, ...]:
        try:
            params: list = []
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND ns.nspname = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT ns.nspname, p.proname, "
                "CASE p.prokind WHEN 'f' THEN 'function' WHEN 'p' THEN 'procedure' ELSE 'function' END, "
                "l.lanname, "
                "pg_catalog.pg_get_function_arguments(p.oid), "
                "CASE p.prokind WHEN 'p' THEN '' ELSE pg_catalog.pg_get_function_result(p.oid) END, "
                "pg_catalog.pg_get_functiondef(p.oid) "
                "FROM pg_catalog.pg_proc p "
                "JOIN pg_catalog.pg_namespace ns ON ns.oid = p.pronamespace "
                "JOIN pg_catalog.pg_language l ON l.oid = p.prolang "
                "WHERE ns.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.debug("Could not query stored procedures")
            return ()
        return tuple(
            StoredProcedureInfo(
                schema_name=r[0],
                name=r[1],
                kind=r[2],
                language=r[3],
                argument_types=r[4] or "",
                return_type=r[5] or "",
                definition=r[6] or "",
            )
            for r in rows
        )

    def _get_triggers(
        self, conn: duckdb.DuckDBPyConnection, schema_filter: str | None
    ) -> tuple[TriggerInfo, ...]:
        try:
            params: list = []
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND ns.nspname = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT ns.nspname, cl.relname, t.tgname, "
                "CASE t.tgtype & 2 WHEN 2 THEN 'BEFORE' ELSE "
                "  CASE t.tgtype & 64 WHEN 64 THEN 'INSTEAD OF' ELSE 'AFTER' END "
                "END, "
                "ARRAY_TO_STRING(ARRAY["
                "  CASE t.tgtype & 4 WHEN 4 THEN 'INSERT' ELSE NULL END,"
                "  CASE t.tgtype & 8 WHEN 8 THEN 'DELETE' ELSE NULL END,"
                "  CASE t.tgtype & 16 WHEN 16 THEN 'UPDATE' ELSE NULL END"
                "], ' OR '), "
                "CASE t.tgtype & 1 WHEN 1 THEN 'ROW' ELSE 'STATEMENT' END, "
                "pfn.proname "
                "FROM pg_catalog.pg_trigger t "
                "JOIN pg_catalog.pg_class cl ON cl.oid = t.tgrelid "
                "JOIN pg_catalog.pg_namespace ns ON ns.oid = cl.relnamespace "
                "JOIN pg_catalog.pg_proc pfn ON pfn.oid = t.tgfoid "
                "WHERE NOT t.tgisinternal "
                + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.debug("Could not query triggers")
            return ()
        return tuple(
            TriggerInfo(
                schema_name=r[0],
                table_name=r[1],
                trigger_name=r[2],
                timing=r[3],
                events=r[4] or "",
                orientation=r[5],
                function_name=r[6],
            )
            for r in rows
        )

    def _get_columnar_tables(
        self, conn: duckdb.DuckDBPyConnection, schema_filter: str | None
    ) -> tuple[str, ...]:
        """Return FQN of tables using the pg_duckdb columnar access method."""
        try:
            params: list = []
            schema_clause = ""
            if schema_filter:
                schema_clause = "AND ns.nspname = ? "
                params.append(schema_filter)
            rows = conn.execute(
                "SELECT ns.nspname, cl.relname "
                "FROM pg_catalog.pg_class cl "
                "JOIN pg_catalog.pg_namespace ns ON ns.oid = cl.relnamespace "
                "JOIN pg_catalog.pg_am am ON am.oid = cl.relam "
                "WHERE am.amname = 'duckdb' "
                "AND cl.relkind = 'r' "
                + schema_clause,
                params,
            ).fetchall()
        except duckdb.Error:
            logger.debug("Could not query columnar tables")
            return ()
        return tuple(f"{r[0]}.{r[1]}" for r in rows)
