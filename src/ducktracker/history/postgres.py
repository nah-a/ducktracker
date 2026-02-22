"""Postgres-native history manager — two-part FQN, Postgres types."""
from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from ducktracker.history import HistoryManagerBase
from ducktracker.models import AppliedMigration, SchemaSnapshot
from ducktracker.sql_utils import quote_ident

_HISTORY_TABLE_DDL = """
CREATE TABLE {schema}.{table} (
    installed_rank    INTEGER NOT NULL PRIMARY KEY,
    version           INTEGER,
    description       TEXT NOT NULL,
    migration_type    TEXT NOT NULL,
    script            TEXT NOT NULL,
    checksum          TEXT NOT NULL,
    installed_by      TEXT NOT NULL DEFAULT 'ducktracker',
    installed_on      TIMESTAMPTZ NOT NULL,
    execution_time_ms INTEGER NOT NULL,
    success           BOOLEAN NOT NULL,
    snapshot_json     TEXT
)
"""


class PostgresNativeHistoryManager(HistoryManagerBase):

    def _table_exists(
        self,
        conn: duckdb.DuckDBPyConnection,
        schema: str,
        table: str,
    ) -> bool:
        try:
            row = conn.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = ? AND table_name = ?",
                [schema, table],
            ).fetchone()
            return (row[0] if row else 0) > 0
        except duckdb.Error:
            return False

    def ensure_history_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        if not self._table_exists(conn, schema, table):
            conn.execute(_HISTORY_TABLE_DDL.format(schema=quote_ident(schema), table=quote_ident(table)))

    def get_applied_migrations(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> list[AppliedMigration]:
        if not self._table_exists(conn, schema, table):
            return []
        try:
            rows = conn.execute(
                f"SELECT installed_rank, version, description, migration_type, "
                f"script, checksum, installed_by, installed_on, execution_time_ms, success "
                f"FROM {quote_ident(schema)}.{quote_ident(table)} ORDER BY installed_rank"
            ).fetchall()
        except duckdb.Error:
            return []
        return [
            AppliedMigration(
                installed_rank=row[0], version=row[1], description=row[2],
                migration_type=row[3], script=row[4], checksum=row[5],
                installed_by=row[6], installed_on=row[7],
                execution_time_ms=row[8], success=row[9],
            )
            for row in rows
        ]

    def record_migration(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
        version: int | None,
        description: str,
        migration_type: str,
        script: str,
        checksum: str,
        execution_time_ms: int,
        success: bool,
        snapshot_json: str | None = None,
    ) -> None:
        row = conn.execute(
            f"SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {quote_ident(schema)}.{quote_ident(table)}"
        ).fetchone()
        rank = row[0]
        now = datetime.now(tz=UTC)
        conn.execute(
            f"INSERT INTO {quote_ident(schema)}.{quote_ident(table)} "
            f"(installed_rank, version, description, migration_type, script, "
            f"checksum, installed_on, execution_time_ms, success, snapshot_json) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [rank, version, description, migration_type, script, checksum,
             now, execution_time_ms, success, snapshot_json],
        )

    def get_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> SchemaSnapshot | None:
        if not self._table_exists(conn, schema, table):
            return None
        try:
            row = conn.execute(
                f"SELECT snapshot_json FROM {quote_ident(schema)}.{quote_ident(table)} "
                f"WHERE snapshot_json IS NOT NULL AND success = true "
                f"ORDER BY installed_rank DESC LIMIT 1"
            ).fetchone()
        except duckdb.Error:
            return None
        if row is None or row[0] is None:
            return None
        return SchemaSnapshot.from_json(row[0])

    def record_baseline(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
        version: int,
        description: str,
        snapshot_json: str | None = None,
    ) -> None:
        self.record_migration(
            conn=conn, catalog=catalog, schema=schema, table=table,
            version=version, description=description, migration_type="V",
            script=f"<< Baseline V{version} >>", checksum="baseline",
            execution_time_ms=0, success=True, snapshot_json=snapshot_json,
        )

    def update_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
        snapshot_json: str,
    ) -> None:
        conn.execute(
            f"UPDATE {quote_ident(schema)}.{quote_ident(table)} SET snapshot_json = ? "
            f"WHERE installed_rank = (SELECT MAX(installed_rank) FROM {quote_ident(schema)}.{quote_ident(table)})",
            [snapshot_json],
        )
