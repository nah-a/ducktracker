"""DuckLake history manager — three-part FQN, DuckDB types."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime

import duckdb

from ducktracker.history import HistoryManagerBase
from ducktracker.models import AppliedMigration, SchemaSnapshot
from ducktracker.sql_utils import quote_ident

logger = logging.getLogger(__name__)

_HISTORY_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS {fqn} (
    installed_rank    INTEGER NOT NULL,
    version           INTEGER,
    description       VARCHAR NOT NULL,
    migration_type    VARCHAR NOT NULL,
    script            VARCHAR NOT NULL,
    checksum          VARCHAR NOT NULL,
    installed_by      VARCHAR NOT NULL DEFAULT 'ducktracker',
    installed_on      TIMESTAMP NOT NULL,
    execution_time_ms INTEGER NOT NULL,
    success           BOOLEAN NOT NULL,
    snapshot_json     VARCHAR,
    snapshot_data     BLOB
)
"""


class DuckLakeHistoryManager(HistoryManagerBase):
    def _fqn(self, catalog: str, schema: str, table: str) -> str:
        """Build a three-part fully-qualified name with safe quoting."""
        return f"{quote_ident(catalog)}.{quote_ident(schema)}.{quote_ident(table)}"

    def ensure_history_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        fqn = self._fqn(catalog, schema, table)
        conn.execute(_HISTORY_TABLE_DDL.format(fqn=fqn))
        # Add snapshot_data BLOB column to tables created before this column existed.
        try:
            conn.execute(f"ALTER TABLE {fqn} ADD COLUMN snapshot_data BLOB")
        except duckdb.Error:
            pass  # Column already exists

    def get_applied_migrations(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> list[AppliedMigration]:
        fqn = self._fqn(catalog, schema, table)
        try:
            rows = conn.execute(
                f"SELECT installed_rank, version, description, migration_type, "
                f"script, checksum, installed_by, installed_on, execution_time_ms, success "
                f"FROM {fqn} ORDER BY installed_rank"
            ).fetchall()
        except duckdb.CatalogException:
            return []
        return [
            AppliedMigration(
                installed_rank=row[0],
                version=row[1],
                description=row[2],
                migration_type=row[3],
                script=row[4],
                checksum=row[5],
                installed_by=row[6],
                installed_on=row[7],
                execution_time_ms=row[8],
                success=row[9],
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
        fqn = self._fqn(catalog, schema, table)
        rank = conn.execute(f"SELECT COALESCE(MAX(installed_rank), 0) + 1 FROM {fqn}").fetchone()[0]
        now = datetime.now(tz=UTC).replace(tzinfo=None)
        snapshot_blob = snapshot_json.encode() if snapshot_json else None
        conn.execute(
            f"INSERT INTO {fqn} "
            f"(installed_rank, version, description, migration_type, script, "
            f"checksum, installed_on, execution_time_ms, success, snapshot_data) "
            f"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                rank,
                version,
                description,
                migration_type,
                script,
                checksum,
                now,
                execution_time_ms,
                success,
                snapshot_blob,
            ],
        )

    def get_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> SchemaSnapshot | None:
        fqn = self._fqn(catalog, schema, table)
        # Try BLOB column first (reliable across DuckDB versions), fall back to VARCHAR.
        snapshot_str = self._read_snapshot_blob(conn, fqn)
        if snapshot_str is None:
            snapshot_str = self._read_snapshot_varchar(conn, fqn)
        if snapshot_str is None:
            return None
        try:
            return SchemaSnapshot.from_json(snapshot_str)
        except (json.JSONDecodeError, ValueError) as exc:
            logger.warning(
                "Corrupt snapshot in history table (len=%d): %s",
                len(snapshot_str),
                exc,
            )
            return None

    def _read_snapshot_blob(self, conn: duckdb.DuckDBPyConnection, fqn: str) -> str | None:
        """Read snapshot from the BLOB column."""
        try:
            row = conn.execute(
                f"SELECT snapshot_data FROM {fqn} "
                f"WHERE snapshot_data IS NOT NULL AND success "
                f"ORDER BY installed_rank DESC LIMIT 1"
            ).fetchone()
        except duckdb.Error:
            return None
        if row is None or row[0] is None:
            return None
        raw = row[0]
        if isinstance(raw, (bytes, bytearray, memoryview)):
            return bytes(raw).decode()
        return str(raw) if raw else None

    def _read_snapshot_varchar(self, conn: duckdb.DuckDBPyConnection, fqn: str) -> str | None:
        """Read snapshot from the legacy VARCHAR column (backward compat)."""
        try:
            row = conn.execute(
                f"SELECT snapshot_json FROM {fqn} "
                f"WHERE snapshot_json IS NOT NULL AND snapshot_json != '' AND success "
                f"ORDER BY installed_rank DESC LIMIT 1"
            ).fetchone()
        except duckdb.Error:
            return None
        if row is None or row[0] is None or row[0] == "":
            return None
        return row[0]

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
            conn=conn,
            catalog=catalog,
            schema=schema,
            table=table,
            version=version,
            description=description,
            migration_type="V",
            script=f"<< Baseline V{version} >>",
            checksum="baseline",
            execution_time_ms=0,
            success=True,
            snapshot_json=snapshot_json,
        )

    def update_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
        snapshot_json: str,
    ) -> None:
        fqn = self._fqn(catalog, schema, table)
        conn.execute(
            f"UPDATE {fqn} SET snapshot_data = ? WHERE installed_rank = (SELECT MAX(installed_rank) FROM {fqn})",
            [snapshot_json.encode()],
        )
