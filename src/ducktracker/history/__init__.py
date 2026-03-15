"""History manager ABC for ducktracker backends."""

from __future__ import annotations

from abc import ABC, abstractmethod

import duckdb

from ducktracker.models import AppliedMigration, SchemaSnapshot


class HistoryManagerBase(ABC):
    """Manages the ducktracker_schema_history table."""

    @abstractmethod
    def ensure_history_table(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> None:
        """Create the schema history table if it does not already exist."""
        ...

    @abstractmethod
    def get_applied_migrations(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> list[AppliedMigration]:
        """Return all recorded migrations ordered by installed_rank."""
        ...

    @abstractmethod
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
        """Insert a migration record (success or failure) into the history table."""
        ...

    @abstractmethod
    def get_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
    ) -> SchemaSnapshot | None:
        """Return the most recent successful schema snapshot, or None if none exists."""
        ...

    @abstractmethod
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
        """Record a baseline entry marking an existing schema version as applied."""
        ...

    @abstractmethod
    def update_latest_snapshot(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        schema: str,
        table: str,
        snapshot_json: str,
    ) -> None:
        """Attach a schema snapshot to the most recent history record."""
        ...
