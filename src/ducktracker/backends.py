"""Factory functions to select the right backend implementations from config."""
from __future__ import annotations

from ducktracker.config import DuckTrackerConfig
from ducktracker.history import HistoryManagerBase
from ducktracker.history.ducklake import DuckLakeHistoryManager
from ducktracker.history.postgres import PostgresNativeHistoryManager
from ducktracker.introspection import IntrospectorBase
from ducktracker.introspection.ducklake import DuckLakeIntrospector
from ducktracker.introspection.postgres import PostgresNativeIntrospector

_INTROSPECTORS: dict[str, type[IntrospectorBase]] = {
    "duckdb": DuckLakeIntrospector,
    "postgres": DuckLakeIntrospector,
    "pg_duckdb": PostgresNativeIntrospector,
}

_HISTORY_MANAGERS: dict[str, type[HistoryManagerBase]] = {
    "duckdb": DuckLakeHistoryManager,
    "postgres": DuckLakeHistoryManager,
    "pg_duckdb": PostgresNativeHistoryManager,
}


def get_introspector(config: DuckTrackerConfig) -> IntrospectorBase:
    """Return the appropriate IntrospectorBase implementation for this config."""
    return _INTROSPECTORS[config.catalog_backend]()


def get_history_manager(config: DuckTrackerConfig) -> HistoryManagerBase:
    """Return the appropriate HistoryManagerBase implementation for this config."""
    return _HISTORY_MANAGERS[config.catalog_backend]()
