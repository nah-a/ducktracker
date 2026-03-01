"""Factory functions to select the right backend implementations from config."""
from __future__ import annotations

from ducktracker.config import DuckTrackerConfig
from ducktracker.history import HistoryManagerBase
from ducktracker.history.ducklake import DuckLakeHistoryManager
from ducktracker.introspection import IntrospectorBase
from ducktracker.introspection.ducklake import DuckLakeIntrospector

_INTROSPECTORS: dict[str, type[IntrospectorBase]] = {
    "duckdb": DuckLakeIntrospector,
    "postgres": DuckLakeIntrospector,
}

_HISTORY_MANAGERS: dict[str, type[HistoryManagerBase]] = {
    "duckdb": DuckLakeHistoryManager,
    "postgres": DuckLakeHistoryManager,
}


def get_introspector(config: DuckTrackerConfig) -> IntrospectorBase:
    """Return the appropriate IntrospectorBase implementation for this config."""
    return _INTROSPECTORS[config.catalog_backend]()


def get_history_manager(config: DuckTrackerConfig) -> HistoryManagerBase:
    """Return the appropriate HistoryManagerBase implementation for this config."""
    return _HISTORY_MANAGERS[config.catalog_backend]()
