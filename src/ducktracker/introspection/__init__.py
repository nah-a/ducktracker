"""Introspection ABC for ducktracker backends."""

from __future__ import annotations

from abc import ABC, abstractmethod

import duckdb

from ducktracker.models import SchemaSnapshot


class IntrospectorBase(ABC):
    """Extract a complete SchemaSnapshot from the live catalog."""

    @abstractmethod
    def introspect(
        self,
        conn: duckdb.DuckDBPyConnection,
        catalog: str,
        exclude_tables: frozenset[str] | None = None,
    ) -> SchemaSnapshot: ...
