"""Drift test helpers and factory functions."""

from __future__ import annotations

from datetime import UTC, datetime

from ducktracker.models import (
    ColumnInfo,
    IndexInfo,
    MacroInfo,
    SchemaSnapshot,
    SequenceInfo,
    StoredProcedureInfo,
    TableInfo,
    TriggerInfo,
)


def make_snapshot(**kwargs) -> SchemaSnapshot:
    defaults = dict(
        catalog_name="test",
        captured_at=datetime(2026, 2, 14, tzinfo=UTC),
        schemas=("main",),
        tables=(),
        views=(),
        indexes=(),
        sequences=(),
        macros=(),
    )
    defaults.update(kwargs)
    return SchemaSnapshot(**defaults)


def make_table(schema: str, name: str, columns: list[tuple] | None = None) -> TableInfo:
    cols = tuple(
        ColumnInfo(
            name=c[0], data_type=c[1], is_nullable=c[2],
            column_default=None, ordinal_position=i + 1,
        )
        for i, c in enumerate(columns or [("id", "INTEGER", False)])
    )
    return TableInfo(
        schema_name=schema, table_name=name, columns=cols,
        primary_key=None, unique_constraints=(),
    )


def make_index(schema: str, table: str, name: str) -> IndexInfo:
    return IndexInfo(
        schema_name=schema, table_name=table, index_name=name,
        is_unique=False, sql_definition="",
    )


def make_sequence(schema: str, name: str, start: int = 1, increment: int = 1) -> SequenceInfo:
    return SequenceInfo(
        schema_name=schema, sequence_name=name, start_value=start,
        increment_by=increment, min_value=1, max_value=9_999_999,
    )


def make_macro(schema: str, name: str, definition: str = "x + 1") -> MacroInfo:
    return MacroInfo(
        schema_name=schema, macro_name=name, macro_type="scalar",
        parameters="(x)", definition=definition,
    )


def make_proc(
    schema: str = "main", name: str = "my_func",
    definition: str = "BEGIN RETURN 1; END",
) -> StoredProcedureInfo:
    return StoredProcedureInfo(
        schema_name=schema, name=name, kind="function",
        language="plpgsql", argument_types="",
        return_type="integer", definition=definition,
    )


def make_trigger(
    schema: str = "main", table: str = "users",
    name: str = "audit_trigger",
) -> TriggerInfo:
    return TriggerInfo(
        schema_name=schema, table_name=table, trigger_name=name,
        timing="AFTER", events="INSERT OR UPDATE",
        orientation="ROW", function_name="audit_fn",
    )
