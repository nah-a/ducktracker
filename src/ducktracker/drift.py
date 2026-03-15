"""Schema drift detection — compare expected snapshot vs live introspection."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime

from ducktracker.models import (
    DriftItem,
    DriftReport,
    IndexInfo,
    MacroInfo,
    SchemaSnapshot,
    SequenceInfo,
    TableInfo,
    ViewInfo,
)


def detect_drift(
    expected: SchemaSnapshot,
    actual: SchemaSnapshot,
    catalog_name: str,
    snapshot_version: int | None = None,
) -> DriftReport:
    """Compare expected schema state against actual live introspection."""
    items: list[DriftItem] = []
    items.extend(_compare_schemas(expected.schemas, actual.schemas))
    items.extend(_compare_tables(expected.tables, actual.tables))
    items.extend(_compare_views(expected.views, actual.views))
    items.extend(_compare_indexes(expected.indexes, actual.indexes))
    items.extend(_compare_sequences(expected.sequences, actual.sequences))
    items.extend(_compare_macros(expected.macros, actual.macros))

    return DriftReport(
        catalog_name=catalog_name,
        checked_at=datetime.now(tz=UTC),
        snapshot_version=snapshot_version,
        items=items,
    )


def _compare_entities[T](
    entity_type: str,
    expected: tuple[T, ...],
    actual: tuple[T, ...],
    key_fn: Callable[[T], tuple[str, ...]],
    detail_fn: Callable[[str, T, T], list[DriftItem]] | None = None,
) -> list[DriftItem]:
    """Generic set-diff comparison for named schema entities.

    Detects added/removed entities and optionally delegates to detail_fn
    for modified-entity comparison on the intersection.
    """
    items: list[DriftItem] = []
    exp_map = {key_fn(e): e for e in expected}
    act_map = {key_fn(a): a for a in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = ".".join(key)
        items.append(
            DriftItem(
                entity_type,
                fqn,
                "added",
                None,
                fqn,
                f"{entity_type.title()} {fqn} exists in live but not in snapshot",
            )
        )

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = ".".join(key)
        items.append(
            DriftItem(
                entity_type,
                fqn,
                "removed",
                fqn,
                None,
                f"{entity_type.title()} {fqn} exists in snapshot but not in live",
            )
        )

    if detail_fn:
        for key in sorted(set(exp_map) & set(act_map)):
            fqn = ".".join(key)
            items.extend(detail_fn(fqn, exp_map[key], act_map[key]))

    return items


def _compare_schemas(
    expected: tuple[str, ...],
    actual: tuple[str, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_set, act_set = set(expected), set(actual)

    for name in sorted(act_set - exp_set):
        items.append(
            DriftItem("schema", name, "added", None, name, f"Schema {name} exists in live but not in snapshot")
        )

    for name in sorted(exp_set - act_set):
        items.append(
            DriftItem("schema", name, "removed", name, None, f"Schema {name} exists in snapshot but not in live")
        )

    return items


def _table_key(t: TableInfo) -> tuple[str, ...]:
    return (t.schema_name, t.table_name)


def _compare_tables(
    expected: tuple[TableInfo, ...],
    actual: tuple[TableInfo, ...],
) -> list[DriftItem]:
    return _compare_entities("table", expected, actual, _table_key, _compare_columns)


def _compare_columns(table_fqn: str, expected: TableInfo, actual: TableInfo) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_cols = {c.name: c for c in expected.columns}
    act_cols = {c.name: c for c in actual.columns}

    for name in sorted(set(act_cols) - set(exp_cols)):
        col = act_cols[name]
        items.append(
            DriftItem(
                "column",
                f"{table_fqn}.{name}",
                "added",
                None,
                col.data_type,
                f"Column {name} ({col.data_type}) added to {table_fqn}",
            )
        )

    for name in sorted(set(exp_cols) - set(act_cols)):
        col = exp_cols[name]
        items.append(
            DriftItem(
                "column",
                f"{table_fqn}.{name}",
                "removed",
                col.data_type,
                None,
                f"Column {name} ({col.data_type}) removed from {table_fqn}",
            )
        )

    for name in sorted(set(exp_cols) & set(act_cols)):
        ec, ac = exp_cols[name], act_cols[name]
        diffs: list[str] = []
        if ec.data_type != ac.data_type:
            diffs.append(f"type: {ec.data_type} -> {ac.data_type}")
        if ec.is_nullable != ac.is_nullable:
            diffs.append(f"nullable: {ec.is_nullable} -> {ac.is_nullable}")
        if ec.column_default != ac.column_default:
            diffs.append(f"default: {ec.column_default} -> {ac.column_default}")
        if ec.ordinal_position != ac.ordinal_position:
            diffs.append(f"position: {ec.ordinal_position} -> {ac.ordinal_position}")

        if diffs:
            detail = f"Column {table_fqn}.{name} changed: {'; '.join(diffs)}"
            items.append(DriftItem("column", f"{table_fqn}.{name}", "modified", str(ec), str(ac), detail))

    return items


def _view_key(v: ViewInfo) -> tuple[str, ...]:
    return (v.schema_name, v.view_name)


def _compare_view_details(fqn: str, exp: ViewInfo, act: ViewInfo) -> list[DriftItem]:
    exp_def = _normalize_whitespace(exp.sql_definition)
    act_def = _normalize_whitespace(act.sql_definition)
    if exp_def != act_def:
        return [
            DriftItem(
                "view",
                fqn,
                "modified",
                exp.sql_definition,
                act.sql_definition,
                f"View {fqn} definition changed",
            )
        ]
    return []


def _compare_views(
    expected: tuple[ViewInfo, ...],
    actual: tuple[ViewInfo, ...],
) -> list[DriftItem]:
    return _compare_entities("view", expected, actual, _view_key, _compare_view_details)


def _index_key(i: IndexInfo) -> tuple[str, ...]:
    return (i.schema_name, i.index_name)


def _compare_indexes(
    expected: tuple[IndexInfo, ...],
    actual: tuple[IndexInfo, ...],
) -> list[DriftItem]:
    return _compare_entities("index", expected, actual, _index_key)


def _sequence_key(s: SequenceInfo) -> tuple[str, ...]:
    return (s.schema_name, s.sequence_name)


def _compare_sequence_details(fqn: str, exp: SequenceInfo, act: SequenceInfo) -> list[DriftItem]:
    diffs: list[str] = []
    if exp.start_value != act.start_value:
        diffs.append(f"start: {exp.start_value} -> {act.start_value}")
    if exp.increment_by != act.increment_by:
        diffs.append(f"increment: {exp.increment_by} -> {act.increment_by}")
    if diffs:
        detail = f"Sequence {fqn}: {'; '.join(diffs)}"
        return [DriftItem("sequence", fqn, "modified", str(exp), str(act), detail)]
    return []


def _compare_sequences(
    expected: tuple[SequenceInfo, ...],
    actual: tuple[SequenceInfo, ...],
) -> list[DriftItem]:
    return _compare_entities("sequence", expected, actual, _sequence_key, _compare_sequence_details)


def _macro_key(m: MacroInfo) -> tuple[str, ...]:
    return (m.schema_name, m.macro_name)


def _compare_macro_details(fqn: str, exp: MacroInfo, act: MacroInfo) -> list[DriftItem]:
    if _normalize_whitespace(exp.definition) != _normalize_whitespace(act.definition):
        return [
            DriftItem(
                "macro",
                fqn,
                "modified",
                exp.definition,
                act.definition,
                f"Macro {fqn} definition changed",
            )
        ]
    return []


def _compare_macros(
    expected: tuple[MacroInfo, ...],
    actual: tuple[MacroInfo, ...],
) -> list[DriftItem]:
    return _compare_entities("macro", expected, actual, _macro_key, _compare_macro_details)


def _normalize_whitespace(s: str) -> str:
    """Collapse whitespace for comparison (avoids false positives from formatting)."""
    return " ".join(s.split())
