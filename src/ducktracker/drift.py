"""Schema drift detection — compare expected snapshot vs live introspection."""

from __future__ import annotations

from datetime import UTC, datetime

from ducktracker.models import (
    DriftItem,
    DriftReport,
    IndexInfo,
    MacroInfo,
    SchemaSnapshot,
    SequenceInfo,
    StoredProcedureInfo,
    TableInfo,
    TriggerInfo,
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
    items.extend(_compare_stored_procedures(expected.stored_procedures, actual.stored_procedures))
    items.extend(_compare_triggers(expected.triggers, actual.triggers))

    return DriftReport(
        catalog_name=catalog_name,
        checked_at=datetime.now(tz=UTC),
        snapshot_version=snapshot_version,
        items=items,
    )


def _compare_schemas(
    expected: tuple[str, ...],
    actual: tuple[str, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_set, act_set = set(expected), set(actual)

    for name in sorted(act_set - exp_set):
        items.append(DriftItem(
            "schema", name, "added", None, name, f"Schema {name} exists in live but not in snapshot"
        ))

    for name in sorted(exp_set - act_set):
        items.append(
            DriftItem("schema", name, "removed", name, None, f"Schema {name} exists in snapshot but not in live")
        )

    return items


def _compare_tables(
    expected: tuple[TableInfo, ...],
    actual: tuple[TableInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(t.schema_name, t.table_name): t for t in expected}
    act_map = {(t.schema_name, t.table_name): t for t in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("table", fqn, "added", None, fqn, f"Table {fqn} exists in live but not in snapshot"))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("table", fqn, "removed", fqn, None, f"Table {fqn} exists in snapshot but not in live"))

    for key in sorted(set(exp_map) & set(act_map)):
        items.extend(_compare_columns(f"{key[0]}.{key[1]}", exp_map[key], act_map[key]))

    return items


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
            items.append(
                DriftItem("column", f"{table_fqn}.{name}", "modified", str(ec), str(ac), detail)
            )

    return items


def _compare_views(
    expected: tuple[ViewInfo, ...],
    actual: tuple[ViewInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(v.schema_name, v.view_name): v for v in expected}
    act_map = {(v.schema_name, v.view_name): v for v in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("view", fqn, "added", None, fqn, f"View {fqn} exists in live but not in snapshot"))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("view", fqn, "removed", fqn, None, f"View {fqn} exists in snapshot but not in live"))

    for key in sorted(set(exp_map) & set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        exp_def = _normalize_whitespace(exp_map[key].sql_definition)
        act_def = _normalize_whitespace(act_map[key].sql_definition)
        if exp_def != act_def:
            items.append(
                DriftItem(
                    "view",
                    fqn,
                    "modified",
                    exp_map[key].sql_definition,
                    act_map[key].sql_definition,
                    f"View {fqn} definition changed",
                )
            )

    return items


def _compare_indexes(
    expected: tuple[IndexInfo, ...],
    actual: tuple[IndexInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(i.schema_name, i.index_name): i for i in expected}
    act_map = {(i.schema_name, i.index_name): i for i in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("index", fqn, "added", None, fqn, f"Index {fqn} exists in live but not in snapshot"))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("index", fqn, "removed", fqn, None, f"Index {fqn} exists in snapshot but not in live"))

    return items


def _compare_sequences(
    expected: tuple[SequenceInfo, ...],
    actual: tuple[SequenceInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(s.schema_name, s.sequence_name): s for s in expected}
    act_map = {(s.schema_name, s.sequence_name): s for s in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(
            DriftItem("sequence", fqn, "added", None, fqn, f"Sequence {fqn} exists in live but not in snapshot")
        )

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(
            DriftItem("sequence", fqn, "removed", fqn, None, f"Sequence {fqn} exists in snapshot but not in live")
        )

    for key in sorted(set(exp_map) & set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        es, as_ = exp_map[key], act_map[key]
        diffs: list[str] = []
        if es.start_value != as_.start_value:
            diffs.append(f"start: {es.start_value} -> {as_.start_value}")
        if es.increment_by != as_.increment_by:
            diffs.append(f"increment: {es.increment_by} -> {as_.increment_by}")
        if diffs:
            detail = f"Sequence {fqn}: {'; '.join(diffs)}"
            items.append(DriftItem("sequence", fqn, "modified", str(es), str(as_), detail))

    return items


def _compare_macros(
    expected: tuple[MacroInfo, ...],
    actual: tuple[MacroInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(m.schema_name, m.macro_name): m for m in expected}
    act_map = {(m.schema_name, m.macro_name): m for m in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("macro", fqn, "added", None, fqn, f"Macro {fqn} exists in live but not in snapshot"))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem("macro", fqn, "removed", fqn, None, f"Macro {fqn} exists in snapshot but not in live"))

    for key in sorted(set(exp_map) & set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        em, am = exp_map[key], act_map[key]
        if _normalize_whitespace(em.definition) != _normalize_whitespace(am.definition):
            items.append(
                DriftItem("macro", fqn, "modified", em.definition, am.definition, f"Macro {fqn} definition changed")
            )

    return items


def _compare_stored_procedures(
    expected: tuple[StoredProcedureInfo, ...],
    actual: tuple[StoredProcedureInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(p.schema_name, p.name): p for p in expected}
    act_map = {(p.schema_name, p.name): p for p in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem(
            "stored_procedure", fqn, "added", None, fqn,
            f"Stored procedure {fqn} exists in live but not in snapshot",
        ))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        items.append(DriftItem(
            "stored_procedure", fqn, "removed", fqn, None,
            f"Stored procedure {fqn} exists in snapshot but not in live",
        ))

    for key in sorted(set(exp_map) & set(act_map)):
        fqn = f"{key[0]}.{key[1]}"
        ep, ap = exp_map[key], act_map[key]
        if _normalize_whitespace(ep.definition) != _normalize_whitespace(ap.definition):
            items.append(DriftItem(
                "stored_procedure", fqn, "modified", ep.definition, ap.definition,
                f"Stored procedure {fqn} definition changed",
            )
            )

    return items


def _compare_triggers(
    expected: tuple[TriggerInfo, ...],
    actual: tuple[TriggerInfo, ...],
) -> list[DriftItem]:
    items: list[DriftItem] = []
    exp_map = {(t.schema_name, t.table_name, t.trigger_name): t for t in expected}
    act_map = {(t.schema_name, t.table_name, t.trigger_name): t for t in actual}

    for key in sorted(set(act_map) - set(exp_map)):
        fqn = f"{key[0]}.{key[2]}"
        items.append(DriftItem(
            "trigger", fqn, "added", None, fqn,
            f"Trigger {fqn} on {key[1]} exists in live but not in snapshot",
        ))

    for key in sorted(set(exp_map) - set(act_map)):
        fqn = f"{key[0]}.{key[2]}"
        items.append(DriftItem(
            "trigger", fqn, "removed", fqn, None,
            f"Trigger {fqn} on {key[1]} exists in snapshot but not in live",
        ))

    return items


def _normalize_whitespace(s: str) -> str:
    """Collapse whitespace for comparison (avoids false positives from formatting)."""
    return " ".join(s.split())
