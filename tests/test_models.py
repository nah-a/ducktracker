"""Tests for schema snapshot serialization."""

from __future__ import annotations

import json
from datetime import datetime

from ducktracker.models import (
    ColumnInfo,
    IndexInfo,
    MacroInfo,
    SchemaSnapshot,
    SequenceInfo,
    TableInfo,
    ViewInfo,
)


def _make_snapshot(**overrides) -> SchemaSnapshot:
    """Build a minimal SchemaSnapshot with sensible defaults."""
    defaults = dict(
        catalog_name="test_lake",
        captured_at=datetime(2025, 6, 15, 12, 0, 0),
        schemas=("main",),
        tables=(),
        views=(),
        indexes=(),
        sequences=(),
        macros=(),
    )
    defaults.update(overrides)
    return SchemaSnapshot(**defaults)


# --- Round-trip tests ---


def test_to_dict_and_from_dict_round_trip():
    snap = _make_snapshot()
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.catalog_name == snap.catalog_name
    assert restored.captured_at == snap.captured_at
    assert restored.schemas == snap.schemas


def test_to_json_and_from_json_round_trip():
    snap = _make_snapshot()
    restored = SchemaSnapshot.from_json(snap.to_json())
    assert restored.catalog_name == snap.catalog_name
    assert restored.captured_at == snap.captured_at


def test_to_json_produces_valid_json():
    snap = _make_snapshot()
    parsed = json.loads(snap.to_json())
    assert parsed["catalog_name"] == "test_lake"
    assert parsed["captured_at"] == "2025-06-15T12:00:00"


# --- Empty collections ---


def test_round_trip_empty_collections():
    snap = _make_snapshot(
        schemas=(),
        tables=(),
        views=(),
        indexes=(),
        sequences=(),
        macros=(),
    )
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.schemas == ()
    assert restored.tables == ()
    assert restored.views == ()
    assert restored.indexes == ()
    assert restored.sequences == ()
    assert restored.macros == ()


# --- Table with columns, primary key, unique constraints ---


def test_round_trip_table_with_columns():
    col = ColumnInfo(
        name="id",
        data_type="INTEGER",
        is_nullable=False,
        column_default=None,
        ordinal_position=1,
    )
    table = TableInfo(
        schema_name="main",
        table_name="users",
        columns=(col,),
        primary_key=("id",),
        unique_constraints=(("id",),),
    )
    snap = _make_snapshot(tables=(table,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert len(restored.tables) == 1
    t = restored.tables[0]
    assert t.table_name == "users"
    assert t.columns[0].name == "id"
    assert t.columns[0].column_default is None
    assert t.primary_key == ("id",)
    assert t.unique_constraints == (("id",),)


def test_round_trip_table_with_none_primary_key():
    table = TableInfo(
        schema_name="main",
        table_name="events",
        columns=(),
        primary_key=None,
        unique_constraints=(),
    )
    snap = _make_snapshot(tables=(table,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.tables[0].primary_key is None


def test_round_trip_table_with_empty_primary_key_tuple():
    """An empty tuple primary_key should round-trip as None (falsy)."""
    table = TableInfo(
        schema_name="main",
        table_name="logs",
        columns=(),
        primary_key=(),
        unique_constraints=(),
    )
    snap = _make_snapshot(tables=(table,))
    d = snap.to_dict()
    # Empty tuple serializes as empty list, which is falsy -> from_dict returns None
    restored = SchemaSnapshot.from_dict(d)
    assert restored.tables[0].primary_key is None


# --- Views ---


def test_round_trip_view():
    view = ViewInfo(schema_name="main", view_name="active_users", sql_definition="SELECT * FROM users WHERE active")
    snap = _make_snapshot(views=(view,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.views[0].view_name == "active_users"
    assert restored.views[0].sql_definition == "SELECT * FROM users WHERE active"


# --- Indexes ---


def test_round_trip_index():
    idx = IndexInfo(
        schema_name="main",
        table_name="users",
        index_name="idx_users_email",
        is_unique=True,
        sql_definition="CREATE UNIQUE INDEX idx_users_email ON users(email)",
    )
    snap = _make_snapshot(indexes=(idx,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.indexes[0].index_name == "idx_users_email"
    assert restored.indexes[0].is_unique is True


# --- Sequences ---


def test_round_trip_sequence():
    seq = SequenceInfo(
        schema_name="main",
        sequence_name="user_id_seq",
        start_value=1,
        increment_by=1,
        min_value=1,
        max_value=9999,
    )
    snap = _make_snapshot(sequences=(seq,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.sequences[0].sequence_name == "user_id_seq"
    assert restored.sequences[0].max_value == 9999


# --- Macros ---


def test_round_trip_macro():
    macro = MacroInfo(
        schema_name="main",
        macro_name="double",
        macro_type="scalar",
        parameters="x",
        definition="x * 2",
    )
    snap = _make_snapshot(macros=(macro,))
    restored = SchemaSnapshot.from_dict(snap.to_dict())
    assert restored.macros[0].macro_name == "double"
    assert restored.macros[0].macro_type == "scalar"


# --- Full snapshot with all object types ---


def test_round_trip_full_snapshot():
    col = ColumnInfo(name="id", data_type="INTEGER", is_nullable=False, column_default=None, ordinal_position=1)
    snap = _make_snapshot(
        schemas=("main", "staging"),
        tables=(
            TableInfo(
                schema_name="main",
                table_name="t1",
                columns=(col,),
                primary_key=("id",),
                unique_constraints=(),
            ),
        ),
        views=(ViewInfo(schema_name="main", view_name="v1", sql_definition="SELECT 1"),),
        indexes=(
            IndexInfo(
                schema_name="main",
                table_name="t1",
                index_name="idx1",
                is_unique=False,
                sql_definition="CREATE INDEX idx1 ON t1(id)",
            ),
        ),
        sequences=(
            SequenceInfo(
                schema_name="main",
                sequence_name="seq1",
                start_value=1,
                increment_by=1,
                min_value=1,
                max_value=999,
            ),
        ),
        macros=(
            MacroInfo(schema_name="main", macro_name="m1", macro_type="table", parameters="", definition="SELECT 1"),
        ),
    )
    json_str = snap.to_json()
    restored = SchemaSnapshot.from_json(json_str)
    assert restored.schemas == ("main", "staging")
    assert len(restored.tables) == 1
    assert len(restored.views) == 1
    assert len(restored.indexes) == 1
    assert len(restored.sequences) == 1
    assert len(restored.macros) == 1


# --- to_dict structure ---


def test_to_dict_captured_at_is_isoformat():
    snap = _make_snapshot(captured_at=datetime(2025, 1, 2, 3, 4, 5))
    d = snap.to_dict()
    assert d["captured_at"] == "2025-01-02T03:04:05"
    assert isinstance(d["captured_at"], str)
