"""Tests for model extensions supporting Postgres-native schema objects."""
from __future__ import annotations

from datetime import datetime

from ducktracker.models import (
    SchemaSnapshot,
    StoredProcedureInfo,
    TriggerInfo,
)


def test_stored_procedure_info_is_frozen():
    proc = StoredProcedureInfo(
        schema_name="public",
        name="get_user",
        kind="function",
        language="plpgsql",
        argument_types="integer",
        return_type="text",
        definition="BEGIN RETURN 'hello'; END;",
    )
    assert proc.schema_name == "public"
    assert proc.kind == "function"


def test_trigger_info_is_frozen():
    trig = TriggerInfo(
        schema_name="public",
        table_name="users",
        trigger_name="audit_users",
        timing="AFTER",
        events="INSERT OR UPDATE",
        orientation="ROW",
        function_name="audit_trigger_fn",
    )
    assert trig.timing == "AFTER"
    assert trig.events == "INSERT OR UPDATE"


def test_schema_snapshot_new_fields_default_empty():
    snapshot = SchemaSnapshot(
        catalog_name="test",
        captured_at=datetime(2026, 2, 21),
        schemas=("public",),
        tables=(),
        views=(),
        indexes=(),
        sequences=(),
        macros=(),
    )
    assert snapshot.stored_procedures == ()
    assert snapshot.triggers == ()
    assert snapshot.columnar_tables == ()


def test_schema_snapshot_round_trips_new_fields():
    proc = StoredProcedureInfo(
        schema_name="public",
        name="my_fn",
        kind="function",
        language="sql",
        argument_types="",
        return_type="integer",
        definition="SELECT 1",
    )
    trig = TriggerInfo(
        schema_name="public",
        table_name="orders",
        trigger_name="trg_orders",
        timing="BEFORE",
        events="INSERT",
        orientation="ROW",
        function_name="trg_fn",
    )
    snapshot = SchemaSnapshot(
        catalog_name="test",
        captured_at=datetime(2026, 2, 21),
        schemas=("public",),
        tables=(),
        views=(),
        indexes=(),
        sequences=(),
        macros=(),
        stored_procedures=(proc,),
        triggers=(trig,),
        columnar_tables=("public.events",),
    )
    restored = SchemaSnapshot.from_json(snapshot.to_json())
    assert len(restored.stored_procedures) == 1
    assert restored.stored_procedures[0].name == "my_fn"
    assert len(restored.triggers) == 1
    assert restored.triggers[0].trigger_name == "trg_orders"
    assert restored.columnar_tables == ("public.events",)


def test_schema_snapshot_deserializes_old_json_without_new_fields():
    """Old snapshots stored without stored_procedures/triggers/columnar_tables must still load."""
    old_dict = {
        "catalog_name": "legacy",
        "captured_at": "2026-01-01T00:00:00",
        "schemas": ["main"],
        "tables": [],
        "views": [],
        "indexes": [],
        "sequences": [],
        "macros": [],
    }
    snapshot = SchemaSnapshot.from_dict(old_dict)
    assert snapshot.stored_procedures == ()
    assert snapshot.triggers == ()
    assert snapshot.columnar_tables == ()
