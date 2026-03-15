"""Tests for table and column drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift
from ducktracker.models import ColumnInfo, TableInfo

from .conftest import make_snapshot, make_table


def test_added_table():
    expected = make_snapshot()
    actual = make_snapshot(tables=(make_table("main", "new_table"),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert len(report.items) == 1
    assert report.items[0].drift_type == "added"
    assert report.items[0].object_type == "table"


def test_removed_table():
    expected = make_snapshot(tables=(make_table("main", "old_table"),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].drift_type == "removed"


def test_added_column():
    t_expected = make_table("main", "users", [("id", "INTEGER", False)])
    t_actual = make_table("main", "users", [("id", "INTEGER", False), ("email", "VARCHAR", True)])
    expected = make_snapshot(tables=(t_expected,))
    actual = make_snapshot(tables=(t_actual,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert any(i.drift_type == "added" and i.object_type == "column" for i in report.items)


def test_removed_column():
    t_expected = make_table("main", "users", [("id", "INTEGER", False), ("name", "VARCHAR", False)])
    t_actual = make_table("main", "users", [("id", "INTEGER", False)])
    expected = make_snapshot(tables=(t_expected,))
    actual = make_snapshot(tables=(t_actual,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert any(i.drift_type == "removed" and "name" in i.object_name for i in report.items)


def test_column_type_change():
    t_expected = make_table("main", "users", [("id", "INTEGER", False)])
    t_actual = make_table("main", "users", [("id", "BIGINT", False)])
    expected = make_snapshot(tables=(t_expected,))
    actual = make_snapshot(tables=(t_actual,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].drift_type == "modified"
    assert "type" in report.items[0].detail


def test_column_nullable_change():
    t_expected = make_table("main", "users", [("id", "INTEGER", False)])
    t_actual = make_table("main", "users", [("id", "INTEGER", True)])
    expected = make_snapshot(tables=(t_expected,))
    actual = make_snapshot(tables=(t_actual,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert "nullable" in report.items[0].detail


def test_column_default_changed():
    col_no_default = ColumnInfo(
        name="id",
        data_type="INTEGER",
        is_nullable=False,
        column_default=None,
        ordinal_position=1,
    )
    col_with_default = ColumnInfo(
        name="id",
        data_type="INTEGER",
        is_nullable=False,
        column_default="0",
        ordinal_position=1,
    )
    t_exp = TableInfo(
        schema_name="main",
        table_name="t",
        columns=(col_no_default,),
        primary_key=None,
        unique_constraints=(),
    )
    t_act = TableInfo(
        schema_name="main",
        table_name="t",
        columns=(col_with_default,),
        primary_key=None,
        unique_constraints=(),
    )
    report = detect_drift(make_snapshot(tables=(t_exp,)), make_snapshot(tables=(t_act,)), "test")
    assert report.has_drift
    assert any("default" in i.detail for i in report.items)


def test_composite_primary_key_table_drift():
    """Drift detection works for tables with multi-column primary keys."""
    t_exp = TableInfo(
        schema_name="main",
        table_name="order_items",
        columns=(
            ColumnInfo(
                name="order_id",
                data_type="INTEGER",
                is_nullable=False,
                column_default=None,
                ordinal_position=1,
            ),
            ColumnInfo(
                name="item_id",
                data_type="INTEGER",
                is_nullable=False,
                column_default=None,
                ordinal_position=2,
            ),
        ),
        primary_key=("order_id", "item_id"),
        unique_constraints=(),
    )
    t_act = TableInfo(
        schema_name="main",
        table_name="order_items",
        columns=(
            ColumnInfo(
                name="order_id",
                data_type="INTEGER",
                is_nullable=False,
                column_default=None,
                ordinal_position=1,
            ),
            ColumnInfo(
                name="item_id",
                data_type="INTEGER",
                is_nullable=False,
                column_default=None,
                ordinal_position=2,
            ),
            ColumnInfo(name="qty", data_type="INTEGER", is_nullable=True, column_default=None, ordinal_position=3),
        ),
        primary_key=("order_id", "item_id"),
        unique_constraints=(),
    )
    report = detect_drift(make_snapshot(tables=(t_exp,)), make_snapshot(tables=(t_act,)), "test")
    assert report.has_drift
    assert any(i.drift_type == "added" and "qty" in i.object_name for i in report.items)


def test_special_character_table_names():
    """Drift detection handles table/schema names with special characters."""
    t1 = make_table("my schema", "my table")
    expected = make_snapshot(schemas=("my schema",), tables=(t1,))
    actual = make_snapshot(schemas=("my schema",))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_name == "my schema.my table"
    assert report.items[0].drift_type == "removed"


def test_column_ordinal_position_changed():
    col_pos1 = ColumnInfo(
        name="id",
        data_type="INTEGER",
        is_nullable=False,
        column_default=None,
        ordinal_position=1,
    )
    col_pos2 = ColumnInfo(
        name="id",
        data_type="INTEGER",
        is_nullable=False,
        column_default=None,
        ordinal_position=2,
    )
    t_exp = TableInfo(
        schema_name="main",
        table_name="t",
        columns=(col_pos1,),
        primary_key=None,
        unique_constraints=(),
    )
    t_act = TableInfo(
        schema_name="main",
        table_name="t",
        columns=(col_pos2,),
        primary_key=None,
        unique_constraints=(),
    )
    report = detect_drift(make_snapshot(tables=(t_exp,)), make_snapshot(tables=(t_act,)), "test")
    assert report.has_drift
    assert any("position" in i.detail for i in report.items)
