"""Tests for view drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift
from ducktracker.models import ViewInfo

from .conftest import make_snapshot


def test_added_view():
    v = ViewInfo(schema_name="main", view_name="new_view", sql_definition="SELECT 1")
    expected = make_snapshot()
    actual = make_snapshot(views=(v,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "view"
    assert report.items[0].drift_type == "added"


def test_removed_view():
    v = ViewInfo(schema_name="main", view_name="old_view", sql_definition="SELECT 1")
    expected = make_snapshot(views=(v,))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "view"
    assert report.items[0].drift_type == "removed"


def test_view_definition_changed():
    v1 = ViewInfo(schema_name="main", view_name="v", sql_definition="SELECT 1")
    v2 = ViewInfo(schema_name="main", view_name="v", sql_definition="SELECT 2")
    expected = make_snapshot(views=(v1,))
    actual = make_snapshot(views=(v2,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].drift_type == "modified"


def test_view_whitespace_normalization():
    v1 = ViewInfo(schema_name="main", view_name="v", sql_definition="SELECT   1\n  FROM t")
    v2 = ViewInfo(schema_name="main", view_name="v", sql_definition="SELECT 1 FROM t")
    expected = make_snapshot(views=(v1,))
    actual = make_snapshot(views=(v2,))
    report = detect_drift(expected, actual, "test")
    assert not report.has_drift
