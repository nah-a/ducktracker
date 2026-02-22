"""Tests for basic drift detection (snapshots, schemas)."""

from __future__ import annotations

from ducktracker.drift import detect_drift

from .conftest import make_snapshot, make_table


def test_identical_snapshots_no_drift():
    s = make_snapshot(tables=(make_table("main", "users"),))
    report = detect_drift(s, s, "test")
    assert not report.has_drift


def test_added_schema():
    expected = make_snapshot(schemas=("main",))
    actual = make_snapshot(schemas=("main", "staging"))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "schema"
    assert report.items[0].drift_type == "added"


def test_removed_schema():
    expected = make_snapshot(schemas=("main", "staging"))
    actual = make_snapshot(schemas=("main",))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].drift_type == "removed"
