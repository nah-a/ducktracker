"""Tests for stored procedure drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift

from .conftest import make_proc, make_snapshot


def test_stored_procedures_added():
    expected = make_snapshot()
    actual = make_snapshot(stored_procedures=(make_proc(),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "stored_procedure"
    assert report.items[0].drift_type == "added"


def test_stored_procedures_removed():
    expected = make_snapshot(stored_procedures=(make_proc(),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "stored_procedure"
    assert report.items[0].drift_type == "removed"


def test_stored_procedures_modified():
    p1 = make_proc(definition="BEGIN RETURN 1; END")
    p2 = make_proc(definition="BEGIN RETURN 2; END")
    expected = make_snapshot(stored_procedures=(p1,))
    actual = make_snapshot(stored_procedures=(p2,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "stored_procedure"
    assert report.items[0].drift_type == "modified"
