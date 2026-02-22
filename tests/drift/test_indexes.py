"""Tests for index drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift

from .conftest import make_index, make_snapshot


def test_added_index():
    expected = make_snapshot()
    actual = make_snapshot(indexes=(make_index("main", "users", "idx_new"),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "index"
    assert report.items[0].drift_type == "added"


def test_removed_index():
    expected = make_snapshot(indexes=(make_index("main", "users", "idx_old"),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "index"
    assert report.items[0].drift_type == "removed"
