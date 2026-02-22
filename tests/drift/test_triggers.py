"""Tests for trigger drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift

from .conftest import make_snapshot, make_trigger


def test_triggers_added():
    expected = make_snapshot()
    actual = make_snapshot(triggers=(make_trigger(),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "trigger"
    assert report.items[0].drift_type == "added"


def test_triggers_removed():
    expected = make_snapshot(triggers=(make_trigger(),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "trigger"
    assert report.items[0].drift_type == "removed"
