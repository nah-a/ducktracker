"""Tests for sequence drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift
from ducktracker.models import SequenceInfo

from .conftest import make_sequence, make_snapshot


def test_sequence_drift():
    s1 = SequenceInfo(
        schema_name="main",
        sequence_name="seq",
        start_value=1,
        increment_by=1,
        min_value=1,
        max_value=100,
    )
    s2 = SequenceInfo(
        schema_name="main",
        sequence_name="seq",
        start_value=10,
        increment_by=1,
        min_value=1,
        max_value=100,
    )
    expected = make_snapshot(sequences=(s1,))
    actual = make_snapshot(sequences=(s2,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert "start" in report.items[0].detail


def test_added_sequence():
    expected = make_snapshot()
    actual = make_snapshot(sequences=(make_sequence("main", "seq_new"),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "sequence"
    assert report.items[0].drift_type == "added"


def test_removed_sequence():
    expected = make_snapshot(sequences=(make_sequence("main", "seq_old"),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "sequence"
    assert report.items[0].drift_type == "removed"


def test_sequence_increment_changed():
    expected = make_snapshot(sequences=(make_sequence("main", "seq", start=1, increment=1),))
    actual = make_snapshot(sequences=(make_sequence("main", "seq", start=1, increment=5),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert "increment" in report.items[0].detail
