"""Tests for macro drift detection."""

from __future__ import annotations

from ducktracker.drift import detect_drift
from ducktracker.models import MacroInfo

from .conftest import make_macro, make_snapshot


def test_macro_drift():
    m1 = MacroInfo(
        schema_name="main",
        macro_name="my_macro",
        macro_type="scalar",
        parameters="(x)",
        definition="x + 1",
    )
    m2 = MacroInfo(
        schema_name="main",
        macro_name="my_macro",
        macro_type="scalar",
        parameters="(x)",
        definition="x + 2",
    )
    expected = make_snapshot(macros=(m1,))
    actual = make_snapshot(macros=(m2,))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "macro"


def test_added_macro():
    expected = make_snapshot()
    actual = make_snapshot(macros=(make_macro("main", "new_macro"),))
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "macro"
    assert report.items[0].drift_type == "added"


def test_removed_macro():
    expected = make_snapshot(macros=(make_macro("main", "old_macro"),))
    actual = make_snapshot()
    report = detect_drift(expected, actual, "test")
    assert report.has_drift
    assert report.items[0].object_type == "macro"
    assert report.items[0].drift_type == "removed"
