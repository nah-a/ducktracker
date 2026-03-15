"""Tests for apply_migrations."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from ducktracker.config import DuckTrackerConfig
from ducktracker.migrator import apply_migrations, get_pending_migrations
from ducktracker.resolver import discover


def test_apply_migrations_success(setup_conn, cfg, introspector, mgr):
    discovered = discover(cfg.migrations_dir)
    pending = get_pending_migrations([], discovered)
    results = apply_migrations(setup_conn, cfg, [pending[0]], introspector=introspector, history=mgr)
    assert len(results) == 1
    assert results[0][1] is True


def test_apply_migrations_dry_run(setup_conn, cfg, introspector, mgr):
    discovered = discover(cfg.migrations_dir)
    results = apply_migrations(
        setup_conn,
        cfg,
        discovered,
        introspector=introspector,
        history=mgr,
        dry_run=True,
    )
    assert len(results) == 3
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", cfg.schema_history_table)
    assert len(applied) == 0


def test_apply_migrations_rolls_back_on_failure(setup_conn, introspector, mgr, tmp_path):
    """A failing migration is rolled back -- no partial table creation survives."""
    d = tmp_path / "fail_migs"
    d.mkdir()
    (d / "V1__partial.sql").write_text("CREATE TABLE main.rollback_test (id INTEGER);\nTHIS IS NOT VALID SQL;")
    fail_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )
    results = apply_migrations(setup_conn, fail_cfg, discover(d), introspector=introspector, history=mgr)
    assert results[0][1] is False
    rows = setup_conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_name = 'rollback_test'"
    ).fetchall()
    assert len(rows) == 0


def test_apply_migrations_records_failure_in_history(setup_conn, introspector, mgr, tmp_path):
    """A failed migration is recorded in history with success=False."""
    d = tmp_path / "fail_hist"
    d.mkdir()
    (d / "V1__bad.sql").write_text("NOT VALID SQL;")
    fail_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )
    apply_migrations(setup_conn, fail_cfg, discover(d), introspector=introspector, history=mgr)
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    assert len(applied) == 1
    assert applied[0].success is False


def test_apply_migrations_empty_pending(setup_conn, cfg, introspector, mgr):
    """apply_migrations returns [] immediately when there are no pending migrations."""
    results = apply_migrations(setup_conn, cfg, [], introspector=introspector, history=mgr)
    assert results == []


def test_apply_migrations_snapshot_failure_is_warned(setup_conn, cfg, mgr, caplog):
    """When snapshot capture raises, apply_migrations logs a warning but still succeeds."""
    bad_introspector = MagicMock()
    bad_introspector.introspect.side_effect = RuntimeError("introspect blew up")

    discovered = discover(cfg.migrations_dir)
    with caplog.at_level(logging.WARNING, logger="ducktracker.migrator"):
        results = apply_migrations(
            setup_conn,
            cfg,
            [discovered[0]],
            introspector=bad_introspector,
            history=mgr,
        )

    assert results[0][1] is True
    assert "Failed to capture schema snapshot" in caplog.text
