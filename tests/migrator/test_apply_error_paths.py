"""Tests for error paths in apply_migrations — rollback failures, record failures."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import duckdb

from ducktracker.config import DuckTrackerConfig
from ducktracker.migrator import apply_migrations
from ducktracker.resolver import discover


def test_record_failure_recording_error_is_warned(setup_conn, cfg, introspector, caplog, tmp_path):
    """When recording a failure itself fails, it's logged as a warning."""
    d = tmp_path / "migs"
    d.mkdir()
    (d / "V1__bad.sql").write_text("INVALID SQL;")
    fail_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )

    bad_mgr = MagicMock()
    bad_mgr.ensure_history_table.return_value = None
    bad_mgr.record_migration.side_effect = duckdb.Error("record failed")

    with caplog.at_level(logging.WARNING, logger="ducktracker.migrator"):
        results = apply_migrations(
            setup_conn,
            fail_cfg,
            discover(d),
            introspector=introspector,
            history=bad_mgr,
        )

    assert results[0][1] is False
    assert "Failed to record failure" in caplog.text


def test_commit_path_failure_rolls_back(setup_conn, cfg, introspector, caplog, tmp_path):
    """When record_migration raises on the success path, the migration is marked failed."""
    d = tmp_path / "migs"
    d.mkdir()
    (d / "V1__ok.sql").write_text("CREATE TABLE main.commit_test (id INTEGER);")
    ok_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )

    bad_mgr = MagicMock()
    bad_mgr.ensure_history_table.return_value = None
    bad_mgr.record_migration.side_effect = duckdb.Error("commit path failed")

    with caplog.at_level(logging.ERROR, logger="ducktracker.migrator"):
        results = apply_migrations(
            setup_conn,
            ok_cfg,
            discover(d),
            introspector=introspector,
            history=bad_mgr,
        )

    assert results[0][1] is False
    assert "Failed to record migration" in caplog.text


def test_rollback_failure_on_bad_sql_is_logged(caplog, tmp_path):
    """When ROLLBACK itself fails after a migration error, it's logged."""
    d = tmp_path / "migs"
    d.mkdir()
    (d / "V1__bad.sql").write_text("INVALID SQL;")
    fail_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )

    # Use a MagicMock connection that simulates execute behavior
    mock_conn = MagicMock()
    call_log: list[str] = []

    def mock_execute(sql, *args, **kwargs):
        call_log.append(sql)
        if sql == "ROLLBACK":
            raise duckdb.Error("rollback failed")
        if sql.startswith("INVALID"):
            raise duckdb.Error("syntax error")
        return MagicMock()

    mock_conn.execute = mock_execute

    bad_mgr = MagicMock()
    bad_mgr.ensure_history_table.return_value = None
    bad_mgr.record_migration.return_value = None

    bad_introspector = MagicMock()

    with caplog.at_level(logging.ERROR, logger="ducktracker.migrator"):
        results = apply_migrations(
            mock_conn,
            fail_cfg,
            discover(d),
            introspector=bad_introspector,
            history=bad_mgr,
        )

    assert results[0][1] is False
    assert "ROLLBACK also failed" in caplog.text


def test_rollback_failure_on_commit_path_is_logged(caplog, tmp_path):
    """When ROLLBACK fails after a commit-path error, both errors are logged."""
    d = tmp_path / "migs"
    d.mkdir()
    (d / "V1__ok.sql").write_text("SELECT 1;")
    ok_cfg = DuckTrackerConfig(
        catalog_name="memory",
        migrations_dir=str(d),
        target_schema="main",
        schema_history_table="ducktracker_schema_history",
    )

    mock_conn = MagicMock()

    def mock_execute(sql, *args, **kwargs):
        if sql == "ROLLBACK":
            raise duckdb.Error("rollback also failed")
        return MagicMock()

    mock_conn.execute = mock_execute

    bad_mgr = MagicMock()
    bad_mgr.ensure_history_table.return_value = None
    bad_mgr.record_migration.side_effect = duckdb.Error("record failed")

    bad_introspector = MagicMock()

    with caplog.at_level(logging.ERROR, logger="ducktracker.migrator"):
        results = apply_migrations(
            mock_conn,
            ok_cfg,
            discover(d),
            introspector=bad_introspector,
            history=bad_mgr,
        )

    assert results[0][1] is False
    assert "ROLLBACK also failed" in caplog.text
