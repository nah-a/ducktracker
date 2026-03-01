"""Tests for the drift CLI command."""

from __future__ import annotations

from ducktracker.cli import cli
from ducktracker.config import load_config
from ducktracker.connection import connect


def test_drift_no_drift_after_migrate(runner, ducklake_cfg_file):
    # apply_migrations captures a schema snapshot on the last migration
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "drift"])
    assert result.exit_code == 0
    assert "No schema drift" in result.output


def test_drift_no_snapshot_exits_1(runner, ducklake_cfg_file):
    """drift exits 1 with a helpful message when no snapshot exists yet."""
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "drift"])
    assert result.exit_code != 0
    assert "snapshot" in result.output.lower()


def test_drift_shows_modified_item_detail(runner, ducklake_cfg_file):
    """drift prints snapshot:/live: lines for modified schema objects."""
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])

    cfg = load_config(config_path=ducklake_cfg_file)
    with connect(cfg) as conn:
        # Replace the view with a narrower definition to trigger a "modified" drift item
        conn.execute("CREATE OR REPLACE VIEW main.active_users AS SELECT id FROM main.users")

    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "drift"])
    assert result.exit_code != 0
    assert "snapshot:" in result.output
    assert "live:" in result.output


def test_drift_with_live_drift_exits_1_and_prints_report(runner, ducklake_cfg_file):
    """drift exits 1 and prints a full report when the live schema has changed."""
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])

    cfg = load_config(config_path=ducklake_cfg_file)
    with connect(cfg) as conn:
        conn.execute("CREATE TABLE main.out_of_band_drift_table (id INTEGER)")

    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "drift"])
    assert result.exit_code != 0
    assert "out_of_band_drift_table" in result.output
    assert "Schema Drift Report" in result.output
