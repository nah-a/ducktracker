"""Tests for the info CLI command."""

from __future__ import annotations

from ducktracker.cli import cli


def test_info_shows_pending_before_migrate(runner, ducklake_cfg_file):
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "info"])
    assert result.exit_code == 0
    assert "PENDING" in result.output


def test_info_shows_applied_after_migrate(runner, ducklake_cfg_file):
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "info"])
    assert result.exit_code == 0
    assert "APPLIED" in result.output
