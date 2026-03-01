"""Tests for the baseline CLI command."""

from __future__ import annotations

from ducktracker.cli import cli


def test_baseline_success(runner, ducklake_cfg_file):
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "baseline", "--version", "1"])
    assert result.exit_code == 0
    assert "V1" in result.output


def test_baseline_fails_after_migrate(runner, ducklake_cfg_file):
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "baseline", "--version", "1"])
    assert result.exit_code != 0
    assert "Cannot baseline" in result.output
