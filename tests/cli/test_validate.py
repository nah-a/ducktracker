"""Tests for the validate CLI command."""

from __future__ import annotations

from ducktracker.cli import cli


def test_validate_passes_after_migrate(runner, ducklake_cfg_file):
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "validate"])
    assert result.exit_code == 0
    assert "All checksums match" in result.output


def test_validate_fails_on_tamper(runner, ducklake_cfg_file, migrations_dir):
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    # Overwrite a migration file to simulate tampering after it was applied
    (migrations_dir / "V1__create_users.sql").write_text("-- tampered content")
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "validate"])
    assert result.exit_code != 0
    assert "MISMATCH" in result.output
