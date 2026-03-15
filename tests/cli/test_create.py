"""Tests for the create CLI command."""

from __future__ import annotations

from pathlib import Path

from ducktracker.cli import cli


def test_create_command(runner, tmp_path: Path):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    # Write a config file pointing to the tmp migrations dir
    config_file = tmp_path / "ducktracker.toml"
    config_file.write_text(f'[migrations]\ndirectory = "{migrations_dir}"\n')
    result = runner.invoke(cli, ["-c", str(config_file), "create", "add users table"])
    assert result.exit_code == 0
    assert "Created" in result.output
    files = list(migrations_dir.glob("V1__*.sql"))
    assert len(files) == 1


def test_create_repeatable(runner, tmp_path: Path):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config_file = tmp_path / "ducktracker.toml"
    config_file.write_text(f'[migrations]\ndirectory = "{migrations_dir}"\n')
    result = runner.invoke(cli, ["-c", str(config_file), "create", "--repeatable", "refresh views"])
    assert result.exit_code == 0
    files = list(migrations_dir.glob("R__*.sql"))
    assert len(files) == 1
