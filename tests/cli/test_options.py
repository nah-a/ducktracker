"""Tests for global CLI options (version, help, backend, secrets-dir, etc.)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

from ducktracker.cli import cli
from ducktracker.config import DuckTrackerConfig

from .conftest import cli_override_fixture


def test_version(runner):
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert "0.1.0" in result.output


def test_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "ducktracker" in result.output


def test_secrets_dir_appears_in_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "--secrets-dir" in result.output


def test_secrets_dir_passed_to_load_config(runner, tmp_path: Path):
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config_file = tmp_path / "ducktracker.toml"
    config_file.write_text(f'[migrations]\ndirectory = "{migrations_dir}"\n')

    with patch(
        "ducktracker.cli.load_config",
        return_value=DuckTrackerConfig(migrations_dir=str(migrations_dir)),
    ) as mock_load:
        result = runner.invoke(
            cli,
            ["-c", str(config_file), "--secrets-dir", "/my/secrets", "create", "test migration"],
        )

    assert result.exit_code == 0
    _, kwargs = mock_load.call_args
    assert kwargs["overrides"].get("secret_directory") == "/my/secrets"


def test_catalog_option_sets_override(runner, tmp_path: Path):
    migrations_dir, config_file = cli_override_fixture(tmp_path)
    with patch("ducktracker.cli.load_config",
               return_value=DuckTrackerConfig(migrations_dir=str(migrations_dir))) as mock_load:
        runner.invoke(cli, ["-c", str(config_file), "--catalog", "my_cat", "create", "t"])
    assert mock_load.call_args[1]["overrides"].get("catalog_name") == "my_cat"


def test_backend_option_sets_override(runner, tmp_path: Path):
    migrations_dir, config_file = cli_override_fixture(tmp_path)
    with patch("ducktracker.cli.load_config",
               return_value=DuckTrackerConfig(migrations_dir=str(migrations_dir))) as mock_load:
        runner.invoke(cli, ["-c", str(config_file), "--backend", "duckdb", "create", "t"])
    assert mock_load.call_args[1]["overrides"].get("catalog_backend") == "duckdb"


def test_metadata_option_sets_override(runner, tmp_path: Path):
    migrations_dir, config_file = cli_override_fixture(tmp_path)
    with patch("ducktracker.cli.load_config",
               return_value=DuckTrackerConfig(migrations_dir=str(migrations_dir))) as mock_load:
        runner.invoke(cli, ["-c", str(config_file), "--metadata", "/tmp/meta.db", "create", "t"])
    assert mock_load.call_args[1]["overrides"].get("duckdb_metadata_path") == "/tmp/meta.db"


def test_connection_option_sets_override(runner, tmp_path: Path):
    migrations_dir, config_file = cli_override_fixture(tmp_path)
    with patch("ducktracker.cli.load_config",
               return_value=DuckTrackerConfig(migrations_dir=str(migrations_dir))) as mock_load:
        runner.invoke(cli, ["-c", str(config_file), "--connection", "dbname=foo", "create", "t"])
    assert mock_load.call_args[1]["overrides"].get("postgres_connection") == "dbname=foo"
