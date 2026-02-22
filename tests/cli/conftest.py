"""CLI-specific test fixtures."""

from __future__ import annotations

from pathlib import Path

import pytest
from click.testing import CliRunner


@pytest.fixture
def runner():
    return CliRunner()


def cli_override_fixture(tmp_path: Path):
    """Returns (migrations_dir, config_file_path) for CLI override tests."""
    migrations_dir = tmp_path / "migrations"
    migrations_dir.mkdir()
    config_file = tmp_path / "ducktracker.toml"
    config_file.write_text(f'[migrations]\ndirectory = "{migrations_dir}"\n')
    return migrations_dir, config_file
