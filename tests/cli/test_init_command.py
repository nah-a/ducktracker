"""Tests for the init CLI command."""

from __future__ import annotations

from pathlib import Path

from ducktracker.cli import cli


def test_init_backend_flag_creates_files(runner, tmp_path: Path):
    result = runner.invoke(cli, ["init", "--backend", "ducklake-duckdb", str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / "ducktracker.toml").exists()
    assert (tmp_path / "migrations").is_dir()
    assert (tmp_path / ".gitignore").exists()
    assert (tmp_path / "README.md").exists()


def test_init_shows_created_files_in_output(runner, tmp_path: Path):
    result = runner.invoke(cli, ["init", "--backend", "ducklake-duckdb", str(tmp_path)])
    assert result.exit_code == 0
    assert "ducktracker.toml" in result.output
    assert "migrations" in result.output


def test_init_default_path_is_cwd(runner, tmp_path: Path):
    with runner.isolated_filesystem(temp_dir=tmp_path):
        result = runner.invoke(cli, ["init", "--backend", "ducklake-duckdb"])
    assert result.exit_code == 0


def test_init_nonexistent_path_exits_nonzero(runner, tmp_path: Path):
    result = runner.invoke(cli, ["init", "--backend", "ducklake-duckdb", str(tmp_path / "nope")])
    assert result.exit_code != 0
    assert "does not exist" in result.output.lower() or "nope" in result.output


def test_init_conflict_exits_nonzero_and_lists_conflicts(runner, tmp_path: Path):
    (tmp_path / "ducktracker.toml").write_text("[connection]\n")
    result = runner.invoke(cli, ["init", "--backend", "ducklake-duckdb", str(tmp_path)])
    assert result.exit_code != 0
    # Rich may line-wrap long paths, so check without newlines
    assert "ducktracker.toml" in result.output.replace("\n", "")


def test_init_prompts_for_backend_when_not_specified(runner, tmp_path: Path):
    """When --backend is omitted, init interactively prompts for the backend type."""
    result = runner.invoke(cli, ["init", str(tmp_path)], input="ducklake-duckdb\n")
    assert result.exit_code == 0
    assert (tmp_path / "ducktracker.toml").exists()


def test_init_appears_in_help(runner):
    result = runner.invoke(cli, ["--help"])
    assert result.exit_code == 0
    assert "init" in result.output
