"""Tests for the init command initializer module."""

from __future__ import annotations

import tomllib
from pathlib import Path

import pytest

from ducktracker.initializer import ConflictError, init_directory


def test_init_ducklake_duckdb_creates_valid_toml(tmp_path: Path) -> None:
    init_directory(tmp_path, "ducklake-duckdb")
    toml_path = tmp_path / "ducktracker.toml"
    assert toml_path.exists()
    with open(toml_path, "rb") as f:
        data = tomllib.load(f)
    assert data["connection"]["catalog_backend"] == "duckdb"
    assert "catalog_name" in data["connection"]
    assert "duckdb_metadata_path" in data["connection"]
    assert "migrations" in data
    assert "behavior" in data


def test_init_ducklake_postgres_creates_valid_toml(tmp_path: Path) -> None:
    init_directory(tmp_path, "ducklake-postgres")
    with open(tmp_path / "ducktracker.toml", "rb") as f:
        data = tomllib.load(f)
    assert data["connection"]["catalog_backend"] == "postgres"


def test_init_creates_migrations_dir(tmp_path: Path) -> None:
    init_directory(tmp_path, "ducklake-duckdb")
    assert (tmp_path / "migrations").is_dir()


def test_init_creates_gitignore(tmp_path: Path) -> None:
    init_directory(tmp_path, "ducklake-duckdb")
    gitignore = tmp_path / ".gitignore"
    assert gitignore.exists()
    assert "*.db" in gitignore.read_text()


def test_init_creates_readme(tmp_path: Path) -> None:
    init_directory(tmp_path, "ducklake-duckdb")
    readme = tmp_path / "README.md"
    assert readme.exists()
    assert len(readme.read_text()) > 0


def test_init_returns_four_created_paths(tmp_path: Path) -> None:
    created = init_directory(tmp_path, "ducklake-duckdb")
    assert len(created) == 4
    assert tmp_path / "ducktracker.toml" in created
    assert tmp_path / "migrations" in created
    assert tmp_path / ".gitignore" in created
    assert tmp_path / "README.md" in created


def test_init_conflict_toml_exists(tmp_path: Path) -> None:
    (tmp_path / "ducktracker.toml").write_text("[connection]\n")
    with pytest.raises(ConflictError) as exc_info:
        init_directory(tmp_path, "ducklake-duckdb")
    assert tmp_path / "ducktracker.toml" in exc_info.value.conflicts


def test_init_conflict_migrations_dir_exists(tmp_path: Path) -> None:
    (tmp_path / "migrations").mkdir()
    with pytest.raises(ConflictError) as exc_info:
        init_directory(tmp_path, "ducklake-duckdb")
    assert tmp_path / "migrations" in exc_info.value.conflicts


def test_init_no_partial_writes_on_conflict(tmp_path: Path) -> None:
    (tmp_path / "ducktracker.toml").write_text("[connection]\n")
    with pytest.raises(ConflictError):
        init_directory(tmp_path, "ducklake-duckdb")
    assert not (tmp_path / "migrations").exists()
    assert not (tmp_path / ".gitignore").exists()
    assert not (tmp_path / "README.md").exists()


def test_init_nonexistent_path_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        init_directory(tmp_path / "does_not_exist", "ducklake-duckdb")


def test_init_file_path_raises_not_a_directory(tmp_path: Path) -> None:
    """init_directory raises NotADirectoryError when given an existing file, not a dir."""
    file_path = tmp_path / "i_am_a_file.txt"
    file_path.write_text("hello")
    with pytest.raises(NotADirectoryError):
        init_directory(file_path, "ducklake-duckdb")
