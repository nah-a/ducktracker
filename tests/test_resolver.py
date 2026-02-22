"""Tests for migration file discovery and resolution."""

from __future__ import annotations

from pathlib import Path

import pytest

from ducktracker.models import MigrationType
from ducktracker.resolver import (
    MigrationResolutionError,
    discover,
    next_version,
    scaffold_migration,
)


def test_discover_finds_all_files(migrations_dir: Path):
    files = discover(migrations_dir)
    assert len(files) == 3
    # Versioned first, then repeatable
    assert files[0].version == 1
    assert files[1].version == 2
    assert files[2].version is None
    assert files[2].migration_type == MigrationType.REPEATABLE


def test_discover_sorts_versioned_by_number(migrations_dir: Path):
    (migrations_dir / "V10__later.sql").write_text("SELECT 1;")
    files = discover(migrations_dir)
    versioned = [f for f in files if f.migration_type == MigrationType.VERSIONED]
    assert [f.version for f in versioned] == [1, 2, 10]


def test_discover_empty_dir(empty_migrations_dir: Path):
    files = discover(empty_migrations_dir)
    assert files == []


def test_discover_nonexistent_dir():
    with pytest.raises(MigrationResolutionError, match="does not exist"):
        discover("/nonexistent/path")


def test_discover_rejects_invalid_filenames(tmp_path: Path):
    d = tmp_path / "bad"
    d.mkdir()
    (d / "bad_name.sql").write_text("SELECT 1;")
    with pytest.raises(MigrationResolutionError, match="Invalid migration filename"):
        discover(d)


def test_discover_rejects_duplicate_versions(tmp_path: Path):
    d = tmp_path / "dup"
    d.mkdir()
    (d / "V1__first.sql").write_text("SELECT 1;")
    (d / "V1__second.sql").write_text("SELECT 2;")
    with pytest.raises(MigrationResolutionError, match="Duplicate version"):
        discover(d)


def test_discover_rejects_empty_files(tmp_path: Path):
    d = tmp_path / "empty"
    d.mkdir()
    (d / "V1__empty.sql").write_text("   ")
    with pytest.raises(MigrationResolutionError, match="Empty migration"):
        discover(d)


def test_checksum_deterministic(migrations_dir: Path):
    files1 = discover(migrations_dir)
    files2 = discover(migrations_dir)
    assert files1[0].checksum == files2[0].checksum


def test_checksum_changes_on_content_change(migrations_dir: Path):
    files_before = discover(migrations_dir)
    (migrations_dir / "V1__create_users.sql").write_text("CREATE TABLE main.users (id INTEGER);")
    files_after = discover(migrations_dir)
    assert files_before[0].checksum != files_after[0].checksum


def test_next_version_empty(empty_migrations_dir: Path):
    assert next_version(empty_migrations_dir) == 1


def test_next_version_existing(migrations_dir: Path):
    assert next_version(migrations_dir) == 3


def test_next_version_nonexistent_dir():
    assert next_version("/nonexistent") == 1


def test_scaffold_versioned(empty_migrations_dir: Path):
    path = scaffold_migration(empty_migrations_dir, "create orders table")
    assert path.name == "V1__create_orders_table.sql"
    assert path.exists()
    assert path.read_text().startswith("-- create orders table")


def test_scaffold_repeatable(empty_migrations_dir: Path):
    path = scaffold_migration(empty_migrations_dir, "refresh views", repeatable=True)
    assert path.name == "R__refresh_views.sql"
    assert path.exists()


def test_scaffold_increments_version(migrations_dir: Path):
    path = scaffold_migration(migrations_dir, "next one")
    assert path.name == "V3__next_one.sql"


def test_scaffold_normalizes_description(empty_migrations_dir: Path):
    path = scaffold_migration(empty_migrations_dir, "Add User-Email Column!")
    assert path.name == "V1__add_user_email_column.sql"


def test_scaffold_empty_description_raises(empty_migrations_dir: Path):
    """scaffold_migration raises when description normalizes to empty."""
    with pytest.raises(MigrationResolutionError, match="empty"):
        scaffold_migration(empty_migrations_dir, "---")  # only special chars → empty desc
