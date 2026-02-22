"""Tests for validate_checksums."""

from __future__ import annotations

from ducktracker.migrator import validate_checksums
from ducktracker.resolver import discover


def test_validate_checksums_match(setup_conn, migrations_dir, mgr):
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn, catalog="memory", schema="main", table="ducktracker_schema_history",
        version=1, description="create_users", migration_type="V",
        script="V1__create_users.sql", checksum=discovered[0].checksum,
        execution_time_ms=10, success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    mismatches = validate_checksums(applied, discovered)
    assert len(mismatches) == 0


def test_validate_checksums_mismatch(setup_conn, migrations_dir, mgr):
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn, catalog="memory", schema="main", table="ducktracker_schema_history",
        version=1, description="create_users", migration_type="V",
        script="V1__create_users.sql", checksum="wrong_checksum",
        execution_time_ms=10, success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    mismatches = validate_checksums(applied, discovered)
    assert len(mismatches) == 1


def test_validate_checksums_skips_baseline(setup_conn, migrations_dir, mgr):
    """validate_checksums ignores baseline records."""
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn, catalog="memory", schema="main", table="ducktracker_schema_history",
        version=0, description="baseline", migration_type="V",
        script="<< Baseline V0 >>", checksum="baseline",
        execution_time_ms=0, success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    mismatches = validate_checksums(applied, discovered)
    assert len(mismatches) == 0


def test_validate_checksums_skips_failed(setup_conn, migrations_dir, mgr):
    """validate_checksums ignores failed migration records."""
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn, catalog="memory", schema="main", table="ducktracker_schema_history",
        version=1, description="create_users", migration_type="V",
        script="V1__create_users.sql", checksum="wrong_checksum",
        execution_time_ms=10, success=False,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    mismatches = validate_checksums(applied, discovered)
    assert len(mismatches) == 0
