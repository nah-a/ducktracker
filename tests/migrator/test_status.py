"""Tests for get_migration_status and get_pending_migrations."""

from __future__ import annotations

import pytest

from ducktracker.migrator import (
    MigrationError,
    get_migration_status,
    get_pending_migrations,
)
from ducktracker.models import MigrationState
from ducktracker.resolver import discover


def test_get_migration_status_all_pending(setup_conn, migrations_dir, mgr):
    discovered = discover(migrations_dir)
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    statuses = get_migration_status(applied, discovered)
    assert all(s == MigrationState.PENDING for _, _, s in statuses)


def test_get_pending_migrations(setup_conn, migrations_dir, mgr):
    discovered = discover(migrations_dir)
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    pending = get_pending_migrations(applied, discovered)
    assert len(pending) == 3


def test_get_pending_skips_applied(setup_conn, migrations_dir, mgr):
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=1,
        description="create_users",
        migration_type="V",
        script="V1__create_users.sql",
        checksum=discovered[0].checksum,
        execution_time_ms=10,
        success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    pending = get_pending_migrations(applied, discovered)
    assert len(pending) == 2
    assert pending[0].version == 2


def test_out_of_order_rejected(setup_conn, tmp_path, mgr):
    d = tmp_path / "ooo"
    d.mkdir()
    (d / "V5__later.sql").write_text("SELECT 1;")
    (d / "V3__earlier.sql").write_text("SELECT 2;")
    discovered = discover(d)

    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=5,
        description="later",
        migration_type="V",
        script="V5__later.sql",
        checksum=discovered[0].checksum,
        execution_time_ms=10,
        success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")

    with pytest.raises(MigrationError, match="older than the latest"):
        get_pending_migrations(applied, discovered, out_of_order=False)


def test_out_of_order_allowed_when_flag_set(setup_conn, mgr, tmp_path):
    """out_of_order=True allows applying a lower version after a higher one."""
    d = tmp_path / "ooo_allow"
    d.mkdir()
    (d / "V5__later.sql").write_text("SELECT 1;")
    (d / "V3__earlier.sql").write_text("SELECT 2;")
    discovered = discover(d)
    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=5,
        description="later",
        migration_type="V",
        script="V5__later.sql",
        checksum=discovered[0].checksum,
        execution_time_ms=10,
        success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    pending = get_pending_migrations(applied, discovered, out_of_order=True)
    assert any(mf.version == 3 for mf in pending)


def test_get_migration_status_failed(setup_conn, migrations_dir, mgr):
    """get_migration_status returns FAILED for a migration recorded with success=False."""
    discovered = discover(migrations_dir)
    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=1,
        description="create_users",
        migration_type="V",
        script="V1__create_users.sql",
        checksum=discovered[0].checksum,
        execution_time_ms=10,
        success=False,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    statuses = get_migration_status(applied, discovered)
    v1_status = next(s for mf, _, s in statuses if mf and mf.version == 1)
    assert v1_status == MigrationState.FAILED


def test_get_migration_status_outdated(setup_conn, tmp_path, mgr):
    """get_migration_status returns OUTDATED for a repeatable migration with a changed checksum."""
    d = tmp_path / "migs"
    d.mkdir()
    (d / "R__refresh.sql").write_text("SELECT 1;")
    discovered = discover(d)
    # Record it with a wrong checksum to simulate the file having changed
    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=None,
        description="refresh",
        migration_type="R",
        script="R__refresh.sql",
        checksum="old_checksum_that_no_longer_matches",
        execution_time_ms=5,
        success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    statuses = get_migration_status(applied, discovered)
    r_status = next(s for mf, _, s in statuses if mf and mf.migration_type.value == "R")
    assert r_status == MigrationState.OUTDATED


def test_get_migration_status_missing(setup_conn, mgr):
    """get_migration_status returns MISSING for an applied migration whose file is gone."""
    mgr.record_migration(
        conn=setup_conn,
        catalog="memory",
        schema="main",
        table="ducktracker_schema_history",
        version=99,
        description="deleted",
        migration_type="V",
        script="V99__deleted.sql",
        checksum="somehash",
        execution_time_ms=5,
        success=True,
    )
    applied = mgr.get_applied_migrations(setup_conn, "memory", "main", "ducktracker_schema_history")
    # Pass an empty discovered list so V99 has no matching file
    statuses = get_migration_status(applied, [])
    assert len(statuses) == 1
    assert statuses[0][2] == MigrationState.MISSING
