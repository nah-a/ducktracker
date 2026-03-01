"""Tests for the migrate CLI command."""

from __future__ import annotations

from ducktracker.cli import cli


def test_migrate_applies_all(runner, ducklake_cfg_file):
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    assert result.exit_code == 0
    assert "Successfully applied 3" in result.output


def test_migrate_dry_run_no_changes(runner, ducklake_cfg_file):
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate", "--dry-run"])
    assert result.exit_code == 0
    assert "DRY RUN" in result.output
    # Nothing was persisted — re-invoking still sees 3 pending
    result2 = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate", "--dry-run"])
    assert result2.exit_code == 0
    assert "Applying 3 migration(s)" in result2.output


def test_migrate_up_to_date(runner, ducklake_cfg_file):
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    assert result.exit_code == 0
    assert "up to date" in result.output


def test_migrate_target_version(runner, ducklake_cfg_file):
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate", "--target", "1"])
    assert result.exit_code == 0
    # V2 should NOT be applied; V1 (and any repeatable migrations) should be
    assert "V1__create_users.sql" in result.output
    assert "V2__add_email.sql" not in result.output


def test_migrate_blocks_on_checksum_mismatch(runner, ducklake_cfg_file, migrations_dir):
    """With validate_on_migrate=true, a tampered file blocks the next migrate."""
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    (migrations_dir / "V1__create_users.sql").write_text("-- tampered")
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    assert result.exit_code != 0
    assert "CHECKSUM MISMATCH" in result.output


def test_migrate_exits_on_out_of_order_error(runner, ducklake_cfg_file, migrations_dir):
    """migrate exits 1 with an error message when an out-of-order migration is detected."""
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    (migrations_dir / "V0__retroactive.sql").write_text("SELECT 1;")
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    assert result.exit_code != 0
    assert "ERROR" in result.output


def test_migrate_exits_1_on_sql_failure(runner, ducklake_cfg_file, migrations_dir):
    """migrate exits 1 and reports failure when a migration SQL is invalid."""
    runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    (migrations_dir / "V3__bad.sql").write_text("THIS IS NOT VALID SQL;")
    result = runner.invoke(cli, ["-c", ducklake_cfg_file, "migrate"])
    assert result.exit_code != 0
    assert "FAILED" in result.output
