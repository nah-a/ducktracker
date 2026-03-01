"""Core migration engine — apply pending migrations, validate checksums."""

from __future__ import annotations

import logging
import time
from pathlib import Path

import duckdb

from ducktracker.config import DuckTrackerConfig
from ducktracker.history import HistoryManagerBase
from ducktracker.introspection import IntrospectorBase
from ducktracker.models import AppliedMigration, ChecksumMismatch, MigrationFile, MigrationState, MigrationType

logger = logging.getLogger(__name__)


class MigrationError(Exception):
    pass


def get_migration_status(
    applied: list[AppliedMigration],
    discovered: list[MigrationFile],
) -> list[tuple[MigrationFile | None, AppliedMigration | None, MigrationState]]:
    """Correlate discovered files with applied history to determine state."""
    applied_by_script: dict[str, AppliedMigration] = {}
    for a in applied:
        applied_by_script[a.script] = a

    discovered_scripts: set[str] = set()
    result: list[tuple[MigrationFile | None, AppliedMigration | None, MigrationState]] = []

    for mf in discovered:
        script_name = _script_name(mf)
        discovered_scripts.add(script_name)
        record = applied_by_script.get(script_name)

        if record is None:
            result.append((mf, None, MigrationState.PENDING))
        elif not record.success:
            result.append((mf, record, MigrationState.FAILED))
        elif mf.migration_type == MigrationType.REPEATABLE and mf.checksum != record.checksum:
            result.append((mf, record, MigrationState.OUTDATED))
        else:
            result.append((mf, record, MigrationState.APPLIED))

    # Applied but file no longer on disk
    for a in applied:
        if a.script not in discovered_scripts and a.script.startswith(("V", "R")):
            result.append((None, a, MigrationState.MISSING))

    return result


def get_pending_migrations(
    applied: list[AppliedMigration],
    discovered: list[MigrationFile],
    out_of_order: bool = False,
) -> list[MigrationFile]:
    """Return migrations that need to be applied, in order."""
    applied_versions: set[int] = set()
    applied_scripts: set[str] = set()
    max_applied_version = 0

    for a in applied:
        if a.success:
            applied_scripts.add(a.script)
            if a.version is not None:
                applied_versions.add(a.version)
                max_applied_version = max(max_applied_version, a.version)

    pending: list[MigrationFile] = []
    for mf in discovered:
        script_name = _script_name(mf)

        if mf.migration_type == MigrationType.VERSIONED:
            if mf.version in applied_versions:
                continue
            if not out_of_order and mf.version is not None and mf.version < max_applied_version:
                raise MigrationError(
                    f"Migration V{mf.version} is older than the latest applied version "
                    f"V{max_applied_version}. Use out_of_order=true to allow this."
                )
            pending.append(mf)

        elif mf.migration_type == MigrationType.REPEATABLE:
            # Re-apply if not yet applied or checksum changed
            record = next((a for a in applied if a.script == script_name and a.success), None)
            if record is None or record.checksum != mf.checksum:
                pending.append(mf)

    return pending


def apply_migrations(
    conn: duckdb.DuckDBPyConnection,
    config: DuckTrackerConfig,
    pending: list[MigrationFile],
    introspector: IntrospectorBase,
    history: HistoryManagerBase,
    dry_run: bool = False,
) -> list[tuple[MigrationFile, bool, int]]:
    """Apply pending migrations. Returns list of (migration, success, execution_time_ms)."""
    if not pending:
        return []

    history.ensure_history_table(conn, config.catalog_name, config.target_schema, config.schema_history_table)
    results: list[tuple[MigrationFile, bool, int]] = []

    for i, mf in enumerate(pending):
        is_last = i == len(pending) - 1
        script_name = _script_name(mf)

        if dry_run:
            logger.info("DRY RUN: Would apply %s", script_name)
            results.append((mf, True, 0))
            continue

        logger.info("Applying migration: %s", script_name)
        start = time.monotonic()
        success = True

        conn.execute("BEGIN")
        try:
            for statement in _split_statements(mf.sql):
                conn.execute(statement)
        except Exception as e:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                logger.error("ROLLBACK also failed for %s", script_name)
            success = False
            logger.error("Migration %s failed: %s", script_name, e)

        elapsed_ms = int((time.monotonic() - start) * 1000)

        if success:
            # Record history INSIDE the transaction so it's atomic with the migration.
            try:
                history.record_migration(
                    conn=conn,
                    catalog=config.catalog_name,
                    schema=config.target_schema,
                    table=config.schema_history_table,
                    version=mf.version,
                    description=mf.description,
                    migration_type=mf.migration_type.value,
                    script=script_name,
                    checksum=mf.checksum,
                    execution_time_ms=elapsed_ms,
                    success=True,
                    snapshot_json=None,
                )
                conn.execute("COMMIT")
            except Exception as e:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    logger.error("ROLLBACK also failed for %s", script_name)
                success = False
                logger.error("Failed to record migration %s: %s", script_name, e)
        else:
            # Record failure outside transaction (already rolled back)
            try:
                history.record_migration(
                    conn=conn,
                    catalog=config.catalog_name,
                    schema=config.target_schema,
                    table=config.schema_history_table,
                    version=mf.version,
                    description=mf.description,
                    migration_type=mf.migration_type.value,
                    script=script_name,
                    checksum=mf.checksum,
                    execution_time_ms=elapsed_ms,
                    success=False,
                    snapshot_json=None,
                )
            except Exception as e:
                logger.warning("Failed to record failure for %s: %s", script_name, e)

        # Capture schema snapshot after commit, only on last successful migration.
        if success and is_last:
            try:
                exclude = frozenset({config.schema_history_table})
                snapshot = introspector.introspect(conn, config.catalog_name, exclude_tables=exclude)
                snapshot_json = snapshot.to_json()
                # Update the history record with the snapshot
                history.update_latest_snapshot(
                    conn=conn,
                    catalog=config.catalog_name,
                    schema=config.target_schema,
                    table=config.schema_history_table,
                    snapshot_json=snapshot_json,
                )
            except Exception as e:
                logger.warning("Failed to capture schema snapshot: %s", e)

        results.append((mf, success, elapsed_ms))

        if not success:
            break

    return results


def validate_checksums(
    applied: list[AppliedMigration],
    discovered: list[MigrationFile],
) -> list[ChecksumMismatch]:
    """Verify applied migration checksums match files on disk."""
    discovered_by_script: dict[str, MigrationFile] = {_script_name(mf): mf for mf in discovered}
    mismatches: list[ChecksumMismatch] = []

    for a in applied:
        if not a.success or a.checksum == "baseline":
            continue
        mf = discovered_by_script.get(a.script)
        if mf is not None and a.migration_type == "V" and a.checksum != mf.checksum:
            mismatches.append(ChecksumMismatch(a.script, a.checksum, mf.checksum))

    return mismatches


def _script_name(mf: MigrationFile) -> str:
    """Extract just the filename from the full path."""
    return Path(mf.filepath).name


def _split_statements(sql: str) -> list[str]:
    """Split SQL text on semicolons, respecting quoted strings and comments."""
    statements: list[str] = []
    current: list[str] = []
    i = 0
    length = len(sql)

    while i < length:
        ch = sql[i]

        # Single-line comment
        if ch == "-" and i + 1 < length and sql[i + 1] == "-":
            end = sql.find("\n", i)
            if end == -1:
                current.append(sql[i:])
                break
            current.append(sql[i : end + 1])
            i = end + 1
            continue

        # Block comment
        if ch == "/" and i + 1 < length and sql[i + 1] == "*":
            end = sql.find("*/", i + 2)
            if end == -1:
                current.append(sql[i:])
                break
            current.append(sql[i : end + 2])
            i = end + 2
            continue

        # Quoted string (single quotes)
        if ch == "'":
            j = i + 1
            while j < length:
                if sql[j] == "'" and j + 1 < length and sql[j + 1] == "'":
                    j += 2  # escaped quote
                elif sql[j] == "'":
                    break
                else:
                    j += 1
            current.append(sql[i : j + 1])
            i = j + 1
            continue

        # Dollar-quoted string (PostgreSQL: $$...$$ or $tag$...$tag$)
        if ch == "$":
            j = i + 1
            while j < length and (sql[j].isalnum() or sql[j] == "_"):
                j += 1
            if j < length and sql[j] == "$":
                tag = sql[i : j + 1]  # e.g. "$$" or "$body$"
                end = sql.find(tag, j + 1)
                if end != -1:
                    current.append(sql[i : end + len(tag)])
                    i = end + len(tag)
                    continue

        # Statement terminator
        if ch == ";":
            stmt = "".join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    # Trailing statement without semicolon
    stmt = "".join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements
