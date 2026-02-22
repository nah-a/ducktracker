"""Migration file discovery, ordering, and validation."""

from __future__ import annotations

import hashlib
import re
from pathlib import Path

from ducktracker.models import MigrationFile, MigrationType

_VERSIONED_PATTERN = re.compile(r"^V(\d+)__([a-z0-9_]+)\.sql$")
_REPEATABLE_PATTERN = re.compile(r"^R__([a-z0-9_]+)\.sql$")


class MigrationResolutionError(Exception):
    pass


def discover(migrations_dir: str | Path) -> list[MigrationFile]:
    """Scan directory for migration files, validate, and return sorted."""
    path = Path(migrations_dir)
    if not path.is_dir():
        raise MigrationResolutionError(f"Migrations directory does not exist: {path}")

    sql_files = sorted(path.glob("*.sql"))
    if not sql_files:
        return []

    migrations: list[MigrationFile] = []
    seen_versions: dict[int, str] = {}

    for filepath in sql_files:
        mtype, version, description = _parse_filename(filepath.name)
        sql = filepath.read_text(encoding="utf-8")
        if not sql.strip():
            raise MigrationResolutionError(f"Empty migration file: {filepath.name}")

        if version is not None:
            if version in seen_versions:
                raise MigrationResolutionError(
                    f"Duplicate version {version}: {filepath.name} and {seen_versions[version]}"
                )
            seen_versions[version] = filepath.name

        migrations.append(
            MigrationFile(
                version=version,
                description=description,
                migration_type=mtype,
                filepath=str(filepath),
                checksum=_compute_checksum(filepath),
                sql=sql,
            )
        )

    # Sort: versioned ascending by version, then repeatable alphabetically
    versioned = sorted(
        [m for m in migrations if m.migration_type == MigrationType.VERSIONED],
        key=lambda m: m.version,  # type: ignore[arg-type]
    )
    repeatable = sorted(
        [m for m in migrations if m.migration_type == MigrationType.REPEATABLE],
        key=lambda m: m.description,
    )
    return versioned + repeatable


def _parse_filename(filename: str) -> tuple[MigrationType, int | None, str]:
    match = _VERSIONED_PATTERN.match(filename)
    if match:
        return MigrationType.VERSIONED, int(match.group(1)), match.group(2)

    match = _REPEATABLE_PATTERN.match(filename)
    if match:
        return MigrationType.REPEATABLE, None, match.group(1)

    raise MigrationResolutionError(
        f"Invalid migration filename: {filename}. "
        "Expected V<int>__<description>.sql or R__<description>.sql "
        "(description must be lowercase alphanumeric with underscores)"
    )


def _compute_checksum(filepath: Path) -> str:
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def next_version(migrations_dir: str | Path) -> int:
    """Return the next version number based on existing files."""
    path = Path(migrations_dir)
    if not path.is_dir():
        return 1

    max_version = 0
    for filepath in path.glob("V*__*.sql"):
        match = _VERSIONED_PATTERN.match(filepath.name)
        if match:
            max_version = max(max_version, int(match.group(1)))
    return max_version + 1


def scaffold_migration(migrations_dir: str | Path, description: str, repeatable: bool = False) -> Path:
    """Create a new empty migration file. Returns the path to the created file."""
    path = Path(migrations_dir)
    path.mkdir(parents=True, exist_ok=True)

    # Normalize description to snake_case
    desc = re.sub(r"[^a-z0-9]+", "_", description.lower()).strip("_")
    if not desc:
        raise MigrationResolutionError("Description cannot be empty")

    if repeatable:
        filename = f"R__{desc}.sql"
    else:
        version = next_version(path)
        filename = f"V{version}__{desc}.sql"

    filepath = path / filename
    filepath.write_text(f"-- {description}\n", encoding="utf-8")
    return filepath
