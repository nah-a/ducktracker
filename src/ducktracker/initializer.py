"""Initializes a directory with ducktracker configuration and scaffolding."""

from __future__ import annotations

import importlib.resources
from pathlib import Path

_BACKEND_TEMPLATES: dict[str, str] = {
    "ducklake-duckdb": "ducktracker.ducklake-duckdb.toml",
    "ducklake-postgres": "ducktracker.ducklake-postgres.toml",
    "pg-duckdb": "ducktracker.pg-duckdb.toml",
}


class ConflictError(Exception):
    """Raised when one or more output paths already exist."""

    def __init__(self, conflicts: list[Path]) -> None:
        self.conflicts = conflicts
        super().__init__(f"Conflicting paths: {', '.join(str(p) for p in conflicts)}")


def init_directory(path: Path, backend: str) -> list[Path]:
    """Initialise *path* with ducktracker scaffolding for *backend*.

    Returns the list of paths that were created.
    Raises FileNotFoundError if *path* does not exist.
    Raises NotADirectoryError if *path* exists but is not a directory.
    Raises ConflictError (with no files written) if any output already exists.
    """
    if not path.exists():
        raise FileNotFoundError(f"Directory does not exist: {path}")
    if not path.is_dir():
        raise NotADirectoryError(f"Path is not a directory: {path}")

    toml_path = path / "ducktracker.toml"
    migrations_path = path / "migrations"
    gitignore_path = path / ".gitignore"
    readme_path = path / "README.md"

    conflicts = [p for p in [toml_path, migrations_path, gitignore_path, readme_path] if p.exists()]
    if conflicts:
        raise ConflictError(conflicts)

    templates = importlib.resources.files("ducktracker.templates")
    toml_path.write_text(
        (templates / _BACKEND_TEMPLATES[backend]).read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    migrations_path.mkdir()
    gitignore_path.write_text(
        (templates / ".gitignore").read_text(encoding="utf-8"),
        encoding="utf-8",
    )
    readme_path.write_text(
        (templates / "README.md").read_text(encoding="utf-8"),
        encoding="utf-8",
    )

    return [toml_path, migrations_path, gitignore_path, readme_path]
