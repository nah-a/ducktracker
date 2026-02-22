"""Shared test fixtures."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest


@pytest.fixture
def conn():
    """In-memory DuckDB connection for unit tests."""
    c = duckdb.connect()
    yield c
    c.close()


@pytest.fixture
def migrations_dir(tmp_path: Path) -> Path:
    """Temporary directory pre-populated with sample migration files."""
    d = tmp_path / "migrations"
    d.mkdir()
    (d / "V1__create_users.sql").write_text(
        "CREATE TABLE main.users (id INTEGER NOT NULL, name VARCHAR NOT NULL);"
    )
    (d / "V2__add_email.sql").write_text(
        "ALTER TABLE main.users ADD COLUMN email VARCHAR;"
    )
    (d / "R__create_active_view.sql").write_text(
        "CREATE OR REPLACE VIEW main.active_users AS SELECT * FROM main.users WHERE name IS NOT NULL;"
    )
    return d


@pytest.fixture
def empty_migrations_dir(tmp_path: Path) -> Path:
    d = tmp_path / "empty_migrations"
    d.mkdir()
    return d


@pytest.fixture
def history_table(conn: duckdb.DuckDBPyConnection) -> str:
    """Create the schema history table in memory (using 'memory' as catalog)."""
    from ducktracker.history.ducklake import DuckLakeHistoryManager
    table_name = "ducktracker_schema_history"
    DuckLakeHistoryManager().ensure_history_table(conn, "memory", "main", table_name)
    return table_name


@pytest.fixture
def ducklake_cfg_file(tmp_path: Path, migrations_dir: Path) -> str:
    """Config file backed by a DuckDB DuckLake catalog in tmp_path.

    Returns the path to the written TOML as a string, for use with:
        runner.invoke(cli, ["-c", cfg_file, "migrate"])
    """
    meta_db = tmp_path / "meta.db"
    config_file = tmp_path / "ducktracker.toml"
    config_file.write_text(
        "[connection]\n"
        'catalog_name = "dev"\n'
        'catalog_backend = "duckdb"\n'
        f'duckdb_metadata_path = "{meta_db}"\n'
        "\n[migrations]\n"
        f'directory = "{migrations_dir}"\n'
        'schema_history_table = "ducktracker_schema_history"\n'
        'target_schema = "main"\n'
        "\n[behavior]\n"
        "validate_on_migrate = true\n"
        "out_of_order = false\n"
    )
    return str(config_file)
