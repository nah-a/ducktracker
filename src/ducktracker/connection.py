"""DuckDB connection management and catalog attachment."""

from __future__ import annotations

import contextlib
import logging
from collections.abc import Generator

import duckdb

from ducktracker.config import DuckTrackerConfig
from ducktracker.sql_utils import escape_str_lit, quote_ident

logger = logging.getLogger(__name__)


@contextlib.contextmanager
def connect(config: DuckTrackerConfig) -> Generator[duckdb.DuckDBPyConnection, None, None]:
    """Open DuckDB, load extensions, attach catalog, and yield the connection."""
    conn = duckdb.connect()
    try:
        if config.secret_directory:
            conn.execute("SET secret_directory = ?", [config.secret_directory])
        _setup_extensions(conn, config)
        _attach_ducklake(conn, config)
        yield conn
    finally:
        conn.close()


def _setup_extensions(conn: duckdb.DuckDBPyConnection, config: DuckTrackerConfig) -> None:
    if config.extensions_path:
        conn.execute(f"INSTALL '{escape_str_lit(config.extensions_path)}/ducklake.duckdb_extension'")
    else:
        conn.execute("INSTALL ducklake")
    conn.execute("LOAD ducklake")
    if config.catalog_backend == "postgres":
        if config.extensions_path:
            conn.execute(f"INSTALL '{escape_str_lit(config.extensions_path)}/postgres.duckdb_extension'")
        else:
            conn.execute("INSTALL postgres")
        conn.execute("LOAD postgres")


def _attach_ducklake(conn: duckdb.DuckDBPyConnection, config: DuckTrackerConfig) -> None:
    if config.secret_name:
        uri = f"ducklake:{escape_str_lit(config.secret_name)}"
    elif config.catalog_backend == "duckdb":
        uri = f"ducklake:{escape_str_lit(config.duckdb_metadata_path)}"
    else:
        if not config.postgres_connection:
            raise ValueError("postgres_connection is required when catalog_backend is 'postgres'")
        uri = f"ducklake:postgres:{escape_str_lit(config.postgres_connection)}"

    parts = [f"ATTACH '{uri}' AS {quote_ident(config.catalog_name)}"]
    options = []
    if config.data_path:
        options.append(f"DATA_PATH '{escape_str_lit(config.data_path)}'")
    if config.read_only:
        options.append("READ_ONLY")
    if options:
        parts.append(f"({', '.join(options)})")
    conn.execute(" ".join(parts))
    conn.execute(f"USE {quote_ident(config.catalog_name)}.{quote_ident(config.target_schema)}")


def connect_in_memory() -> duckdb.DuckDBPyConnection:
    """Open a plain in-memory DuckDB connection (no DuckLake). Used for testing."""
    return duckdb.connect()
