"""Configuration loading for ducktracker."""

from __future__ import annotations

import os
import tomllib
from dataclasses import dataclass, fields
from pathlib import Path

VALID_BACKENDS = frozenset({"duckdb", "postgres"})

_DEFAULT_CONFIG_FILE = "ducktracker.toml"
_ENV_PREFIX = "DUCKTRACKER_"


@dataclass(frozen=True)
class DuckTrackerConfig:
    """Resolved configuration."""

    # Connection
    catalog_name: str = "my_ducklake"
    catalog_backend: str = "duckdb"  # "duckdb" or "postgres"
    duckdb_metadata_path: str = "ducklake_metadata.db"
    postgres_connection: str = ""
    data_path: str = ""
    read_only: bool = False
    secret_name: str = ""
    secret_directory: str = ""

    # Migrations
    migrations_dir: str = "migrations"
    schema_history_table: str = "ducktracker_schema_history"
    target_schema: str = "main"

    # Extensions
    extensions_path: str = ""

    # Behavior
    validate_on_migrate: bool = True
    out_of_order: bool = False


# Maps TOML sections/keys to DuckTrackerConfig field names.
_TOML_FIELD_MAP: dict[tuple[str, str], str] = {
    ("connection", "catalog_name"): "catalog_name",
    ("connection", "catalog_backend"): "catalog_backend",
    ("connection", "duckdb_metadata_path"): "duckdb_metadata_path",
    ("connection", "postgres_connection"): "postgres_connection",
    ("connection", "data_path"): "data_path",
    ("connection", "read_only"): "read_only",
    ("connection", "secret_name"): "secret_name",
    ("connection", "secret_directory"): "secret_directory",
    ("connection", "extensions_path"): "extensions_path",
    ("migrations", "directory"): "migrations_dir",
    ("migrations", "schema_history_table"): "schema_history_table",
    ("migrations", "target_schema"): "target_schema",
    ("behavior", "validate_on_migrate"): "validate_on_migrate",
    ("behavior", "out_of_order"): "out_of_order",
}

# Maps environment variable suffix to DuckTrackerConfig field names.
_ENV_FIELD_MAP: dict[str, str] = {
    "CATALOG_NAME": "catalog_name",
    "CATALOG_BACKEND": "catalog_backend",
    "DUCKDB_METADATA_PATH": "duckdb_metadata_path",
    "POSTGRES_CONNECTION": "postgres_connection",
    "DATA_PATH": "data_path",
    "READ_ONLY": "read_only",
    "SECRET_NAME": "secret_name",
    "SECRET_DIRECTORY": "secret_directory",
    "EXTENSIONS_PATH": "extensions_path",
    "MIGRATIONS_DIR": "migrations_dir",
    "SCHEMA_HISTORY_TABLE": "schema_history_table",
    "TARGET_SCHEMA": "target_schema",
    "VALIDATE_ON_MIGRATE": "validate_on_migrate",
    "OUT_OF_ORDER": "out_of_order",
}


def load_config(
    config_path: str | Path | None = None,
    overrides: dict[str, str] | None = None,
) -> DuckTrackerConfig:
    """Load configuration with precedence: CLI flags > env vars > TOML > defaults."""
    values: dict[str, object] = {}

    # Layer 1: TOML file
    toml_path = Path(config_path) if config_path else Path(_DEFAULT_CONFIG_FILE)
    if toml_path.is_file():
        _collect_toml(values, toml_path)

    # Layer 2: Environment variables
    _collect_env_overrides(values)

    # Layer 3: CLI flag overrides
    if overrides:
        for key, value in overrides.items():
            if value is not None and any(f.name == key for f in fields(DuckTrackerConfig)):
                values[key] = value

    config = DuckTrackerConfig(**values)

    if config.catalog_backend not in VALID_BACKENDS:
        raise ValueError(
            f"Invalid catalog_backend: {config.catalog_backend!r}. Must be one of: {', '.join(sorted(VALID_BACKENDS))}"
        )

    return config


def _collect_toml(values: dict[str, object], path: Path) -> None:
    with open(path, "rb") as f:
        data = tomllib.load(f)
    for (section, key), field_name in _TOML_FIELD_MAP.items():
        if section in data and key in data[section]:
            values[field_name] = data[section][key]


def _coerce_env_value(value: str, field_name: str) -> object:
    """Coerce a string env var value to the correct Python type for the given field."""
    type_map = {f.name: f.type for f in fields(DuckTrackerConfig)}
    field_type = type_map.get(field_name, "str")
    # With `from __future__ import annotations`, types are stored as strings.
    if field_type == "bool":
        return value.strip().lower() in ("true", "1", "yes")
    if field_type == "int":
        return int(value)
    return value


def _collect_env_overrides(values: dict[str, object]) -> None:
    for suffix, field_name in _ENV_FIELD_MAP.items():
        env_key = f"{_ENV_PREFIX}{suffix}"
        value = os.environ.get(env_key)
        if value is not None:
            values[field_name] = _coerce_env_value(value, field_name)
