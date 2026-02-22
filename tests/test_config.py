"""Tests for configuration loading."""

from __future__ import annotations

from pathlib import Path

from ducktracker.config import DuckTrackerConfig, load_config


def test_defaults():
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.catalog_name == "my_ducklake"
    assert cfg.catalog_backend == "duckdb"
    assert cfg.duckdb_metadata_path == "ducklake_metadata.db"
    assert cfg.migrations_dir == "migrations"
    assert cfg.validate_on_migrate is True


def test_load_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'catalog_name = "prod_lake"\n'
        'postgres_connection = "dbname=prod host=db.example.com"\n'
        '\n'
        '[migrations]\n'
        'directory = "db/migrations"\n'
        '\n'
        '[behavior]\n'
        'out_of_order = true\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.catalog_name == "prod_lake"
    assert cfg.postgres_connection == "dbname=prod host=db.example.com"
    assert cfg.migrations_dir == "db/migrations"
    assert cfg.out_of_order is True


def test_env_overrides(tmp_path: Path, monkeypatch):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'catalog_name = "from_toml"\n'
    )
    monkeypatch.setenv("DUCKTRACKER_CATALOG_NAME", "from_env")
    cfg = load_config(config_path=toml_file)
    assert cfg.catalog_name == "from_env"


def test_cli_overrides_beat_env(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_CATALOG_NAME", "from_env")
    cfg = load_config(overrides={"catalog_name": "from_cli"})
    assert cfg.catalog_name == "from_cli"


def test_missing_toml_uses_defaults():
    cfg = load_config(config_path="/nonexistent/path/ducktracker.toml")
    assert isinstance(cfg, DuckTrackerConfig)
    assert cfg.catalog_name == "my_ducklake"


def test_duckdb_backend_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'catalog_backend = "duckdb"\n'
        'duckdb_metadata_path = "/tmp/my_catalog.db"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.catalog_backend == "duckdb"
    assert cfg.duckdb_metadata_path == "/tmp/my_catalog.db"


def test_postgres_backend_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'catalog_backend = "postgres"\n'
        'postgres_connection = "dbname=prod host=db.example.com"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.catalog_backend == "postgres"
    assert cfg.postgres_connection == "dbname=prod host=db.example.com"


def test_backend_env_override(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_CATALOG_BACKEND", "postgres")
    monkeypatch.setenv("DUCKTRACKER_DUCKDB_METADATA_PATH", "/override/path.db")
    cfg = load_config()
    assert cfg.catalog_backend == "postgres"
    assert cfg.duckdb_metadata_path == "/override/path.db"


def test_backend_cli_override():
    cfg = load_config(overrides={"catalog_backend": "postgres", "duckdb_metadata_path": "/cli/path.db"})
    assert cfg.catalog_backend == "postgres"
    assert cfg.duckdb_metadata_path == "/cli/path.db"


def test_extensions_path_default_is_empty():
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.extensions_path == ""


def test_extensions_path_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'extensions_path = "/opt/duckdb/extensions"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.extensions_path == "/opt/duckdb/extensions"


def test_extensions_path_from_env(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_EXTENSIONS_PATH", "/env/extensions")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.extensions_path == "/env/extensions"


def test_secret_name_default_is_empty():
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.secret_name == ""


def test_secret_name_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'secret_name = "my_ducklake_secret"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.secret_name == "my_ducklake_secret"


def test_secret_name_from_env(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_SECRET_NAME", "env_secret")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.secret_name == "env_secret"


def test_secret_directory_default_is_empty():
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.secret_directory == ""


def test_secret_directory_from_toml(tmp_path: Path):
    toml_file = tmp_path / "test.toml"
    toml_file.write_text(
        '[connection]\n'
        'secret_directory = "/run/secrets/duckdb"\n'
    )
    cfg = load_config(config_path=toml_file)
    assert cfg.secret_directory == "/run/secrets/duckdb"


def test_secret_directory_from_env(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_SECRET_DIRECTORY", "/env/secrets")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.secret_directory == "/env/secrets"


def test_validate_on_migrate_bool_env_false(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_VALIDATE_ON_MIGRATE", "false")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.validate_on_migrate is False


def test_out_of_order_bool_env_true(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_OUT_OF_ORDER", "true")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.out_of_order is True


def test_bool_env_coercion_accepts_yes(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_OUT_OF_ORDER", "yes")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.out_of_order is True


def test_bool_env_coercion_accepts_1(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_OUT_OF_ORDER", "1")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.out_of_order is True


def test_bool_env_coercion_false_for_other_values(monkeypatch):
    monkeypatch.setenv("DUCKTRACKER_OUT_OF_ORDER", "no")
    cfg = load_config(config_path="/nonexistent/path.toml")
    assert cfg.out_of_order is False
