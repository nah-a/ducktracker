# ducktracker

DDL migration and schema drift detection for DuckLake. Supports DuckDB file (local dev) and PostgreSQL (team/production) catalog backends.

## Quick Start

```bash
# Create a virtual environment and install
python -m venv /venvs/ducktracker
source /venvs/ducktracker/bin/activate
pip install -e ".[dev]"

# Scaffold a migration
ducktracker create "create users table"

# Apply migrations
ducktracker -c ducktracker.toml migrate

# Check status
ducktracker info

# Detect schema drift
ducktracker drift
```

## Configuration

Copy `ducktracker.toml.example` to `ducktracker.toml` and edit the connection settings.

### Catalog Backends

**DuckDB file** (default) — no PostgreSQL needed, ideal for local development:
```toml
[connection]
catalog_backend = "duckdb"
duckdb_metadata_path = "ducklake_metadata.db"
```

**PostgreSQL** — shared metadata catalog for team/production:
```toml
[connection]
catalog_backend = "postgres"
postgres_connection = "dbname=ducklake_catalog host=localhost port=5432 user=ducklake"
```

Environment variable overrides: `DUCKTRACKER_CATALOG_BACKEND`, `DUCKTRACKER_DUCKDB_METADATA_PATH`, `DUCKTRACKER_POSTGRES_CONNECTION`, `DUCKTRACKER_EXTENSIONS_PATH`, etc.

## Migration File Conventions

- Versioned: `V{int}__{description}.sql` — applied once, in order
- Repeatable: `R__{description}.sql` — re-applied when content changes

## Commands

| Command | Purpose |
|---|---|
| `migrate` | Apply pending migrations |
| `info` | Show migration status |
| `validate` | Verify checksums haven't changed |
| `drift` | Detect schema drift (exit code 1 if found) |
| `baseline` | Mark existing DB at a version |
| `create` | Scaffold a new migration file |
