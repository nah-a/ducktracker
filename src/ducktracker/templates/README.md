# ducktracker

This repository manages schema migrations for a DuckLake instance
using [ducktracker](https://github.com/ducklake-io/ducktracker).

## What's here

| Path | Purpose |
|---|---|
| `ducktracker.toml` | Configuration (backend, catalog name, migrations directory) |
| `migrations/` | SQL migration files (versioned `V1__*.sql` and repeatable `R__*.sql`) |

## Working locally

Install ducktracker and run migrations against your local DuckDB-backed catalog:

```bash
# Apply all pending migrations
ducktracker migrate

# Check migration status
ducktracker info

# Scaffold a new migration
ducktracker create "add users table"
```

The default backend in `ducktracker.toml` uses a local DuckDB file
(`ducklake_metadata.db`) — this file is git-ignored. Every developer gets
their own isolated local catalog.

## Pointing at a shared environment

Override connection settings at runtime via environment variables — no changes
to `ducktracker.toml` required:

```bash
# Target a shared PostgreSQL-backed DuckLake catalog
export DUCKTRACKER_CATALOG_BACKEND=postgres
export DUCKTRACKER_POSTGRES_CONNECTION="dbname=prod_catalog host=db.example.com port=5432 user=migrations"
ducktracker migrate

# Or pass flags directly
ducktracker --backend postgres --connection "..." migrate
```

## Dev → production workflow

1. **Write a migration locally** — `ducktracker create "describe your change"`
2. **Test it** — `ducktracker migrate && ducktracker info`
3. **Commit the migration file** — the SQL file in `migrations/` is the source of truth
4. **Apply to shared environments** — set the appropriate env vars and run `ducktracker migrate`
5. **Validate** — `ducktracker validate` checks that applied checksums still match files

## Environment variables

| Variable | Config key | Description |
|---|---|---|
| `DUCKTRACKER_CATALOG_NAME` | `connection.catalog_name` | Catalog name |
| `DUCKTRACKER_CATALOG_BACKEND` | `connection.catalog_backend` | `duckdb` or `postgres` |
| `DUCKTRACKER_DUCKDB_METADATA_PATH` | `connection.duckdb_metadata_path` | Local DuckDB metadata file |
| `DUCKTRACKER_POSTGRES_CONNECTION` | `connection.postgres_connection` | PostgreSQL connection string |
| `DUCKTRACKER_SECRET_NAME` | `connection.secret_name` | Named DuckDB secret (alternative to connection string) |
