# ducktracker

Simple tool to keep your ducks in a row.

DDL migration and schema drift detection for DuckLake.

## Install

```bash
pip install -e .
```

For development (includes pytest and ruff):

```bash
pip install -e ".[dev]"
```

## Initialize

Create a directory and run `init` to scaffold the config and migrations folder:

```bash
mkdir my-ducklake
ducktracker init my-ducklake --backend ducklake-duckdb
```

If you omit `--backend`, you'll be prompted. Two options:

| Backend | Use when |
|---|---|
| `ducklake-duckdb` | DuckLake catalog in a local DuckDB file. Good for local dev. |
| `ducklake-postgres` | DuckLake catalog in PostgreSQL. Good for teams. |

`init` creates `ducktracker.toml`, `migrations/`, `.gitignore`, and a `README.md` in the target directory. The directory must already exist — `init` won't create it.

## Writing migrations

Two file types, both live in your `migrations/` directory:

- **Versioned:** `V{n}__{description}.sql` — applied once, in version order
- **Repeatable:** `R__{description}.sql` — re-applied whenever the file content changes

Use `create` to scaffold the next file:

```bash
ducktracker create "create users table"
# → migrations/V2__create_users_table.sql

ducktracker create "refresh views" --repeatable
# → migrations/R__refresh_views.sql
```

Then open the file and add your DDL.

## Running migrations

```bash
ducktracker migrate
```

Flags worth knowing:

```
--dry-run       Show what would be applied without executing any migration SQL
--target N      Only apply migrations up to version N (inclusive)
```

By default, checksums of previously applied versioned migrations are validated before anything runs. If a migration file has been modified after it was applied, `migrate` will refuse to proceed.

## Checking status

```bash
ducktracker info
```

Prints a table with version, description, type, state, applied timestamp, and checksum for every migration it knows about. States: `applied`, `pending`, `failed`, `outdated`, `missing`.

To check only checksums without touching the database state:

```bash
ducktracker validate
```

Exits 1 if any applied migration's file has changed since it was applied.

## Detecting drift

```bash
ducktracker drift
```

Compares the schema snapshot captured at the last `migrate` or `baseline` run against the live catalog. Exits 1 if differences are found — useful in CI to catch out-of-band schema changes.

Output shows added (`+`), removed (`-`), and modified (`~`) objects.

## Baselining an existing database

If you're bringing an existing database under ducktracker management, use `baseline` before running any migrations:

```bash
ducktracker baseline --version 3
ducktracker baseline --version 3 --description "after initial setup"
```

This captures the current schema as a snapshot and marks the database at the given version. It will refuse to run if the history table already contains any records (including a prior baseline).

## Configuration

ducktracker looks for `ducktracker.toml` in the current directory by default. Override with `-c`:

```bash
ducktracker -c path/to/ducktracker.toml migrate
```

A minimal config looks like this:

```toml
[connection]
catalog_name = "my_ducklake"
catalog_backend = "duckdb"
duckdb_metadata_path = "ducklake_metadata.db"

[migrations]
directory = "migrations"
target_schema = "main"

[behavior]
validate_on_migrate = true
out_of_order = false
```

### Using a DuckDB secret

Instead of putting connection details directly in the config file, you can reference a named DuckDB persistent secret. Set `secret_name` in place of `duckdb_metadata_path` (duckdb backend) or `postgres_connection` (postgres backend):

```toml
[connection]
catalog_name = "my_ducklake"
catalog_backend = "postgres"
secret_name = "my_ducklake_secret"
```

The secret must already exist in DuckDB before ducktracker runs.

If your secrets are stored outside DuckDB's default location (`~/.duckdb/stored_secrets/`), add `secret_directory` to the same `[connection]` block:

```toml
[connection]
catalog_name = "my_ducklake"
catalog_backend = "postgres"
secret_name = "my_ducklake_secret"
secret_directory = "/run/secrets/duckdb"
```

ducktracker issues `SET secret_directory` before opening any catalog connection, so the path is in effect for the entire session.

### Config layers

Settings are resolved in this order, with later layers winning:

1. `ducktracker.toml`
2. Environment variables
3. CLI flags

### Key environment variables

| Variable | Config equivalent |
|---|---|
| `DUCKTRACKER_CATALOG_NAME` | `connection.catalog_name` |
| `DUCKTRACKER_CATALOG_BACKEND` | `connection.catalog_backend` |
| `DUCKTRACKER_DUCKDB_METADATA_PATH` | `connection.duckdb_metadata_path` |
| `DUCKTRACKER_POSTGRES_CONNECTION` | `connection.postgres_connection` |
| `DUCKTRACKER_SECRET_DIRECTORY` | `connection.secret_directory` |

### Global CLI flags

These apply to all commands except `init`:

```
-c, --config PATH       Path to ducktracker.toml
    --catalog NAME      Override catalog name
    --backend TYPE      Override backend (duckdb, postgres) — note these differ from init's backend values
    --metadata PATH     Override DuckDB metadata file path
    --connection STRING Override PostgreSQL connection string
    --secrets-dir PATH  Override DuckDB persistent secrets directory
```
