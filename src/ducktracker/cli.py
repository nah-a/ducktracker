"""CLI entry point using Click."""

from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from ducktracker import __version__
from ducktracker.backends import get_history_manager, get_introspector
from ducktracker.config import DuckTrackerConfig, load_config
from ducktracker.connection import connect
from ducktracker.drift import detect_drift
from ducktracker.migrator import (
    MigrationError,
    apply_migrations,
    get_migration_status,
    get_pending_migrations,
    validate_checksums,
)
from ducktracker.models import MigrationState
from ducktracker.resolver import discover, scaffold_migration

console = Console()


def _truncate(s: str, max_len: int = 12) -> str:
    return s[:max_len] + "..." if len(s) > max_len else s


@click.group()
@click.version_option(version=__version__, prog_name="ducktracker")
@click.option("--config", "-c", type=click.Path(exists=True), default=None, help="Path to ducktracker.toml.")
@click.option("--catalog", default=None, help="DuckLake catalog name (overrides config).")
@click.option(
    "--backend", default=None, type=click.Choice(["duckdb", "postgres", "pg_duckdb"]), help="Catalog backend type."
)
@click.option("--metadata", default=None, help="DuckDB metadata file path (for duckdb backend).")
@click.option("--connection", default=None, help="PostgreSQL connection string (for postgres backend).")
@click.option("--secrets-dir", default=None, help="Directory for DuckDB persistent secrets.")
@click.pass_context
def cli(
    ctx: click.Context,
    config: str | None,
    catalog: str | None,
    backend: str | None,
    metadata: str | None,
    connection: str | None,
    secrets_dir: str | None,
) -> None:
    """ducktracker: DDL migration and schema drift detection for DuckLake and PostgreSQL."""
    ctx.ensure_object(dict)
    overrides: dict[str, str] = {}
    if catalog:
        overrides["catalog_name"] = catalog
    if backend:
        overrides["catalog_backend"] = backend
    if metadata:
        overrides["duckdb_metadata_path"] = metadata
    if connection:
        overrides["postgres_connection"] = connection
    if secrets_dir:
        overrides["secret_directory"] = secrets_dir
    ctx.obj["config_path"] = config
    ctx.obj["overrides"] = overrides


def _load_cfg(ctx: click.Context) -> DuckTrackerConfig:
    return load_config(config_path=ctx.obj["config_path"], overrides=ctx.obj["overrides"])


@cli.command()
@click.option("--dry-run", is_flag=True, help="Show what would be applied without executing.")
@click.option("--target", type=int, default=None, help="Migrate up to this version (inclusive).")
@click.pass_context
def migrate(ctx: click.Context, dry_run: bool, target: int | None) -> None:
    """Apply pending migrations to the target database."""
    cfg = _load_cfg(ctx)
    with connect(cfg) as conn:
        history = get_history_manager(cfg)
        introspector = get_introspector(cfg)
        history.ensure_history_table(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        discovered = discover(cfg.migrations_dir)
        applied = history.get_applied_migrations(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)

        if cfg.validate_on_migrate:
            mismatches = validate_checksums(applied, discovered)
            if mismatches:
                for m in mismatches:
                    console.print(f"[red]CHECKSUM MISMATCH:[/red] {m.script}")
                ctx.exit(1)

        try:
            pending = get_pending_migrations(applied, discovered, out_of_order=cfg.out_of_order)
        except MigrationError as e:
            console.print(f"[red]ERROR:[/red] {e}")
            ctx.exit(1)

        if target is not None:
            pending = [m for m in pending if m.version is None or m.version <= target]

        if not pending:
            console.print("[green]Schema is up to date. No migrations to apply.[/green]")
            return

        prefix = "[yellow]DRY RUN:[/yellow] " if dry_run else ""
        console.print(f"{prefix}Applying {len(pending)} migration(s)...\n")

        results = apply_migrations(conn, cfg, pending, introspector=introspector, history=history, dry_run=dry_run)

        for mf, success, elapsed_ms in results:
            status = "[green]OK[/green]" if success else "[red]FAILED[/red]"
            name = Path(mf.filepath).name
            console.print(f"  {status}  {name}  ({elapsed_ms}ms)")

        failures = [r for r in results if not r[1]]
        if failures:
            console.print(f"\n[red]{len(failures)} migration(s) failed.[/red]")
            ctx.exit(1)
        else:
            console.print(f"\n[green]Successfully applied {len(results)} migration(s).[/green]")


@cli.command()
@click.pass_context
def info(ctx: click.Context) -> None:
    """Show migration status: applied, pending, failed, missing."""
    cfg = _load_cfg(ctx)
    with connect(cfg) as conn:
        history = get_history_manager(cfg)
        history.ensure_history_table(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        discovered = discover(cfg.migrations_dir)
        applied = history.get_applied_migrations(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        statuses = get_migration_status(applied, discovered)

    table = Table(title="Migration Status")
    table.add_column("Version", style="cyan")
    table.add_column("Description")
    table.add_column("Type")
    table.add_column("State")
    table.add_column("Applied On")
    table.add_column("Checksum")

    state_styles = {
        MigrationState.APPLIED: "green",
        MigrationState.PENDING: "yellow",
        MigrationState.FAILED: "red",
        MigrationState.OUTDATED: "magenta",
        MigrationState.MISSING: "red",
    }

    for mf, record, state in statuses:
        version = (
            str(mf.version) if mf and mf.version is not None
            else (str(record.version) if record and record.version else "-")
        )
        desc = mf.description if mf else (record.description if record else "?")
        mtype = (mf.migration_type.value if mf else (record.migration_type if record else "?"))
        style = state_styles.get(state, "white")
        applied_on = str(record.installed_on)[:19] if record else "-"
        checksum = (_truncate(mf.checksum) if mf else (_truncate(record.checksum) if record else "-"))
        table.add_row(version, desc, mtype, f"[{style}]{state.value}[/{style}]", applied_on, checksum)

    console.print(table)


@cli.command()
@click.pass_context
def validate(ctx: click.Context) -> None:
    """Verify checksums of applied migrations haven't been tampered with."""
    cfg = _load_cfg(ctx)
    with connect(cfg) as conn:
        history = get_history_manager(cfg)
        history.ensure_history_table(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        discovered = discover(cfg.migrations_dir)
        applied = history.get_applied_migrations(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        mismatches = validate_checksums(applied, discovered)

    if not mismatches:
        console.print("[green]All checksums match.[/green]")
    else:
        for m in mismatches:
            console.print(
                f"[red]MISMATCH:[/red] {m.script} "
                f"(applied: {_truncate(m.expected)}, file: {_truncate(m.actual)})"
            )
        console.print(f"\n[red]{len(mismatches)} checksum mismatch(es) found.[/red]")
        ctx.exit(1)


@cli.command()
@click.pass_context
def drift(ctx: click.Context) -> None:
    """Detect schema drift between expected state and live catalog."""
    cfg = _load_cfg(ctx)
    with connect(cfg) as conn:
        history = get_history_manager(cfg)
        introspector = get_introspector(cfg)
        snapshot = history.get_latest_snapshot(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        if snapshot is None:
            console.print("[yellow]No schema snapshot found. Run 'migrate' first to establish a baseline.[/yellow]")
            ctx.exit(1)

        exclude = frozenset({cfg.schema_history_table})
        kwargs = {}
        if cfg.catalog_backend == "pg_duckdb":
            kwargs["schema_filter"] = cfg.target_schema
        actual = introspector.introspect(conn, cfg.catalog_name, exclude_tables=exclude, **kwargs)

        # Determine snapshot version from applied migrations
        applied = history.get_applied_migrations(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        snapshot_version = None
        for a in reversed(applied):
            if a.version is not None and a.success:
                snapshot_version = a.version
                break

        report = detect_drift(snapshot, actual, cfg.catalog_name, snapshot_version=snapshot_version)

    if not report.has_drift:
        console.print("[green]No schema drift detected.[/green]")
        return

    console.print("[bold]Schema Drift Report[/bold]")
    console.print(f"Catalog: {report.catalog_name}")
    if report.snapshot_version is not None:
        console.print(f"Snapshot: V{report.snapshot_version}")
    console.print(f"Checked: {report.checked_at:%Y-%m-%d %H:%M:%S}")
    console.print(f"\n[red]{len(report.items)} difference(s) detected:[/red]\n")

    symbols = {"added": "+", "removed": "-", "modified": "~"}
    styles = {"added": "green", "removed": "red", "modified": "yellow"}

    for item in report.items:
        sym = symbols.get(item.drift_type, "?")
        style = styles.get(item.drift_type, "white")
        console.print(f"  [{style}]{sym}[/{style}] [{style}][{item.object_type}][/{style}]  {item.object_name}")
        if item.drift_type == "modified" and item.expected and item.actual:
            console.print(f"      snapshot: {item.expected}")
            console.print(f"      live:     {item.actual}")

    ctx.exit(1)


@cli.command()
@click.option("--version", "-v", type=int, required=True, help="Version to baseline at.")
@click.option("--description", "-d", default="baseline", help="Baseline description.")
@click.pass_context
def baseline(ctx: click.Context, version: int, description: str) -> None:
    """Mark existing database state as a specific version without running migrations."""
    cfg = _load_cfg(ctx)
    with connect(cfg) as conn:
        history = get_history_manager(cfg)
        introspector = get_introspector(cfg)
        history.ensure_history_table(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)
        applied = history.get_applied_migrations(conn, cfg.catalog_name, cfg.target_schema, cfg.schema_history_table)

        if applied:
            console.print("[red]Cannot baseline: migrations have already been applied.[/red]")
            ctx.exit(1)

        # Capture current state as snapshot
        exclude = frozenset({cfg.schema_history_table})
        kwargs = {}
        if cfg.catalog_backend == "pg_duckdb":
            kwargs["schema_filter"] = cfg.target_schema
        snapshot = introspector.introspect(conn, cfg.catalog_name, exclude_tables=exclude, **kwargs)

        history.record_baseline(
            conn=conn,
            catalog=cfg.catalog_name,
            schema=cfg.target_schema,
            table=cfg.schema_history_table,
            version=version,
            description=description,
            snapshot_json=snapshot.to_json(),
        )

    console.print(f"[green]Baselined at version V{version}.[/green]")


@cli.command()
@click.argument("description")
@click.option("--repeatable", "-r", is_flag=True, help="Create a repeatable migration instead.")
@click.pass_context
def create(ctx: click.Context, description: str, repeatable: bool) -> None:
    """Scaffold a new migration file with the next version number."""
    cfg = _load_cfg(ctx)
    filepath = scaffold_migration(cfg.migrations_dir, description, repeatable=repeatable)
    console.print(f"[green]Created:[/green] {filepath}")


@cli.command()
@click.argument("path", default=".", type=click.Path(file_okay=False))
@click.option(
    "--backend",
    type=click.Choice(["ducklake-duckdb", "ducklake-postgres", "pg-duckdb"]),
    default=None,
    help="Backend type: ducklake-duckdb, ducklake-postgres, or pg-duckdb.",
)
@click.pass_context
def init(ctx: click.Context, path: str, backend: str | None) -> None:
    """Initialize a directory with ducktracker configuration and scaffolding."""
    from ducktracker.initializer import ConflictError, init_directory

    target = Path(path).resolve()

    if not target.exists():
        console.print(f"[red]ERROR:[/red] Directory does not exist: {target}")
        console.print(f"Create it first with: mkdir {target}")
        ctx.exit(1)

    if backend is None:
        backend = click.prompt(
            "Select a backend",
            type=click.Choice(["ducklake-duckdb", "ducklake-postgres", "pg-duckdb"]),
        )

    try:
        created = init_directory(target, backend)
    except ConflictError as e:
        console.print("[red]ERROR:[/red] The following files already exist:")
        for p in e.conflicts:
            console.print(f"  {p}")
        ctx.exit(1)

    console.print(f"[green]Initialized {target}[/green]")
    for p in created:
        rel = p.relative_to(target) if p.is_relative_to(target) else p
        console.print(f"  Created: {rel}")
    console.print("\nNext steps:")
    console.print("  1. Review and edit ducktracker.toml")
    console.print("  2. Add migration files to migrations/")
    console.print("  3. Run: ducktracker migrate")
