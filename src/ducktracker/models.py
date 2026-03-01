"""Data models for ducktracker."""

from __future__ import annotations

import dataclasses
import enum
import json
from dataclasses import dataclass, field
from datetime import datetime


class MigrationType(enum.Enum):
    VERSIONED = "V"
    REPEATABLE = "R"


class MigrationState(enum.Enum):
    PENDING = "PENDING"
    APPLIED = "APPLIED"
    FAILED = "FAILED"
    OUTDATED = "OUTDATED"  # repeatable: file changed since last apply
    MISSING = "MISSING"  # applied but file no longer on disk


@dataclass(frozen=True)
class MigrationFile:
    """A migration file discovered on disk."""

    version: int | None  # None for repeatable
    description: str
    migration_type: MigrationType
    filepath: str
    checksum: str  # sha256 hex digest
    sql: str


@dataclass
class AppliedMigration:
    """A row from the ducktracker_schema_history table."""

    installed_rank: int
    version: int | None
    description: str
    migration_type: str  # "V" or "R"
    script: str
    checksum: str
    installed_by: str
    installed_on: datetime
    execution_time_ms: int
    success: bool


# --- Schema introspection models ---


@dataclass(frozen=True)
class ColumnInfo:
    name: str
    data_type: str
    is_nullable: bool
    column_default: str | None
    ordinal_position: int


@dataclass(frozen=True)
class TableInfo:
    schema_name: str
    table_name: str
    columns: tuple[ColumnInfo, ...]
    primary_key: tuple[str, ...] | None
    unique_constraints: tuple[tuple[str, ...], ...]


@dataclass(frozen=True)
class ViewInfo:
    schema_name: str
    view_name: str
    sql_definition: str


@dataclass(frozen=True)
class IndexInfo:
    schema_name: str
    table_name: str
    index_name: str
    is_unique: bool
    sql_definition: str


@dataclass(frozen=True)
class SequenceInfo:
    schema_name: str
    sequence_name: str
    start_value: int
    increment_by: int
    min_value: int
    max_value: int


@dataclass(frozen=True)
class MacroInfo:
    schema_name: str
    macro_name: str
    macro_type: str  # "scalar" or "table"
    parameters: str
    definition: str


@dataclass
class SchemaSnapshot:
    """Complete schema state at a point in time."""

    catalog_name: str
    captured_at: datetime
    schemas: tuple[str, ...]
    tables: tuple[TableInfo, ...]
    views: tuple[ViewInfo, ...]
    indexes: tuple[IndexInfo, ...]
    sequences: tuple[SequenceInfo, ...]
    macros: tuple[MacroInfo, ...]

    def to_dict(self) -> dict:
        """Serialize to a JSON-compatible dict for storage."""
        d = dataclasses.asdict(self)
        d["captured_at"] = self.captured_at.isoformat()
        return d

    def to_json(self) -> str:
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, data: dict) -> SchemaSnapshot:
        """Deserialize from a stored dict."""
        return cls(
            catalog_name=data["catalog_name"],
            captured_at=datetime.fromisoformat(data["captured_at"]),
            schemas=tuple(data["schemas"]),
            tables=tuple(
                TableInfo(
                    schema_name=t["schema_name"],
                    table_name=t["table_name"],
                    columns=tuple(ColumnInfo(**c) for c in t["columns"]),
                    primary_key=tuple(t["primary_key"]) if t["primary_key"] else None,
                    unique_constraints=tuple(tuple(uc) for uc in t["unique_constraints"]),
                )
                for t in data["tables"]
            ),
            views=tuple(ViewInfo(**v) for v in data["views"]),
            indexes=tuple(IndexInfo(**i) for i in data["indexes"]),
            sequences=tuple(SequenceInfo(**s) for s in data["sequences"]),
            macros=tuple(MacroInfo(**m) for m in data["macros"]),
        )

    @classmethod
    def from_json(cls, json_str: str) -> SchemaSnapshot:
        return cls.from_dict(json.loads(json_str))


# --- Checksum validation models ---


@dataclass(frozen=True)
class ChecksumMismatch:
    """A checksum mismatch between an applied migration and its file on disk."""

    script: str
    expected: str
    actual: str


# --- Drift models ---


@dataclass(frozen=True)
class DriftItem:
    """A single schema drift finding."""

    object_type: str  # "table", "view", "column", etc.
    object_name: str  # fully qualified name
    drift_type: str  # "added", "removed", "modified"
    expected: str | None
    actual: str | None
    detail: str


@dataclass
class DriftReport:
    """Collection of all drift findings."""

    catalog_name: str
    checked_at: datetime
    snapshot_version: int | None
    items: list[DriftItem] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return len(self.items) > 0
