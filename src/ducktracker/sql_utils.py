"""Shared SQL utility functions."""

from __future__ import annotations


def quote_ident(name: str) -> str:
    """Double-quote a SQL identifier, escaping any embedded double quotes."""
    return '"' + name.replace('"', '""') + '"'


def escape_str_lit(value: str) -> str:
    """Escape a string value for embedding inside a single-quoted SQL string literal."""
    return value.replace("'", "''")
