"""Tests for _split_statements."""

from __future__ import annotations

from ducktracker.migrator import _split_statements


def test_split_statements_basic():
    sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);"
    stmts = _split_statements(sql)
    assert len(stmts) == 2


def test_split_statements_with_quotes():
    sql = "INSERT INTO t VALUES ('hello; world'); SELECT 1;"
    stmts = _split_statements(sql)
    assert len(stmts) == 2
    assert "hello; world" in stmts[0]


def test_split_statements_with_comments():
    sql = "-- this has a ; in the comment\nSELECT 1; SELECT 2;"
    stmts = _split_statements(sql)
    assert len(stmts) == 2


def test_split_statements_block_comment():
    sql = "/* multi; line; */ SELECT 1;"
    stmts = _split_statements(sql)
    assert len(stmts) == 1


def test_split_statements_no_trailing_semicolon():
    sql = "SELECT 1"
    stmts = _split_statements(sql)
    assert len(stmts) == 1
    assert stmts[0] == "SELECT 1"


def test_split_statements_dollar_quote():
    """Semicolons inside $$ dollar-quoted strings are not treated as terminators."""
    sql = "CREATE FUNCTION f() RETURNS INT LANGUAGE sql AS $$ SELECT 1; $$;"
    stmts = _split_statements(sql)
    assert len(stmts) == 1
    assert "SELECT 1;" in stmts[0]


def test_split_statements_tagged_dollar_quote():
    """Tagged dollar quotes ($body$...$body$) are handled correctly."""
    sql = "CREATE FUNCTION g() RETURNS INT LANGUAGE plpgsql AS $body$ BEGIN RETURN 1; END $body$;"
    stmts = _split_statements(sql)
    assert len(stmts) == 1


def test_split_statements_multiple_after_dollar_quote():
    """Statements after a dollar-quoted block are split correctly."""
    sql = "CREATE FUNCTION f() RETURNS INT AS $$ SELECT 1; $$; SELECT 2;"
    stmts = _split_statements(sql)
    assert len(stmts) == 2


def test_split_statements_unterminated_single_line_comment():
    """A SQL string ending in a -- comment without a trailing newline is handled."""
    sql = "SELECT 1 -- no newline at end"
    stmts = _split_statements(sql)
    assert len(stmts) == 1
    assert "SELECT 1" in stmts[0]


def test_split_statements_unterminated_block_comment():
    """A SQL string with an unterminated /* block comment is handled without error."""
    sql = "SELECT 1 /* oops, never closed"
    stmts = _split_statements(sql)
    assert len(stmts) == 1
    assert "SELECT 1" in stmts[0]


def test_split_statements_escaped_single_quote_in_string():
    """An escaped single-quote ('') inside a string is not treated as a terminator."""
    sql = "INSERT INTO t VALUES ('it''s fine'); SELECT 1;"
    stmts = _split_statements(sql)
    assert len(stmts) == 2
    assert "it''s fine" in stmts[0]
