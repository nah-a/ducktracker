"""Tests for the connect context manager and connect_in_memory."""

from __future__ import annotations

from unittest.mock import MagicMock
from unittest.mock import patch as mock_patch

from ducktracker.config import DuckTrackerConfig
from ducktracker.connection import connect, connect_in_memory


def test_connect_sets_secret_directory():
    conn_mock = MagicMock()
    calls = []
    conn_mock.execute.side_effect = lambda *args: calls.append(args)

    config = DuckTrackerConfig(secret_directory="/my/secrets")

    with mock_patch("duckdb.connect", return_value=conn_mock), \
         mock_patch("ducktracker.connection._setup_extensions"), \
         mock_patch("ducktracker.connection._attach_ducklake"):
        with connect(config):
            pass

    # SET secret_directory is now parameterized to avoid path injection
    assert any(
        args[0] == "SET secret_directory = ?" and args[1] == ["/my/secrets"]
        for args in calls
    )


def test_connect_skips_secret_directory_when_empty():
    conn_mock = MagicMock()
    executed = []
    conn_mock.execute.side_effect = executed.append

    config = DuckTrackerConfig()  # secret_directory defaults to ""

    with mock_patch("duckdb.connect", return_value=conn_mock), \
         mock_patch("ducktracker.connection._setup_extensions"), \
         mock_patch("ducktracker.connection._attach_ducklake"):
        with connect(config):
            pass

    assert not any("secret_directory" in s for s in executed)


def test_connect_in_memory_returns_usable_connection():
    """connect_in_memory returns a working in-memory DuckDB connection."""
    conn = connect_in_memory()
    try:
        result = conn.execute("SELECT 42").fetchone()
        assert result[0] == 42
    finally:
        conn.close()
