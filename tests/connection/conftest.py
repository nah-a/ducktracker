"""Connection test helpers."""

from __future__ import annotations

from unittest.mock import MagicMock


def recorded_conn():
    """Returns (conn_mock, executed_list) for tracking SQL calls."""
    executed = []
    conn = MagicMock()
    conn.execute.side_effect = executed.append
    return conn, executed
