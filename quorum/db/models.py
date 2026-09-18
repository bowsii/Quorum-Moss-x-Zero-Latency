"""
quorum/db/models.py
--------------------
SQLite schema definitions and async database helpers built on *aiosqlite*.

Tables
------
* runs               – top-level debate / deliberation run records
* consent_events     – participant consent audit trail per run
* replay_logs        – ordered action log used for session replay
* telemetry_tombstones – span IDs that must be suppressed from telemetry
                         after a run is erased (right-to-erasure support)
"""

import aiosqlite
from contextlib import asynccontextmanager
from typing import AsyncIterator

# ---------------------------------------------------------------------------
# DDL statements
# ---------------------------------------------------------------------------

_CREATE_RUNS = """
CREATE TABLE IF NOT EXISTS runs (
    id          TEXT PRIMARY KEY,
    question    TEXT NOT NULL,
    started_at  TEXT NOT NULL,
    ended_at    TEXT,
    status      TEXT NOT NULL
);
"""

_CREATE_CONSENT_EVENTS = """
CREATE TABLE IF NOT EXISTS consent_events (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT    NOT NULL REFERENCES runs(id),
    event_type     TEXT    NOT NULL,
    timestamp      TEXT    NOT NULL,
    participant_id TEXT    NOT NULL
);
"""

_CREATE_REPLAY_LOGS = """
CREATE TABLE IF NOT EXISTS replay_logs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id         TEXT    NOT NULL REFERENCES runs(id),
    step_index     INTEGER NOT NULL,
    participant_id TEXT    NOT NULL,
    action         TEXT    NOT NULL,
    payload        TEXT    NOT NULL,   -- JSON-encoded blob
    timestamp      TEXT    NOT NULL
);
"""

_CREATE_TELEMETRY_TOMBSTONES = """
CREATE TABLE IF NOT EXISTS telemetry_tombstones (
    span_id        TEXT PRIMARY KEY,
    run_id         TEXT NOT NULL,
    tombstoned_at  TEXT NOT NULL
);
"""

_ALL_DDL: list[str] = [
    _CREATE_RUNS,
    _CREATE_CONSENT_EVENTS,
    _CREATE_REPLAY_LOGS,
    _CREATE_TELEMETRY_TOMBSTONES,
]


# ---------------------------------------------------------------------------
# Context manager — yields a connected, WAL-mode aiosqlite connection
# ---------------------------------------------------------------------------

@asynccontextmanager
async def get_db(db_path: str = "quorum.db") -> AsyncIterator[aiosqlite.Connection]:
    """Async context manager that opens an aiosqlite connection.

    Enables WAL journal mode and row_factory so rows are accessible by column
    name.  The connection is committed and closed automatically on exit; any
    exception triggers a rollback.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file (relative or absolute).

    Yields
    ------
    aiosqlite.Connection
        An open, configured database connection.

    Example
    -------
    ::

        async with get_db() as db:
            cursor = await db.execute("SELECT * FROM runs")
            rows = await cursor.fetchall()
    """
    db: aiosqlite.Connection = await aiosqlite.connect(db_path)
    db.row_factory = aiosqlite.Row
    try:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute("PRAGMA foreign_keys=ON;")
        yield db
        await db.commit()
    except Exception:
        await db.rollback()
        raise
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Schema initialisation
# ---------------------------------------------------------------------------

async def init_db(db_path: str = "quorum.db") -> None:
    """Create all Quorum tables if they do not already exist.

    Safe to call at application startup on every process boot — all statements
    use ``CREATE TABLE IF NOT EXISTS``.

    Parameters
    ----------
    db_path:
        Path to the SQLite database file.  Defaults to ``quorum.db`` in the
        current working directory.
    """
    async with get_db(db_path) as db:
        for ddl in _ALL_DDL:
            await db.execute(ddl)
