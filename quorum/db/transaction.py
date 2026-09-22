"""
quorum/db/transaction.py
--------------------------
Transaction abstraction for Quorum authoritative state operations.
Provides clean context-managed transactions with isolation configuration,
automatic commit/rollback, and ambient connection propagation via contextvars.
"""

import contextvars
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import asyncpg

from db.connection import get_connection, DatabaseError

logger = logging.getLogger(__name__)

# Context variable holding the currently active transaction connection in this asyncio task
_ambient_conn: contextvars.ContextVar[Optional[asyncpg.Connection]] = contextvars.ContextVar(
    "_ambient_conn", default=None
)


def get_current_connection() -> Optional[asyncpg.Connection]:
    """Retrieve the ambient transaction connection for the current task, if any."""
    return _ambient_conn.get()


@asynccontextmanager
async def transaction(
    conn: Optional[asyncpg.Connection] = None,
    isolation: str = "read_committed",
    pool: Optional[asyncpg.Pool] = None,
) -> AsyncIterator[asyncpg.Connection]:
    """Async context manager for an authoritative database transaction.

    Parameters
    ----------
    conn:
        Optional existing connection. If None, resolves from ambient context or acquires from pool.
    isolation:
        Transaction isolation level: 'read_committed', 'repeatable_read', or 'serializable'.
    pool:
        Optional asyncpg.Pool to acquire from if conn is not provided.

    Yields
    ------
    asyncpg.Connection
        The connection with the active transaction.

    Example
    -------
    ::

        async with transaction(isolation="read_committed") as tx_conn:
            await task_repo.create_task(...)
            await claim_repo.create_claim(...)
            # auto-commits on normal exit, auto-rolls back on exception
    """
    existing_conn = conn or get_current_connection()

    if existing_conn is not None:
        # Reusing existing connection (nested transaction / savepoint)
        async with existing_conn.transaction():
            yield existing_conn
    else:
        # Acquire fresh connection and begin transaction
        async with get_connection(pool=pool) as acquired_conn:
            token = _ambient_conn.set(acquired_conn)
            try:
                async with acquired_conn.transaction(isolation=isolation):
                    yield acquired_conn
            finally:
                _ambient_conn.reset(token)
