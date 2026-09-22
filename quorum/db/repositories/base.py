"""
quorum/db/repositories/base.py
--------------------------------
Base helper for repository connection resolution.
"""

from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional
import asyncpg

from db.connection import get_connection, DatabaseError
from db.transaction import get_current_connection


@asynccontextmanager
async def resolve_connection(conn: Optional[asyncpg.Connection] = None) -> AsyncIterator[asyncpg.Connection]:
    """Context manager yielding either the explicit/ambient connection or a pool connection.

    Ensures deterministic connection handling whether invoked within an active
    transaction context or standalone.
    """
    existing = conn or get_current_connection()
    if existing is not None:
        yield existing
    else:
        async with get_connection() as acquired:
            yield acquired
