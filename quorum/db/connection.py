"""
quorum/db/connection.py
-------------------------
PostgreSQL connection pool management and lifecycle for Quorum authoritative state.
Fails closed with dedicated exceptions if PostgreSQL is unavailable.
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Optional

import asyncpg

from config.settings import settings

logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Base exception for authoritative database operations."""
    pass


class DatabaseUnavailableError(DatabaseError):
    """Raised when PostgreSQL connection cannot be established or database is unreachable.
    Preserves fail-closed invariants: no authoritative claim, no external work.
    """
    pass


_pool: Optional[asyncpg.Pool] = None
_pool_loop: Optional[asyncio.AbstractEventLoop] = None
_pool_lock: Optional[asyncio.Lock] = None
_pool_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_pool_lock() -> asyncio.Lock:
    global _pool_lock, _pool_lock_loop
    loop = asyncio.get_running_loop()
    if _pool_lock is None or _pool_lock_loop is not loop:
        _pool_lock = asyncio.Lock()
        _pool_lock_loop = loop
    return _pool_lock


async def init_pool(
    dsn: Optional[str] = None,
    min_size: int = 2,
    max_size: int = 10,
    timeout: float = 5.0,
) -> asyncpg.Pool:
    """Initialize the global asyncpg connection pool.

    Parameters
    ----------
    dsn:
        PostgreSQL connection DSN. Defaults to settings.POSTGRES_URL.
    min_size:
        Minimum connections in pool.
    max_size:
        Maximum connections in pool.
    timeout:
        Connection attempt timeout in seconds.

    Returns
    -------
    asyncpg.Pool
        The initialized connection pool.

    Raises
    ------
    DatabaseUnavailableError
        If connection cannot be established.
    """
    global _pool, _pool_loop
    target_dsn = dsn or settings.POSTGRES_URL

    if not target_dsn:
        raise DatabaseUnavailableError(
            "Cannot initialize database pool: Neither POSTGRES_URL setting nor explicit DSN was provided."
        )

    loop = asyncio.get_running_loop()
    lock = _get_pool_lock()

    async with lock:
        if _pool is not None and not _pool._closed and _pool_loop is loop:
            if dsn is None:
                return _pool
            try:
                await _pool.close()
            except Exception:
                pass
            _pool = None
            _pool_loop = None

        if _pool is not None and (_pool._closed or _pool_loop is not loop):
            _pool = None
            _pool_loop = None

        try:
            _pool = await asyncio.wait_for(
                asyncpg.create_pool(
                    dsn=target_dsn,
                    min_size=min_size,
                    max_size=max_size,
                    command_timeout=60.0,
                ),
                timeout=timeout,
            )
            _pool_loop = loop
            logger.info("Authoritative PostgreSQL connection pool initialized.")
            return _pool
        except Exception as exc:
            logger.error(f"Failed to connect to PostgreSQL: {exc}")
            raise DatabaseUnavailableError(f"PostgreSQL unavailable: {exc}") from exc


async def close_pool() -> None:
    """Close the global connection pool gracefully."""
    global _pool, _pool_loop
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None
        _pool_loop = None
        logger.info("PostgreSQL connection pool closed.")


def get_pool() -> Optional[asyncpg.Pool]:
    """Return the active global pool if it belongs to the current event loop."""
    global _pool, _pool_loop
    try:
        loop = asyncio.get_running_loop()
        if _pool is not None and not _pool._closed and _pool_loop is loop:
            return _pool
    except RuntimeError:
        pass
    return None


@asynccontextmanager
async def get_connection(
    pool: Optional[asyncpg.Pool] = None,
    dsn: Optional[str] = None,
    timeout: float = 5.0,
) -> AsyncIterator[asyncpg.Connection]:
    """Async context manager that yields a single PostgreSQL connection.

    If a pool exists, acquires a connection from the pool.
    If no pool exists but DSN is supplied, opens an isolated direct connection.
    Fails closed by raising DatabaseUnavailableError.

    Yields
    ------
    asyncpg.Connection
    """
    if pool is not None and not pool._closed:
        try:
            conn = await asyncio.wait_for(pool.acquire(), timeout=timeout)
        except Exception as exc:
            raise DatabaseUnavailableError(f"Database connection error from pool: {exc}") from exc
        try:
            yield conn
        finally:
            await pool.release(conn)
    elif dsn is not None:
        try:
            conn = await asyncio.wait_for(asyncpg.connect(dsn), timeout=timeout)
        except Exception as exc:
            raise DatabaseUnavailableError(f"Direct PostgreSQL connection failed: {exc}") from exc
        try:
            yield conn
        finally:
            await conn.close()
    else:
        active_pool = get_pool()
        if active_pool is not None and not active_pool._closed:
            try:
                conn = await asyncio.wait_for(active_pool.acquire(), timeout=timeout)
            except Exception as exc:
                raise DatabaseUnavailableError(f"Database connection error from pool: {exc}") from exc
            try:
                yield conn
            finally:
                await active_pool.release(conn)
        else:
            target_dsn = settings.POSTGRES_URL
            if not target_dsn:
                raise DatabaseUnavailableError("PostgreSQL unavailable: No pool initialized and no DSN configured.")
            try:
                conn = await asyncio.wait_for(asyncpg.connect(target_dsn), timeout=timeout)
            except Exception as exc:
                raise DatabaseUnavailableError(f"Direct PostgreSQL connection failed: {exc}") from exc
            try:
                yield conn
            finally:
                await conn.close()
