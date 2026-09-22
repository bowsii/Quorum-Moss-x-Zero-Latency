"""
quorum/db/repositories/worker_repo.py
---------------------------------------
Authoritative persistence for Worker identities and heartbeats.
"""

from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection


class WorkerRepository:
    """Repository for managing worker registrations and heartbeat states in PostgreSQL."""

    async def register_worker(
        self,
        worker_id: str,
        run_id: Optional[str] = None,
        worker_type: str = "agent",
        status: str = "ready",
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Register or update a worker process identity."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO workers (id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at)
                VALUES ($1, $2, $3, $4, NOW(), NOW(), NOW())
                ON CONFLICT (id) DO UPDATE
                SET run_id = EXCLUDED.run_id,
                    status = EXCLUDED.status,
                    last_heartbeat_at = NOW(),
                    updated_at = NOW()
                RETURNING id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at;
                """,
                worker_id,
                run_id,
                worker_type,
                status,
            )
            return dict(row)

    async def heartbeat_worker(
        self,
        worker_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update last_heartbeat_at timestamp for a worker."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE workers
                SET last_heartbeat_at = NOW(), updated_at = NOW()
                WHERE id = $1
                RETURNING id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at;
                """,
                worker_id,
            )
            return dict(row) if row else None

    async def update_worker_status(
        self,
        worker_id: str,
        status: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update worker state (starting, ready, busy, draining, stopped, failed)."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE workers
                SET status = $2, updated_at = NOW()
                WHERE id = $1
                RETURNING id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at;
                """,
                worker_id,
                status,
            )
            return dict(row) if row else None

    async def get_worker(
        self,
        worker_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a worker record by ID."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at
                FROM workers WHERE id = $1;
                """,
                worker_id,
            )
            return dict(row) if row else None

    async def list_stale_workers(
        self,
        ttl_seconds: float = 15.0,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List active workers that have not heartbeated within ttl_seconds."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                """
                SELECT id, run_id, worker_type, status, last_heartbeat_at, created_at, updated_at
                FROM workers
                WHERE status IN ('ready', 'busy', 'draining')
                  AND last_heartbeat_at < (NOW() - ($1 || ' seconds')::interval);
                """,
                ttl_seconds,
            )
            return [dict(r) for r in rows]
