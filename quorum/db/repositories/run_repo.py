"""
quorum/db/repositories/run_repo.py
------------------------------------
Authoritative persistence for Run entities.
"""

import json
from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection


class RunRepository:
    """Repository for managing authoritative run records in PostgreSQL."""

    async def create_run(
        self,
        run_id: str,
        question: str,
        status: str = "running",
        metadata: Optional[dict[str, Any]] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Create a new run record."""
        meta_json = json.dumps(metadata or {})
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO runs (id, question, status, metadata, created_at, updated_at)
                VALUES ($1, $2, $3, $4::jsonb, NOW(), NOW())
                RETURNING id, question, status, metadata, created_at, updated_at;
                """,
                run_id,
                question,
                status,
                meta_json,
            )
            return dict(row)

    async def get_run(
        self,
        run_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a run record by ID."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                "SELECT id, question, status, metadata, created_at, updated_at FROM runs WHERE id = $1;",
                run_id,
            )
            return dict(row) if row else None

    async def update_run_status(
        self,
        run_id: str,
        status: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update a run's status."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE runs
                SET status = $2, updated_at = NOW()
                WHERE id = $1
                RETURNING id, question, status, metadata, created_at, updated_at;
                """,
                run_id,
                status,
            )
            return dict(row) if row else None

    async def list_runs(
        self,
        limit: int = 50,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List recent runs ordered by creation timestamp."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                "SELECT id, question, status, metadata, created_at, updated_at FROM runs ORDER BY created_at DESC LIMIT $1;",
                limit,
            )
            return [dict(r) for r in rows]
