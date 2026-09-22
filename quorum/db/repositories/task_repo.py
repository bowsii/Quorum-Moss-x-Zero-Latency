"""
quorum/db/repositories/task_repo.py
-------------------------------------
Authoritative persistence for Task entities.
"""

import json
from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection


class TaskRepository:
    """Repository for managing authoritative task records in PostgreSQL."""

    async def create_task(
        self,
        task_id: str,
        run_id: str,
        task_key: str,
        description: str,
        status: str = "queued",
        metadata: Optional[dict[str, Any]] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Create a new task under a run."""
        meta_json = json.dumps(metadata or {})
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO tasks (id, run_id, task_key, description, status, metadata, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW(), NOW())
                RETURNING id, run_id, task_key, description, status, metadata, created_at, updated_at;
                """,
                task_id,
                run_id,
                task_key,
                description,
                status,
                meta_json,
            )
            return dict(row)

    async def get_task(
        self,
        task_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a task by ID."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, run_id, task_key, description, status, metadata, created_at, updated_at
                FROM tasks WHERE id = $1;
                """,
                task_id,
            )
            return dict(row) if row else None

    async def get_task_by_key(
        self,
        run_id: str,
        task_key: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a task by its canonical identity within a run."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, run_id, task_key, description, status, metadata, created_at, updated_at
                FROM tasks WHERE run_id = $1 AND task_key = $2;
                """,
                run_id,
                task_key,
            )
            return dict(row) if row else None

    async def update_task_status(
        self,
        task_id: str,
        status: str,
        metadata: Optional[dict[str, Any]] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update a task's status and optionally merge metadata."""
        async with resolve_connection(conn) as c:
            if metadata is not None:
                meta_json = json.dumps(metadata)
                row = await c.fetchrow(
                    """
                    UPDATE tasks
                    SET status = $2, metadata = metadata || $3::jsonb, updated_at = NOW()
                    WHERE id = $1
                    RETURNING id, run_id, task_key, description, status, metadata, created_at, updated_at;
                    """,
                    task_id,
                    status,
                    meta_json,
                )
            else:
                row = await c.fetchrow(
                    """
                    UPDATE tasks
                    SET status = $2, updated_at = NOW()
                    WHERE id = $1
                    RETURNING id, run_id, task_key, description, status, metadata, created_at, updated_at;
                    """,
                    task_id,
                    status,
                )
            return dict(row) if row else None

    async def list_tasks_by_run(
        self,
        run_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List all tasks belonging to a run."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                """
                SELECT id, run_id, task_key, description, status, metadata, created_at, updated_at
                FROM tasks WHERE run_id = $1 ORDER BY created_at ASC;
                """,
                run_id,
            )
            return [dict(r) for r in rows]
