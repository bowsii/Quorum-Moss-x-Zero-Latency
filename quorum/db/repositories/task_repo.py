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
        for_update: bool = False,
        nowait: bool = False,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a task by ID, optionally acquiring an exclusive row lock."""
        from db.transaction import get_current_connection

        if for_update and conn is None and get_current_connection() is None:
            raise ValueError(
                "Row-level locking (for_update=True) requires an active transaction context. "
                "Call within 'async with transaction():' or pass an active transaction connection."
            )
        lock_clause = ""
        if for_update:
            lock_clause = "FOR UPDATE NOWAIT" if nowait else "FOR UPDATE"

        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                f"""
                SELECT id, run_id, task_key, description, status, metadata, created_at, updated_at
                FROM tasks WHERE id = $1 {lock_clause};
                """,
                task_id,
            )
            return dict(row) if row else None

    async def lock_task(
        self,
        task_id: str,
        nowait: bool = False,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Lock an existing task row with SELECT ... FOR UPDATE."""
        return await self.get_task(task_id=task_id, for_update=True, nowait=nowait, conn=conn)

    async def get_or_create_task(
        self,
        run_id: str,
        task_key: str,
        description: str = "",
        task_id: Optional[str] = None,
        status: str = "queued",
        metadata: Optional[dict[str, Any]] = None,
        for_update: bool = False,
        nowait: bool = False,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Atomically create or find a task identified by (run_id, task_key).

        Uses INSERT ... ON CONFLICT (run_id, task_key) DO UPDATE to guarantee
        that concurrent attempts converge on the exact same task row.
        If for_update=True, acquires an exclusive row lock within the transaction.
        """
        import uuid
        eff_task_id = task_id or str(uuid.uuid4())
        meta_json = json.dumps(metadata or {})

        async with resolve_connection(conn) as c:
            # 0. Ensure parent run exists
            await c.execute(
                """
                INSERT INTO runs (id, question, status, created_at, updated_at)
                VALUES ($1, 'Run for ' || $1, 'running', NOW(), NOW())
                ON CONFLICT (id) DO NOTHING;
                """,
                run_id,
            )

            # 1. Atomic upsert to ensure row exists
            row = await c.fetchrow(
                """
                INSERT INTO tasks (id, run_id, task_key, description, status, metadata, created_at, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW(), NOW())
                ON CONFLICT (run_id, task_key) DO UPDATE
                SET updated_at = tasks.updated_at
                RETURNING id, run_id, task_key, description, status, metadata, created_at, updated_at;
                """,
                eff_task_id,
                run_id,
                task_key,
                description,
                status,
                meta_json,
            )
            task_dict = dict(row)

            # 2. If row lock requested, lock explicitly
            if for_update:
                locked = await self.lock_task(task_dict["id"], nowait=nowait, conn=c)
                if locked:
                    return locked

            return task_dict

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
