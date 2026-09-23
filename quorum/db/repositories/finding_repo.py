"""
quorum/db/repositories/finding_repo.py
----------------------------------------
Authoritative persistence for Finding metadata.
Semantic representation remains indexed in ChromaDB.
"""

import hashlib
import json
from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection


class FindingRepository:
    """Repository for managing authoritative finding metadata in PostgreSQL."""

    async def create_finding(
        self,
        finding_id: str,
        run_id: str,
        participant_id: str,
        participant_type: str,
        title: str,
        content_hash: Optional[str] = None,
        content: Optional[str] = None,
        task_id: Optional[str] = None,
        claim_id: Optional[str] = None,
        sources: Optional[list[str]] = None,
        supersedes: Optional[str] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Record authoritative finding metadata."""
        effective_hash = content_hash or (
            hashlib.sha256((content or "").encode("utf-8")).hexdigest()
        )
        sources_json = json.dumps(sources or [])

        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO findings (
                    id, task_id, run_id, claim_id, participant_id, participant_type,
                    title, content_hash, sources, supersedes, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10, NOW(), NOW())
                RETURNING id, task_id, run_id, claim_id, participant_id, participant_type,
                          title, content_hash, sources, supersedes, created_at, updated_at;
                """,
                finding_id,
                task_id,
                run_id,
                claim_id,
                participant_id,
                participant_type,
                title,
                effective_hash,
                sources_json,
                supersedes,
            )
            return dict(row)

    async def get_finding(
        self,
        finding_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve finding metadata by ID."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, task_id, run_id, claim_id, participant_id, participant_type,
                       title, content_hash, sources, supersedes, created_at, updated_at
                FROM findings WHERE id = $1;
                """,
                finding_id,
            )
            return dict(row) if row else None

    async def list_findings_by_run(
        self,
        run_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List finding metadata for a specific run."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                """
                SELECT id, task_id, run_id, claim_id, participant_id, participant_type,
                       title, content_hash, sources, supersedes, created_at, updated_at
                FROM findings WHERE run_id = $1 ORDER BY created_at ASC;
                """,
                run_id,
            )
            return [dict(r) for r in rows]

    async def get_completed_finding_for_task(
        self,
        task_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve any authoritative finding recorded for a task."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, task_id, run_id, claim_id, participant_id, participant_type,
                       title, content_hash, sources, supersedes, created_at, updated_at
                FROM findings WHERE task_id = $1 LIMIT 1;
                """,
                task_id,
            )
            return dict(row) if row else None
