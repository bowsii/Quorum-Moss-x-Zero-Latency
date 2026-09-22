"""
quorum/db/repositories/audit_repo.py
--------------------------------------
Authoritative append-only audit event log.
Exposes only record_event and list_events_by_run (no update or delete operations).
"""

import json
from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection


class AuditRepository:
    """Repository for recording authoritative coordination lifecycle events."""

    async def record_event(
        self,
        event_type: str,
        actor_id: str,
        run_id: Optional[str] = None,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        details: Optional[dict[str, Any]] = None,
        task_id: Optional[str] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Append an audit event record (strictly append-only)."""
        eff_entity_type = entity_type or ("task" if task_id else None)
        eff_entity_id = entity_id or task_id
        details_json = json.dumps(details or {})

        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO audit_events (run_id, actor_id, event_type, entity_type, entity_id, details, created_at)
                VALUES ($1, $2, $3, $4, $5, $6::jsonb, NOW())
                RETURNING id, run_id, actor_id, event_type, entity_type, entity_id, details, created_at;
                """,
                run_id,
                actor_id,
                event_type,
                eff_entity_type,
                eff_entity_id,
                details_json,
            )
            return dict(row)

    async def list_events_by_run(
        self,
        run_id: str,
        limit: int = 100,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List audit events for a run in append order."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                """
                SELECT id, run_id, actor_id, event_type, entity_type, entity_id, details, created_at
                FROM audit_events WHERE run_id = $1 ORDER BY id ASC LIMIT $2;
                """,
                run_id,
                limit,
            )
            return [dict(r) for r in rows]
