"""
quorum/db/repositories/idempotency_repo.py
--------------------------------------------
Authoritative persistence for idempotency records.
"""

from datetime import datetime, timezone, timedelta
import json
from typing import Any, Optional
import uuid
import asyncpg

from db.repositories.base import resolve_connection


class IdempotencyRepository:
    """Repository for managing idempotency tokens and execution results in PostgreSQL."""

    async def get_record(
        self,
        idempotency_key: str,
        scope: str = "default",
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve an idempotency record by key and scope."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, idempotency_key, scope, status, payload, expires_at, created_at, updated_at
                FROM idempotency_records
                WHERE idempotency_key = $1 AND scope = $2;
                """,
                idempotency_key,
                scope,
            )
            return dict(row) if row else None

    async def create_record(
        self,
        idempotency_key: str,
        scope: str = "default",
        status: str = "in_progress",
        payload: Optional[dict[str, Any]] = None,
        record_id: Optional[str] = None,
        ttl_seconds: float = 3600.0,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Create a new idempotency token."""
        effective_id = record_id or str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)
        payload_json = json.dumps(payload) if payload is not None else None

        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO idempotency_records (
                    id, idempotency_key, scope, status, payload, expires_at, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, NOW(), NOW())
                RETURNING id, idempotency_key, scope, status, payload, expires_at, created_at, updated_at;
                """,
                effective_id,
                idempotency_key,
                scope,
                status,
                payload_json,
                expires_at,
            )
            return dict(row)

    async def update_record(
        self,
        idempotency_key: str,
        status: str,
        scope: str = "default",
        payload: Optional[dict[str, Any]] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update an idempotency token with final status and payload."""
        payload_json = json.dumps(payload) if payload is not None else None
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE idempotency_records
                SET status = $3, payload = $4::jsonb, updated_at = NOW()
                WHERE idempotency_key = $1 AND scope = $2
                RETURNING id, idempotency_key, scope, status, payload, expires_at, created_at, updated_at;
                """,
                idempotency_key,
                scope,
                status,
                payload_json,
            )
            return dict(row) if row else None
