"""
quorum/db/repositories/claim_repo.py
--------------------------------------
Authoritative persistence for Claim entities and lease metadata.
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Optional
import asyncpg

from db.repositories.base import resolve_connection
from db.transaction import get_current_connection
from config.settings import settings


class ClaimRepository:
    """Repository for managing authoritative claim records and leases in PostgreSQL."""

    async def create_claim(
        self,
        claim_id: str,
        task_id: str,
        run_id: str,
        owner_id: str,
        owner_type: str = "agent",
        status: str = "active",
        lease_id: Optional[str] = None,
        lease_version: int = 1,
        expires_at: Optional[datetime] = None,
        created_at: Optional[datetime] = None,
        conn: Optional[asyncpg.Connection] = None,
    ) -> dict[str, Any]:
        """Create a new authoritative claim record with lease metadata."""
        effective_lease_id = lease_id or f"lease-{claim_id}-v{lease_version}"
        effective_expires_at = expires_at or (
            datetime.now(timezone.utc) + timedelta(seconds=float(settings.HEARTBEAT_TTL_SECONDS))
        )
        effective_created_at = created_at or datetime.now(timezone.utc)

        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                INSERT INTO claims (
                    id, task_id, run_id, owner_id, owner_type, status,
                    lease_id, lease_version, expires_at, created_at, updated_at
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW())
                RETURNING id, task_id, run_id, owner_id, owner_type, status,
                          lease_id, lease_version, expires_at, created_at, updated_at;
                """,
                claim_id,
                task_id,
                run_id,
                owner_id,
                owner_type,
                status,
                effective_lease_id,
                lease_version,
                effective_expires_at,
                effective_created_at,
            )
            return dict(row)

    async def get_claim(
        self,
        claim_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve a claim record by ID."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, task_id, run_id, owner_id, owner_type, status,
                       lease_id, lease_version, expires_at, created_at, updated_at
                FROM claims WHERE id = $1;
                """,
                claim_id,
            )
            return dict(row) if row else None

    async def get_active_claim_for_task(
        self,
        task_id: str,
        for_update: bool = False,
        include_expired: bool = False,
        nowait: bool = False,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve the currently active claim for a task.
        Optionally acquires row-level exclusive lock (FOR UPDATE).
        """
        if for_update and conn is None and get_current_connection() is None:
            raise ValueError(
                "Row-level locking (for_update=True) requires an active transaction context. "
                "Call within 'async with transaction():' or pass an active transaction connection."
            )
        lock_clause = ""
        if for_update:
            lock_clause = "FOR UPDATE NOWAIT" if nowait else "FOR UPDATE"

        expiry_clause = "" if include_expired else "AND expires_at > NOW()"

        async with resolve_connection(conn) as c:
            query = f"""
                SELECT id, task_id, run_id, owner_id, owner_type, status,
                       lease_id, lease_version, expires_at, created_at, updated_at
                FROM claims
                WHERE task_id = $1 AND status = 'active' {expiry_clause}
                LIMIT 1 {lock_clause};
            """
            row = await c.fetchrow(query, task_id)
            return dict(row) if row else None

    async def get_latest_claim_for_task(
        self,
        task_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Retrieve the latest claim record for a task regardless of status."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                SELECT id, task_id, run_id, owner_id, owner_type, status,
                       lease_id, lease_version, expires_at, created_at, updated_at
                FROM claims
                WHERE task_id = $1
                ORDER BY lease_version DESC, created_at DESC
                LIMIT 1;
                """,
                task_id,
            )
            return dict(row) if row else None

    async def supersede_claim(
        self,
        claim_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Mark an active claim as superseded."""
        return await self.update_claim_status(claim_id=claim_id, status="superseded", conn=conn)

    async def expire_claim(
        self,
        claim_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Mark an active claim as expired."""
        return await self.update_claim_status(claim_id=claim_id, status="expired", conn=conn)

    async def update_claim_status(
        self,
        claim_id: str,
        status: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update claim status (e.g. active -> superseded, completed, expired)."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE claims
                SET status = $2, updated_at = NOW()
                WHERE id = $1
                RETURNING id, task_id, run_id, owner_id, owner_type, status,
                          lease_id, lease_version, expires_at, created_at, updated_at;
                """,
                claim_id,
                status,
            )
            return dict(row) if row else None

    async def update_claim_lease(
        self,
        claim_id: str,
        lease_id: str,
        lease_version: int,
        expires_at: datetime,
        conn: Optional[asyncpg.Connection] = None,
    ) -> Optional[dict[str, Any]]:
        """Update lease version and expiration for an active claim."""
        async with resolve_connection(conn) as c:
            row = await c.fetchrow(
                """
                UPDATE claims
                SET lease_id = $2, lease_version = $3, expires_at = $4, updated_at = NOW()
                WHERE id = $1
                RETURNING id, task_id, run_id, owner_id, owner_type, status,
                          lease_id, lease_version, expires_at, created_at, updated_at;
                """,
                claim_id,
                lease_id,
                lease_version,
                expires_at,
            )
            return dict(row) if row else None

    async def list_active_claims_for_run(
        self,
        run_id: str,
        conn: Optional[asyncpg.Connection] = None,
    ) -> list[dict[str, Any]]:
        """List all active, unexpired claims for a run."""
        async with resolve_connection(conn) as c:
            rows = await c.fetch(
                """
                SELECT id, task_id, run_id, owner_id, owner_type, status,
                       lease_id, lease_version, expires_at, created_at, updated_at
                FROM claims
                WHERE run_id = $1 AND status = 'active' AND expires_at > NOW();
                """,
                run_id,
            )
            return [dict(r) for r in rows]
