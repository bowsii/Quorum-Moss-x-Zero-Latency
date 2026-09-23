"""
quorum/db/arbitration.py
--------------------------
Authoritative PostgreSQL-backed Claim Arbitration for Quorum (Stage 2 / Phase C).

Guarantees:
-----------
For N independent processes competing for the same authoritative task:
- owners == 1
- non_owners == N - 1
- new_capabilities_issued == 1
- external_work_authorizations == 1
- stale_successful_writes == 0

Correctness relies strictly on PostgreSQL row-level locks (SELECT ... FOR UPDATE)
and transactional semantics, with a partial unique constraint on active claims
serving as defense-in-depth.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from enum import Enum
from typing import Any, Optional

import asyncpg

from config.settings import settings
from db.connection import get_connection, is_database_available, DatabaseUnavailableError, DatabaseError
from db.transaction import transaction, get_current_connection
from db.repositories import (
    task_repo,
    claim_repo,
    finding_repo,
    idempotency_repo,
    audit_repo,
)
from bus.schemas import Claim as BusClaim
from bus.tiebreak import should_claim_win, _EPSILON_SECONDS

logger = logging.getLogger(__name__)


class ArbitrationOutcome(str, Enum):
    """Categorical outcomes of authoritative claim arbitration."""

    OWNER_NEW = "owner_new"
    OWNER_ALREADY_HELD = "owner_already_held"
    DUPLICATE_ABSORBED = "duplicate_absorbed"
    EXPIRED_RECLAIMED = "expired_reclaimed"
    TIEBREAK_WON = "tiebreak_won"
    TIEBREAK_LOST = "tiebreak_lost"
    COMPLETED_FINDING_ABSORBED = "completed_finding_absorbed"


@dataclass(frozen=True)
class AuthoritativeClaimDecision:
    """Authoritative claim decision produced by PostgreSQL transaction arbitration."""

    is_owner: bool
    outcome: ArbitrationOutcome

    task_id: str
    task_key: str
    run_id: str

    claim_id: str | None
    lease_id: str | None
    lease_version: int | None
    expires_at: datetime | None

    existing_claim_id: str | None
    existing_finding_id: str | None
    superseded_claim_id: str | None

    capability_issuance_required: bool

    reason: str


async def claim_task_authoritatively(
    task_key: str,
    run_id: str,
    owner_id: str,
    owner_type: str = "agent",
    description: str = "",
    task_id: Optional[str] = None,
    idempotency_key: Optional[str] = None,
    claim_ttl_seconds: Optional[float] = None,
    challenger_created_at: Optional[datetime] = None,
    conn: Optional[asyncpg.Connection] = None,
    pool: Optional[asyncpg.Pool] = None,
    dsn: Optional[str] = None,
) -> AuthoritativeClaimDecision:
    """Arbitrate task ownership authoritatively via PostgreSQL.

    Executes within an isolated PostgreSQL transaction:
    1. Verifies PostgreSQL availability (fails closed if unavailable).
    2. Atomically creates or acquires the authoritative task row.
    3. Locks the task row with SELECT ... FOR UPDATE, serializing all concurrent contenders.
    4. Inspects authoritative completed findings; absorbs if task is already resolved.
    5. Inspects authoritative active claim.
    6. If no active claim: grants ownership (OWNER_NEW).
    7. If active claim is expired: reclaims lease (EXPIRED_RECLAIMED).
    8. If active claim is healthy:
       a. If same owner & same logical request: idempotently returns (OWNER_ALREADY_HELD).
       b. If outside 50ms race window: absorbs late contender (DUPLICATE_ABSORBED).
       c. If within 50ms race window: resolves tiebreak via Stage 1 policy (TIEBREAK_WON / TIEBREAK_LOST).
    9. Records append-only audit events.
    10. Commits the transaction and returns a structured decision.

    Parameters
    ----------
    task_key:
        Canonical natural identity of the task within the run.
    run_id:
        Identifier of the parent run.
    owner_id:
        Identifier of the contender (agent, human, adjudicator).
    owner_type:
        'agent', 'human', 'adjudicator', or 'reaper'.
    description:
        Task description or prompt.
    task_id:
        Optional predetermined task ID. If None, generated deterministically or via UUID.
    idempotency_key:
        Optional caller-supplied idempotency key.
    claim_ttl_seconds:
        Lease TTL in seconds. Defaults to settings.HEARTBEAT_TTL_SECONDS.
    challenger_created_at:
        Timestamp when challenger created its claim intent (used for 50ms race tiebreak).
    conn:
        Optional existing asyncpg connection.
    pool:
        Optional asyncpg pool.
    dsn:
        Optional PostgreSQL connection string.

    Returns
    -------
    AuthoritativeClaimDecision
        Immutable decision containing ownership flag and capability requirement.
    """
    # 1. Verify PostgreSQL availability (fail-closed if unavailable)
    if not is_database_available() and conn is None and get_current_connection() is None and dsn is None:
        raise DatabaseUnavailableError("PostgreSQL is unavailable. Cannot arbitrate claim ownership (fail-closed).")

    ttl = claim_ttl_seconds if claim_ttl_seconds is not None else float(settings.HEARTBEAT_TTL_SECONDS)

    # 2. Enter PostgreSQL transaction
    async with transaction(conn=conn, pool=pool, dsn=dsn) as tx_conn:
        # Ensure parent run exists
        await tx_conn.execute(
            """
            INSERT INTO runs (id, question, status, created_at, updated_at)
            VALUES ($1, $2, 'running', NOW(), NOW())
            ON CONFLICT (id) DO NOTHING;
            """,
            run_id,
            description or f"Run {run_id}",
        )

        # 3. Atomically upsert and lock task row with FOR UPDATE
        task = await task_repo.get_or_create_task(
            run_id=run_id,
            task_key=task_key,
            description=description,
            task_id=task_id,
            for_update=True,
            conn=tx_conn,
        )
        eff_task_id = task["id"]

        # Obtain authoritative database clock timestamp after acquiring the task row lock
        db_now = await tx_conn.fetchval("SELECT clock_timestamp();")
        if db_now.tzinfo is None:
            db_now = db_now.replace(tzinfo=timezone.utc)
        now_utc = db_now

        c_time = challenger_created_at or db_now
        if c_time.tzinfo is None:
            c_time = c_time.replace(tzinfo=timezone.utc)

        # 4. Inspect authoritative findings
        finding = await finding_repo.get_completed_finding_for_task(eff_task_id, conn=tx_conn)
        if task.get("status") == "completed" or finding is not None:
            finding_id = finding["id"] if finding else None
            logger.info("Task %s has completed finding %s; absorbing contender %s", eff_task_id, finding_id, owner_id)
            return AuthoritativeClaimDecision(
                is_owner=False,
                outcome=ArbitrationOutcome.COMPLETED_FINDING_ABSORBED,
                task_id=eff_task_id,
                task_key=task_key,
                run_id=run_id,
                claim_id=None,
                lease_id=None,
                lease_version=None,
                expires_at=None,
                existing_claim_id=None,
                existing_finding_id=finding_id,
                superseded_claim_id=None,
                capability_issuance_required=False,
                reason="completed_finding_absorbed",
            )

        # 5. Inspect authoritative active claim (including expired active claims)
        active_claim = await claim_repo.get_active_claim_for_task(
            eff_task_id,
            for_update=True,
            include_expired=True,
            conn=tx_conn,
        )

        # ------------------------------------------------------------------
        # Case A: No active claim exists
        # ------------------------------------------------------------------
        if active_claim is None:
            latest = await claim_repo.get_latest_claim_for_task(eff_task_id, conn=tx_conn)
            lease_version = (latest["lease_version"] + 1) if latest else 1
            new_claim_id = str(uuid.uuid4())
            new_lease_id = f"lease-{new_claim_id}-v{lease_version}"
            expires_at = c_time + timedelta(seconds=ttl)

            # Insert new active claim inside savepoint for defense-in-depth against unique conflict
            try:
                async with tx_conn.transaction():
                    await claim_repo.create_claim(
                        claim_id=new_claim_id,
                        task_id=eff_task_id,
                        run_id=run_id,
                        owner_id=owner_id,
                        owner_type=owner_type,
                        status="active",
                        lease_id=new_lease_id,
                        lease_version=lease_version,
                        expires_at=expires_at,
                        created_at=c_time,
                        conn=tx_conn,
                    )
            except asyncpg.UniqueViolationError:
                # Concurrent active claim was committed; resolve as absorbed duplicate
                existing = await claim_repo.get_active_claim_for_task(eff_task_id, conn=tx_conn)
                return AuthoritativeClaimDecision(
                    is_owner=False,
                    outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=None,
                    lease_id=None,
                    lease_version=None,
                    expires_at=None,
                    existing_claim_id=existing["id"] if existing else None,
                    existing_finding_id=None,
                    superseded_claim_id=None,
                    capability_issuance_required=False,
                    reason="unique_constraint_conflict_absorbed",
                )

            # Update task status to running
            await task_repo.update_task_status(eff_task_id, status="running", conn=tx_conn)

            # Record append-only audit event
            await audit_repo.record_event(
                event_type="claim_granted",
                actor_id=owner_id,
                run_id=run_id,
                entity_type="claim",
                entity_id=new_claim_id,
                task_id=eff_task_id,
                details={
                    "task_key": task_key,
                    "outcome": ArbitrationOutcome.OWNER_NEW.value,
                    "lease_version": lease_version,
                    "expires_at": expires_at.isoformat(),
                },
                conn=tx_conn,
            )

            # Record idempotency record if requested
            if idempotency_key:
                await idempotency_repo.create_record(
                    idempotency_key=idempotency_key,
                    scope=f"claim:{eff_task_id}",
                    status="completed",
                    payload={"claim_id": new_claim_id, "owner_id": owner_id},
                    ttl_seconds=ttl,
                    conn=tx_conn,
                )

            return AuthoritativeClaimDecision(
                is_owner=True,
                outcome=ArbitrationOutcome.OWNER_NEW,
                task_id=eff_task_id,
                task_key=task_key,
                run_id=run_id,
                claim_id=new_claim_id,
                lease_id=new_lease_id,
                lease_version=lease_version,
                expires_at=expires_at,
                existing_claim_id=None,
                existing_finding_id=None,
                superseded_claim_id=None,
                capability_issuance_required=True,
                reason="new_claim_granted",
            )

        # ------------------------------------------------------------------
        # Case B: Active claim exists — Check expiration
        # ------------------------------------------------------------------
        claim_exp = active_claim["expires_at"]
        if claim_exp.tzinfo is None:
            claim_exp = claim_exp.replace(tzinfo=timezone.utc)

        if claim_exp <= now_utc:
            # Active claim has expired; reclaim it
            await claim_repo.expire_claim(active_claim["id"], conn=tx_conn)
            await audit_repo.record_event(
                event_type="claim_expired",
                actor_id="system",
                run_id=run_id,
                entity_type="claim",
                entity_id=active_claim["id"],
                task_id=eff_task_id,
                details={"expired_claim_id": active_claim["id"], "expired_at": claim_exp.isoformat()},
                conn=tx_conn,
            )

            new_claim_id = str(uuid.uuid4())
            new_lease_version = active_claim["lease_version"] + 1
            new_lease_id = f"lease-{new_claim_id}-v{new_lease_version}"
            new_expires_at = c_time + timedelta(seconds=ttl)

            try:
                async with tx_conn.transaction():
                    await claim_repo.create_claim(
                        claim_id=new_claim_id,
                        task_id=eff_task_id,
                        run_id=run_id,
                        owner_id=owner_id,
                        owner_type=owner_type,
                        status="active",
                        lease_id=new_lease_id,
                        lease_version=new_lease_version,
                        expires_at=new_expires_at,
                        created_at=c_time,
                        conn=tx_conn,
                    )
            except asyncpg.UniqueViolationError:
                existing = await claim_repo.get_active_claim_for_task(eff_task_id, conn=tx_conn)
                return AuthoritativeClaimDecision(
                    is_owner=False,
                    outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=None,
                    lease_id=None,
                    lease_version=None,
                    expires_at=None,
                    existing_claim_id=existing["id"] if existing else None,
                    existing_finding_id=None,
                    superseded_claim_id=None,
                    capability_issuance_required=False,
                    reason="unique_constraint_conflict_absorbed",
                )

            await task_repo.update_task_status(eff_task_id, status="running", conn=tx_conn)

            await audit_repo.record_event(
                event_type="claim_reclaimed",
                actor_id=owner_id,
                run_id=run_id,
                entity_type="claim",
                entity_id=new_claim_id,
                task_id=eff_task_id,
                details={
                    "task_key": task_key,
                    "reclaimed_from": active_claim["id"],
                    "lease_version": new_lease_version,
                },
                conn=tx_conn,
            )

            return AuthoritativeClaimDecision(
                is_owner=True,
                outcome=ArbitrationOutcome.EXPIRED_RECLAIMED,
                task_id=eff_task_id,
                task_key=task_key,
                run_id=run_id,
                claim_id=new_claim_id,
                lease_id=new_lease_id,
                lease_version=new_lease_version,
                expires_at=new_expires_at,
                existing_claim_id=None,
                existing_finding_id=None,
                superseded_claim_id=active_claim["id"],
                capability_issuance_required=True,
                reason="expired_claim_reclaimed",
            )

        # ------------------------------------------------------------------
        # Case C: Active claim is healthy — Idempotent same-owner retry
        # ------------------------------------------------------------------
        if active_claim["owner_id"] == owner_id:
            logger.info("Task %s already held by same owner %s; returning OWNER_ALREADY_HELD", eff_task_id, owner_id)
            return AuthoritativeClaimDecision(
                is_owner=True,
                outcome=ArbitrationOutcome.OWNER_ALREADY_HELD,
                task_id=eff_task_id,
                task_key=task_key,
                run_id=run_id,
                claim_id=active_claim["id"],
                lease_id=active_claim["lease_id"],
                lease_version=active_claim["lease_version"],
                expires_at=active_claim["expires_at"],
                existing_claim_id=active_claim["id"],
                existing_finding_id=None,
                superseded_claim_id=None,
                capability_issuance_required=False,
                reason="claim_already_held_by_owner",
            )

        # ------------------------------------------------------------------
        # Case D: Active claim is healthy — Different owner: Race window vs duplicate
        # ------------------------------------------------------------------
        incumb_created_at = active_claim["created_at"]
        if incumb_created_at.tzinfo is None:
            incumb_created_at = incumb_created_at.replace(tzinfo=timezone.utc)

        diff_s = (c_time - incumb_created_at).total_seconds()
        within_race_window = abs(diff_s) <= _EPSILON_SECONDS

        if within_race_window:
            # Inside 50ms race window: evaluate tiebreak via Stage 1 policy
            challenger_bus_claim = BusClaim(
                id=str(uuid.uuid4()),
                run_id=run_id,
                participant_id=owner_id,
                participant_type=owner_type,
                content=description,
                created_at=c_time,
                ts_monotonic=c_time.timestamp(),
            )
            incumbent_bus_claim = BusClaim(
                id=active_claim["id"],
                run_id=run_id,
                participant_id=active_claim["owner_id"],
                participant_type=active_claim["owner_type"],
                content="",
                created_at=incumb_created_at,
                ts_monotonic=incumb_created_at.timestamp(),
            )
            challenger_wins = should_claim_win(challenger_bus_claim, incumbent_bus_claim)

            if challenger_wins:
                # Incumbent superseded
                await claim_repo.supersede_claim(active_claim["id"], conn=tx_conn)
                new_claim_id = str(uuid.uuid4())
                new_lease_version = active_claim["lease_version"] + 1
                new_lease_id = f"lease-{new_claim_id}-v{new_lease_version}"
                new_expires_at = c_time + timedelta(seconds=ttl)

                try:
                    async with tx_conn.transaction():
                        await claim_repo.create_claim(
                            claim_id=new_claim_id,
                            task_id=eff_task_id,
                            run_id=run_id,
                            owner_id=owner_id,
                            owner_type=owner_type,
                            status="active",
                            lease_id=new_lease_id,
                            lease_version=new_lease_version,
                            expires_at=new_expires_at,
                            created_at=c_time,
                            conn=tx_conn,
                        )
                except asyncpg.UniqueViolationError:
                    existing = await claim_repo.get_active_claim_for_task(eff_task_id, conn=tx_conn)
                    return AuthoritativeClaimDecision(
                        is_owner=False,
                        outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
                        task_id=eff_task_id,
                        task_key=task_key,
                        run_id=run_id,
                        claim_id=None,
                        lease_id=None,
                        lease_version=None,
                        expires_at=None,
                        existing_claim_id=existing["id"] if existing else None,
                        existing_finding_id=None,
                        superseded_claim_id=None,
                        capability_issuance_required=False,
                        reason="unique_constraint_conflict_absorbed",
                    )

                await task_repo.update_task_status(eff_task_id, status="running", conn=tx_conn)

                await audit_repo.record_event(
                    event_type="claim_superseded_tiebreak",
                    actor_id=owner_id,
                    run_id=run_id,
                    entity_type="claim",
                    entity_id=new_claim_id,
                    task_id=eff_task_id,
                    details={
                        "task_key": task_key,
                        "superseded_claim_id": active_claim["id"],
                        "lease_version": new_lease_version,
                    },
                    conn=tx_conn,
                )

                return AuthoritativeClaimDecision(
                    is_owner=True,
                    outcome=ArbitrationOutcome.TIEBREAK_WON,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=new_claim_id,
                    lease_id=new_lease_id,
                    lease_version=new_lease_version,
                    expires_at=new_expires_at,
                    existing_claim_id=None,
                    existing_finding_id=None,
                    superseded_claim_id=active_claim["id"],
                    capability_issuance_required=True,
                    reason="challenger_won_race_tiebreak",
                )
            else:
                # Incumbent remains active; challenger lost tiebreak
                await audit_repo.record_event(
                    event_type="claim_rejected_tiebreak",
                    actor_id=owner_id,
                    run_id=run_id,
                    entity_type="claim",
                    entity_id=active_claim["id"],
                    task_id=eff_task_id,
                    details={
                        "task_key": task_key,
                        "incumbent_claim_id": active_claim["id"],
                        "reason": "tiebreak_lost",
                    },
                    conn=tx_conn,
                )

                return AuthoritativeClaimDecision(
                    is_owner=False,
                    outcome=ArbitrationOutcome.TIEBREAK_LOST,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=None,
                    lease_id=None,
                    lease_version=None,
                    expires_at=None,
                    existing_claim_id=active_claim["id"],
                    existing_finding_id=None,
                    superseded_claim_id=None,
                    capability_issuance_required=False,
                    reason="challenger_lost_race_tiebreak",
                )

        else:
            # Outside 50ms race window:
            if diff_s < -_EPSILON_SECONDS:
                # Challenger was strictly earlier by > 50ms: challenger wins
                await claim_repo.supersede_claim(active_claim["id"], conn=tx_conn)
                new_claim_id = str(uuid.uuid4())
                new_lease_version = active_claim["lease_version"] + 1
                new_lease_id = f"lease-{new_claim_id}-v{new_lease_version}"
                new_expires_at = c_time + timedelta(seconds=ttl)

                try:
                    async with tx_conn.transaction():
                        await claim_repo.create_claim(
                            claim_id=new_claim_id,
                            task_id=eff_task_id,
                            run_id=run_id,
                            owner_id=owner_id,
                            owner_type=owner_type,
                            status="active",
                            lease_id=new_lease_id,
                            lease_version=new_lease_version,
                            expires_at=new_expires_at,
                            created_at=c_time,
                            conn=tx_conn,
                        )
                except asyncpg.UniqueViolationError:
                    existing = await claim_repo.get_active_claim_for_task(eff_task_id, conn=tx_conn)
                    return AuthoritativeClaimDecision(
                        is_owner=False,
                        outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
                        task_id=eff_task_id,
                        task_key=task_key,
                        run_id=run_id,
                        claim_id=None,
                        lease_id=None,
                        lease_version=None,
                        expires_at=None,
                        existing_claim_id=existing["id"] if existing else None,
                        existing_finding_id=None,
                        superseded_claim_id=None,
                        capability_issuance_required=False,
                        reason="unique_constraint_conflict_absorbed",
                    )

                await task_repo.update_task_status(eff_task_id, status="running", conn=tx_conn)

                await audit_repo.record_event(
                    event_type="claim_superseded_earlier_timestamp",
                    actor_id=owner_id,
                    run_id=run_id,
                    entity_type="claim",
                    entity_id=new_claim_id,
                    task_id=eff_task_id,
                    details={"task_key": task_key, "superseded_claim_id": active_claim["id"]},
                    conn=tx_conn,
                )

                return AuthoritativeClaimDecision(
                    is_owner=True,
                    outcome=ArbitrationOutcome.TIEBREAK_WON,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=new_claim_id,
                    lease_id=new_lease_id,
                    lease_version=new_lease_version,
                    expires_at=new_expires_at,
                    existing_claim_id=None,
                    existing_finding_id=None,
                    superseded_claim_id=active_claim["id"],
                    capability_issuance_required=True,
                    reason="challenger_earlier_timestamp",
                )
            else:
                # Late duplicate arriving outside race window: incumbent is healthy and wins
                await audit_repo.record_event(
                    event_type="duplicate_claim_absorbed",
                    actor_id=owner_id,
                    run_id=run_id,
                    entity_type="claim",
                    entity_id=active_claim["id"],
                    task_id=eff_task_id,
                    details={
                        "task_key": task_key,
                        "incumbent_claim_id": active_claim["id"],
                        "time_diff_s": diff_s,
                    },
                    conn=tx_conn,
                )

                return AuthoritativeClaimDecision(
                    is_owner=False,
                    outcome=ArbitrationOutcome.DUPLICATE_ABSORBED,
                    task_id=eff_task_id,
                    task_key=task_key,
                    run_id=run_id,
                    claim_id=None,
                    lease_id=None,
                    lease_version=None,
                    expires_at=None,
                    existing_claim_id=active_claim["id"],
                    existing_finding_id=None,
                    superseded_claim_id=None,
                    capability_issuance_required=False,
                    reason="late_duplicate_absorbed",
                )
