"""
quorum/bus/arbitration.py
---------------------------
Coordination-facing boundary for Authoritative Claim Arbitration (Stage 2 / Phase C).

Boundary Rule:
--------------
Exposes authoritative arbitration functions to bus and agent runtimes.
PostgreSQL remains the sole authoritative source of truth.
No claim arbitration logic is duplicated here; calls are dispatched directly
to the authoritative db.arbitration module.

CoordinationToken Issuance Rule:
--------------------------------
A CoordinationToken may be issued ONLY AFTER the authoritative transaction has
successfully committed AND decision.capability_issuance_required is True.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from db.arbitration import (
    ArbitrationOutcome,
    AuthoritativeClaimDecision,
    claim_task_authoritatively,
)
from agents.coordination_token import CoordinationToken

logger = logging.getLogger(__name__)

__all__ = [
    "ArbitrationOutcome",
    "AuthoritativeClaimDecision",
    "claim_task_authoritatively",
    "issue_coordination_token_for_decision",
]


def issue_coordination_token_for_decision(
    decision: AuthoritativeClaimDecision,
    owner_id: str,
) -> Optional[CoordinationToken]:
    """Issue a typed CoordinationToken capability based on an authoritative claim decision.

    Guarantees:
    - Returns a valid CoordinationToken ONLY when the decision explicitly requires it
      (capability_issuance_required is True) and ownership is verified.
    - If the contender is absorbed, lost tiebreak, or already held the claim, returns None.
    - Losers without a token fail closed when attempting external operations.
    """
    if not decision.capability_issuance_required:
        return None

    if not decision.is_owner or not decision.claim_id:
        return None

    now = datetime.now(timezone.utc)
    exp = decision.expires_at or (now + timedelta(seconds=60))

    return CoordinationToken(
        run_id=decision.run_id,
        task_id=decision.task_id,
        claim_id=decision.claim_id,
        owner_id=owner_id,
        version=str(decision.lease_version or 1),
        issued_at=now,
        expires_at=exp,
        status="active",
    )
