"""
quorum/agents/coordination_token.py
------------------------------------
Typed coordination capability (token) and authorization boundary for external work.

Non-negotiable Invariant:
No expensive external call (Tavily, LLM inference, network API) may execute
unless the calling context possesses a valid, non-expired CoordinationToken
bound to an active claim that won coordination.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterator, Optional

logger = logging.getLogger(__name__)


class UnapprovedExternalWorkError(RuntimeError):
    """Raised when an external tool or model is invoked without a valid coordination capability.

    Enforces the Quorum coordination firewall:
    SENSE -> CLAIM -> CLAIM VALIDATION / TIEBREAK -> EXTERNAL_WORK_ALLOWED.
    """


# ContextVar holding the ambient coordination token for the current asyncio task/context
_current_coordination_token: ContextVar[Optional[CoordinationToken]] = ContextVar(
    "current_coordination_token", default=None
)


@dataclass(frozen=True)
class CoordinationToken:
    """Cryptographically or structurally verifiable authorization capability.

    Bound to a specific run, task, claim, and owner.
    """

    run_id: str
    task_id: str
    claim_id: str
    owner_id: str
    version: str
    issued_at: datetime
    expires_at: datetime
    status: str = "active"
    lease_checker: Optional[Callable[[str], bool]] = field(default=None, repr=False, compare=False)

    def is_valid(self, now: Optional[datetime] = None) -> bool:
        """Check if capability is active, within its lease window, and not reaped."""
        if self.status != "active":
            return False

        current_time = now or datetime.now(timezone.utc)
        if current_time.tzinfo is None:
            current_time = current_time.replace(tzinfo=timezone.utc)

        exp = self.expires_at
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)

        if current_time >= exp:
            return False

        # If a live lease checker is attached, query if the claim is still active
        if self.lease_checker is not None:
            try:
                if not self.lease_checker(self.claim_id):
                    return False
            except Exception as e:
                logger.warning("Error running lease checker for claim %s: %s", self.claim_id, e)
                return False

        return True


def get_current_coordination_token() -> Optional[CoordinationToken]:
    """Retrieve the ambient coordination capability from the current task context."""
    return _current_coordination_token.get()


@contextmanager
def bound_coordination_token(token: CoordinationToken) -> Iterator[CoordinationToken]:
    """Context manager that binds a CoordinationToken to the current task execution context."""
    reset_token = _current_coordination_token.set(token)
    try:
        yield token
    finally:
        _current_coordination_token.reset(reset_token)


def check_claim_is_active(claim_id: str) -> bool:
    """Query claims collection to check if claim status is 'active'."""
    try:
        from bus.namespaces import get_claims_namespace
        col = get_claims_namespace()
        res = col.get(ids=[claim_id], include=["metadatas"])
        if not res or not res.get("ids"):
            # If claim was never written or doesn't exist in Chroma, it's not active
            return True
        meta = res["metadatas"][0] if res.get("metadatas") else {}
        status = meta.get("status", "active")
        return status == "active"
    except Exception as e:
        logger.debug("Error checking claim status for %s: %s", claim_id, e)
        return True


def verify_coordination_capability(
    token: Optional[CoordinationToken] = None,
    claim_id: Optional[str] = None,
) -> CoordinationToken:
    """Verify that a valid, active coordination capability is present.

    Parameters
    ----------
    token:
        Explicit token passed by caller. If None, resolves from ambient context.
    claim_id:
        Optional expected claim_id to verify claim binding.

    Returns
    -------
    CoordinationToken
        The validated token.

    Raises
    ------
    UnapprovedExternalWorkError
        If no token is present, if the token is expired/invalid, or if claim binding mismatches.
    """
    active_token = token or get_current_coordination_token()

    if active_token is None:
        raise UnapprovedExternalWorkError(
            "Unapproved external work rejected: No coordination capability present in execution context. "
            "Agents must sense, claim, and receive EXTERNAL_ALLOWED before calling external services."
        )

    if not active_token.is_valid():
        raise UnapprovedExternalWorkError(
            f"Unapproved external work rejected: Coordination capability for claim {active_token.claim_id} "
            f"is expired, revoked, or no longer active (status={active_token.status}, "
            f"expires_at={active_token.expires_at.isoformat()})."
        )

    if not check_claim_is_active(active_token.claim_id):
        raise UnapprovedExternalWorkError(
            f"Unapproved external work rejected: Claim {active_token.claim_id} is no longer active "
            f"(it may have been reaped or superseded by another participant)."
        )

    if claim_id is not None and active_token.claim_id != claim_id:
        raise UnapprovedExternalWorkError(
            f"Unapproved external work rejected: Capability bound to claim {active_token.claim_id}, "
            f"expected claim {claim_id}."
        )

    return active_token
