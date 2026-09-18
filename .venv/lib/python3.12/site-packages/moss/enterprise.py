"""
MOSS Enterprise Client - API integration for policy evaluation and evidence retention.

This module handles all enterprise API communication. It's automatically used
when MOSS_API_KEY environment variable is set.

Local mode (no API key): Signs locally, no network calls
Enterprise mode (API key set): Signs locally + policy evaluation + evidence upload
"""

import os
import logging
from dataclasses import dataclass, field
from typing import Any, Dict, Optional
from enum import Enum

logger = logging.getLogger("moss.enterprise")

# Environment configuration
MOSS_API_URL = os.getenv("MOSS_API_URL", "https://api.mosscomputing.com")
MOSS_API_KEY = os.getenv("MOSS_API_KEY")


class Decision(Enum):
    """Policy evaluation decision."""
    ALLOW = "allow"
    BLOCK = "block"
    REAUTH = "reauth"


@dataclass
class PolicyResult:
    """Result from enterprise policy evaluation."""
    decision: Decision
    reason: Optional[str] = None
    shadow_mode: bool = False
    evidence_id: Optional[str] = None
    usage_warning: Optional[str] = None


@dataclass
class EnterpriseResult:
    """Full result from enterprise API call."""
    policy: Optional[PolicyResult] = None
    evidence_id: Optional[str] = None
    error: Optional[str] = None
    
    @property
    def allowed(self) -> bool:
        """Check if action is allowed (or no policy evaluation occurred)."""
        if self.policy is None:
            return True
        return self.policy.decision == Decision.ALLOW
    
    @property
    def blocked(self) -> bool:
        """Check if action was blocked by policy."""
        if self.policy is None:
            return False
        return self.policy.decision == Decision.BLOCK


def is_enterprise_mode() -> bool:
    """Check if enterprise mode is enabled (MOSS_API_KEY is set)."""
    return bool(MOSS_API_KEY)


def _get_headers() -> Dict[str, str]:
    """Get authorization headers for API calls."""
    return {
        "Authorization": f"Bearer {MOSS_API_KEY}",
        "Content-Type": "application/json",
    }


def evaluate_sync(
    agent_id: str,
    action: str,
    envelope: Dict[str, Any],
    payload: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> EnterpriseResult:
    """
    Evaluate action against enterprise policies (synchronous).
    
    Args:
        agent_id: The agent identifier
        action: The action being performed
        envelope: The signed envelope dict
        payload: The payload that was signed
        context: Optional additional context
    
    Returns:
        EnterpriseResult with policy decision and evidence ID
    """
    if not is_enterprise_mode():
        return EnterpriseResult()
    
    try:
        import httpx
    except ImportError:
        logger.warning("httpx not installed, enterprise features disabled. Install with: pip install httpx")
        return EnterpriseResult(error="httpx not installed")
    
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                f"{MOSS_API_URL}/v1/evaluate",
                headers=_get_headers(),
                json={
                    "envelope": envelope,
                    "payload": {
                        "action": action,
                        "agent_id": agent_id,
                        **(context or {}),
                        **payload,
                    },
                },
            )
            
            if response.status_code == 402:
                # Tier limit - shadow mode result
                data = response.json()
                shadow = data.get("shadow_result", {})
                return EnterpriseResult(
                    policy=PolicyResult(
                        decision=Decision.BLOCK if shadow.get("would_block") else Decision.ALLOW,
                        reason=shadow.get("reason"),
                        shadow_mode=True,
                    ),
                    error=data.get("message"),
                )
            
            response.raise_for_status()
            data = response.json()
            
            decision_str = data.get("decision", "allow")
            try:
                decision = Decision(decision_str)
            except ValueError:
                decision = Decision.ALLOW
            
            return EnterpriseResult(
                policy=PolicyResult(
                    decision=decision,
                    reason=data.get("reason"),
                    shadow_mode=data.get("shadow_mode", False),
                    evidence_id=data.get("evidence_id"),
                    usage_warning=data.get("usage_warning"),
                ),
                evidence_id=data.get("evidence_id"),
            )
            
    except httpx.TimeoutException:
        logger.warning("Enterprise API timeout, falling back to local-only mode")
        return EnterpriseResult(error="API timeout")
    except httpx.HTTPStatusError as e:
        logger.warning(f"Enterprise API error {e.response.status_code}, falling back to local-only mode")
        return EnterpriseResult(error=f"API error: {e.response.status_code}")
    except Exception as e:
        logger.warning(f"Enterprise API error: {e}, falling back to local-only mode")
        return EnterpriseResult(error=str(e))


async def evaluate_async(
    agent_id: str,
    action: str,
    envelope: Dict[str, Any],
    payload: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> EnterpriseResult:
    """
    Evaluate action against enterprise policies (asynchronous).
    
    Args:
        agent_id: The agent identifier
        action: The action being performed
        envelope: The signed envelope dict
        payload: The payload that was signed
        context: Optional additional context
    
    Returns:
        EnterpriseResult with policy decision and evidence ID
    """
    if not is_enterprise_mode():
        return EnterpriseResult()
    
    try:
        import httpx
    except ImportError:
        logger.warning("httpx not installed, enterprise features disabled. Install with: pip install httpx")
        return EnterpriseResult(error="httpx not installed")
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{MOSS_API_URL}/v1/evaluate",
                headers=_get_headers(),
                json={
                    "envelope": envelope,
                    "payload": {
                        "action": action,
                        "agent_id": agent_id,
                        **(context or {}),
                        **payload,
                    },
                },
            )
            
            if response.status_code == 402:
                data = response.json()
                shadow = data.get("shadow_result", {})
                return EnterpriseResult(
                    policy=PolicyResult(
                        decision=Decision.BLOCK if shadow.get("would_block") else Decision.ALLOW,
                        reason=shadow.get("reason"),
                        shadow_mode=True,
                    ),
                    error=data.get("message"),
                )
            
            response.raise_for_status()
            data = response.json()
            
            decision_str = data.get("decision", "allow")
            try:
                decision = Decision(decision_str)
            except ValueError:
                decision = Decision.ALLOW
            
            return EnterpriseResult(
                policy=PolicyResult(
                    decision=decision,
                    reason=data.get("reason"),
                    shadow_mode=data.get("shadow_mode", False),
                    evidence_id=data.get("evidence_id"),
                    usage_warning=data.get("usage_warning"),
                ),
                evidence_id=data.get("evidence_id"),
            )
            
    except Exception as e:
        logger.warning(f"Enterprise API error: {e}, falling back to local-only mode")
        return EnterpriseResult(error=str(e))


def upload_evidence_sync(envelope: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    """
    Upload signed evidence to MOSS cloud (synchronous).
    
    Returns evidence ID if successful, None otherwise.
    """
    if not is_enterprise_mode():
        return None
    
    try:
        import httpx
    except ImportError:
        return None
    
    try:
        with httpx.Client(timeout=10.0) as client:
            response = client.post(
                f"{MOSS_API_URL}/v1/evidence",
                headers=_get_headers(),
                json={"envelope": envelope, "payload": payload},
            )
            response.raise_for_status()
            return response.json().get("evidence_id")
    except Exception as e:
        logger.warning(f"Evidence upload failed: {e}")
        return None


async def upload_evidence_async(envelope: Dict[str, Any], payload: Dict[str, Any]) -> Optional[str]:
    """
    Upload signed evidence to MOSS cloud (asynchronous).
    
    Returns evidence ID if successful, None otherwise.
    """
    if not is_enterprise_mode():
        return None
    
    try:
        import httpx
    except ImportError:
        return None
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{MOSS_API_URL}/v1/evidence",
                headers=_get_headers(),
                json={"envelope": envelope, "payload": payload},
            )
            response.raise_for_status()
            return response.json().get("evidence_id")
    except Exception as e:
        logger.warning(f"Evidence upload failed: {e}")
        return None
