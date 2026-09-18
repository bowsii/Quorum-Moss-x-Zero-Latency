"""
MOSS SDK - Cryptographic identity and signed outputs for AI agents.

Quick Start:
    from moss import sign, verify
    
    # Sign any agent output
    result = sign(
        output={"action": "send_email", "to": "user@example.com"},
        agent_id="email-agent",
    )
    
    # Access the envelope
    envelope = result.envelope
    
    # Verify anywhere (no network required)
    verification = verify(envelope)
    assert verification.valid

Enterprise Mode:
    Set MOSS_API_KEY environment variable to enable:
    - Policy evaluation (allow/block/reauth)
    - Evidence retention
    - Usage tracking
    
    Enterprise mode is automatic - same code works in both modes.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, Optional, Union

from .subject import Subject, VerifyResult
from .envelope import Envelope
from .errors import (
    MossError,
    InvalidSubject,
    KeyNotFound,
    InvalidEnvelope,
    InvalidSignature,
    PayloadMismatch,
    ReplayDetected,
    DecryptionFailed,
)
from .enterprise import (
    is_enterprise_mode,
    evaluate_sync,
    evaluate_async,
    upload_evidence_sync,
    upload_evidence_async,
    Decision,
    PolicyResult,
    EnterpriseResult,
)

__version__ = "0.3.0"


# =============================================================================
# Result Types
# =============================================================================

@dataclass
class SignResult:
    """
    Result from signing an agent output.
    
    Attributes:
        envelope: The cryptographic envelope with signature
        payload: The payload that was signed
        enterprise: Enterprise API result (if MOSS_API_KEY set)
        
    Properties:
        signature: The ML-DSA-44 signature string
        agent_id: The agent that signed this output
        timestamp: Unix timestamp when signed
        allowed: True if action is allowed (always True in local mode)
        blocked: True if policy blocked the action
        evidence_id: Evidence record ID (enterprise only)
    """
    envelope: Envelope
    payload: Dict[str, Any]
    enterprise: EnterpriseResult = field(default_factory=EnterpriseResult)
    
    @property
    def signature(self) -> str:
        """The ML-DSA-44 post-quantum signature."""
        return self.envelope.signature
    
    @property
    def agent_id(self) -> str:
        """The agent that signed this output."""
        return self.envelope.subject
    
    @property
    def timestamp(self) -> int:
        """Unix timestamp when this was signed."""
        return self.envelope.issued_at
    
    @property
    def allowed(self) -> bool:
        """True if action is allowed (always True in local mode)."""
        return self.enterprise.allowed
    
    @property
    def blocked(self) -> bool:
        """True if policy blocked the action (always False in local mode)."""
        return self.enterprise.blocked
    
    @property
    def evidence_id(self) -> Optional[str]:
        """Evidence record ID (enterprise mode only)."""
        return self.enterprise.evidence_id
    
    @property
    def policy_decision(self) -> Optional[str]:
        """Policy decision: 'allow', 'block', or 'reauth' (enterprise only)."""
        if self.enterprise.policy:
            return self.enterprise.policy.decision.value
        return None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        result = {
            "envelope": self.envelope.to_dict(),
            "payload": self.payload,
        }
        if self.enterprise.policy:
            result["policy"] = {
                "decision": self.enterprise.policy.decision.value,
                "reason": self.enterprise.policy.reason,
            }
        if self.enterprise.evidence_id:
            result["evidence_id"] = self.enterprise.evidence_id
        return result


# =============================================================================
# Subject Cache
# =============================================================================

_subject_cache: Dict[str, Subject] = {}


def _get_or_create_subject(agent_id: str) -> Subject:
    """Get or create a subject for an agent ID."""
    subject_id = _normalize_subject_id(agent_id)
    
    if subject_id not in _subject_cache:
        try:
            _subject_cache[subject_id] = Subject.load(subject_id)
        except KeyNotFound:
            _subject_cache[subject_id] = Subject.create(subject_id)
    return _subject_cache[subject_id]


def _normalize_subject_id(agent_id: str) -> str:
    """Normalize agent ID to full subject format."""
    if agent_id.startswith("moss:"):
        return agent_id
    return f"moss:agent:{agent_id}"


def _normalize_payload(output: Any, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Normalize output to a dictionary payload."""
    if isinstance(output, dict):
        payload = output.copy()
    elif hasattr(output, "to_dict"):
        payload = output.to_dict()
    elif hasattr(output, "__dict__"):
        payload = {k: v for k, v in output.__dict__.items() if not k.startswith("_")}
    else:
        payload = {"output": output}
    
    if context:
        payload["_context"] = context
    
    return payload


# =============================================================================
# Main API
# =============================================================================

def sign(
    output: Any,
    agent_id: str,
    *,
    action: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    evaluate_policy: bool = True,
) -> SignResult:
    """
    Sign an agent output with MOSS.
    
    Creates a cryptographic signature using ML-DSA-44 (post-quantum secure).
    If MOSS_API_KEY is set, also evaluates policies and uploads evidence.
    
    Args:
        output: The agent output to sign (any JSON-serializable data)
        agent_id: Identifier for the agent (e.g., "email-agent", "finance-bot")
        action: Action name for policy evaluation (default: "sign")
        context: Optional context metadata (user_id, session_id, etc.)
        evaluate_policy: Whether to call enterprise API (default: True)
    
    Returns:
        SignResult containing:
        - envelope: The cryptographic envelope with signature
        - payload: The signed payload
        - enterprise: Policy result and evidence ID (if enterprise mode)
    
    Example:
        from moss import sign
        
        # Simple signing
        result = sign({"action": "send_email"}, agent_id="email-agent")
        print(result.signature)
        
        # With context
        result = sign(
            output=tool_result,
            agent_id="finance-agent",
            action="approve_transaction",
            context={"user_id": "u123", "amount": 50000}
        )
        
        # Check policy (enterprise mode)
        if result.blocked:
            print(f"Blocked: {result.enterprise.policy.reason}")
    """
    # Normalize inputs
    payload = _normalize_payload(output, context)
    subject = _get_or_create_subject(agent_id)
    action_name = action or "sign"
    
    # Sign locally (always happens, regardless of enterprise mode)
    envelope = subject.sign(payload)
    
    # Enterprise mode: evaluate policy and upload evidence
    enterprise_result = EnterpriseResult()
    
    if is_enterprise_mode() and evaluate_policy:
        enterprise_result = evaluate_sync(
            agent_id=agent_id,
            action=action_name,
            envelope=envelope.to_dict(),
            payload=payload,
            context=context,
        )
    
    return SignResult(
        envelope=envelope,
        payload=payload,
        enterprise=enterprise_result,
    )


async def sign_async(
    output: Any,
    agent_id: str,
    *,
    action: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None,
    evaluate_policy: bool = True,
) -> SignResult:
    """
    Sign an agent output with MOSS (async version).
    
    Same as sign() but for async contexts. Use this in async frameworks
    like FastAPI, aiohttp, or async LangChain chains.
    
    Args:
        output: The agent output to sign
        agent_id: Identifier for the agent
        action: Action name for policy evaluation
        context: Optional context metadata
        evaluate_policy: Whether to call enterprise API
    
    Returns:
        SignResult with envelope, payload, and enterprise result
    
    Example:
        from moss import sign_async
        
        async def handle_agent_output(output):
            result = await sign_async(output, agent_id="my-agent")
            return result.envelope
    """
    # Normalize inputs
    payload = _normalize_payload(output, context)
    subject = _get_or_create_subject(agent_id)
    action_name = action or "sign"
    
    # Sign locally
    envelope = subject.sign(payload)
    
    # Enterprise mode: evaluate policy
    enterprise_result = EnterpriseResult()
    
    if is_enterprise_mode() and evaluate_policy:
        enterprise_result = await evaluate_async(
            agent_id=agent_id,
            action=action_name,
            envelope=envelope.to_dict(),
            payload=payload,
            context=context,
        )
    
    return SignResult(
        envelope=envelope,
        payload=payload,
        enterprise=enterprise_result,
    )


def verify(
    envelope: Union[Envelope, Dict[str, Any]],
    payload: Any = None,
) -> VerifyResult:
    """
    Verify a signed envelope - no network required.
    
    Cryptographic verification happens entirely locally using the
    public key stored in the local keystore.
    
    Args:
        envelope: MOSS Envelope or dict representation
        payload: Original payload for hash verification (optional)
    
    Returns:
        VerifyResult with:
        - valid: True if signature is valid and untampered
        - subject: The agent that signed (if valid)
        - reason: Error reason (if invalid)
    
    Example:
        from moss import verify
        
        result = verify(envelope)
        if result.valid:
            print(f"Verified: signed by {result.subject}")
        else:
            print(f"Invalid: {result.reason}")
    """
    return Subject.verify(
        envelope=envelope,
        payload=payload,
        check_replay=False,
    )


# =============================================================================
# Utility Functions
# =============================================================================

def get_agent_subject(agent_id: str) -> Subject:
    """
    Get or create a Subject for an agent ID.
    
    Use this if you need direct access to the Subject for advanced operations.
    Most users should just use sign() and verify().
    """
    return _get_or_create_subject(agent_id)


def enterprise_enabled() -> bool:
    """Check if enterprise mode is enabled (MOSS_API_KEY is set)."""
    return is_enterprise_mode()


# =============================================================================
# Exports
# =============================================================================

__all__ = [
    # Main API
    "sign",
    "sign_async", 
    "verify",
    
    # Result types
    "SignResult",
    "VerifyResult",
    
    # Core types
    "Subject",
    "Envelope",
    
    # Enterprise types
    "Decision",
    "PolicyResult",
    "EnterpriseResult",
    
    # Utilities
    "get_agent_subject",
    "enterprise_enabled",
    
    # Errors
    "MossError",
    "InvalidSubject",
    "KeyNotFound",
    "InvalidEnvelope",
    "InvalidSignature",
    "PayloadMismatch",
    "ReplayDetected",
    "DecryptionFailed",
]
