"""
Pydantic v2 models for the Quorum semantic bus (Moss).

All models use strict mode with extra fields forbidden to enforce
schema integrity across the multi-agent boundary.
"""

from pydantic import BaseModel, Field, ConfigDict
from typing import Literal, Optional
from datetime import datetime
import uuid


# ---------------------------------------------------------------------------
# Literal enums
# ---------------------------------------------------------------------------

ParticipantType = Literal["agent", "human", "adjudicator", "reaper"]
"""Valid participant roles in the Quorum system."""

ClaimStatus = Literal["active", "superseded", "resolved", "reaped"]
"""Lifecycle states for a Claim on the semantic board."""

ReapReason = Literal["heartbeat_timeout", "run_ended", "manual"]
"""Reasons a Claim may be reaped by the reaper agent."""


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------

class Claim(BaseModel):
    """
    A position or assertion posted by a participant to the semantic board.

    Claims are the primary unit of discourse on the Moss semantic bus.
    Each claim is versioned via the supersedes chain (§9.3) and has a
    heartbeat to detect stale participants.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    participant_id: str
    participant_type: ParticipantType
    content: str = Field(max_length=4096)
    status: ClaimStatus = "active"
    supersedes: Optional[str] = None  # ID of claim this supersedes
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    reap_reason: Optional[ReapReason] = None


class Finding(BaseModel):
    """
    A structured research result posted by an agent or adjudicator.

    Findings are written to a separate namespace from Claims and may
    reference external sources. Like claims, findings support a
    supersedes chain for incremental updates.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    participant_id: str
    participant_type: ParticipantType
    title: str = Field(max_length=256)
    content: str = Field(max_length=8192)
    sources: list[str] = Field(default_factory=list)
    supersedes: Optional[str] = None  # ID of finding this supersedes
    created_at: datetime = Field(default_factory=datetime.utcnow)


class Conflict(BaseModel):
    """
    A detected semantic conflict between two Findings.

    Conflicts are raised when two findings exceed a similarity threshold
    but carry contradictory content. They are never auto-resolved —
    an adjudicator must provide a verdict.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    run_id: str
    finding_a_id: str
    finding_b_id: str
    similarity_score: float
    adjudicator_verdict: Optional[str] = None  # LLM summary, never auto-resolves
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved: bool = False


class SenseResult(BaseModel):
    """
    Result of a semantic sense (query) operation against the Moss board.

    Contains raw chromadb results along with the original query and
    measured latency for observability.
    """

    model_config = ConfigDict(strict=True, extra="forbid")

    query: str
    results: list[dict]  # raw chromadb results
    latency_ms: float
