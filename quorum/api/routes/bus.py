"""API routes for the semantic bus: sense, claim, write, conflicts."""
import logging
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

from api.auth import get_current_user, TokenData
from bus.moss_client import sense, write_claim, write_finding, update_claim_status
from bus.schemas import Claim, Finding, Conflict, ParticipantType, SenseResult
from adjudicator.conflict_tray import conflict_tray
from config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter()


class SenseRequest(BaseModel):
    """HTTP surface for sense() — semantic similarity query."""
    model_config = ConfigDict(strict=True, extra="forbid")

    query: str = Field(max_length=1024)
    namespace: str = Field(default="claims", pattern="^(claims|findings)$")
    n_results: int = Field(default=10, ge=1, le=50)
    run_id: Optional[str] = None


class ClaimRequest(BaseModel):
    """HTTP surface for claim() — write a claim to the board."""
    model_config = ConfigDict(strict=True, extra="forbid")

    run_id: str
    participant_id: str
    participant_type: ParticipantType
    content: str = Field(max_length=4096)
    supersedes: Optional[str] = None


class FindingRequest(BaseModel):
    """HTTP surface for write_finding()."""
    model_config = ConfigDict(strict=True, extra="forbid")

    run_id: str
    participant_id: str
    participant_type: ParticipantType
    title: str = Field(max_length=256)
    content: str = Field(max_length=8192)
    sources: list[str] = Field(default_factory=list)
    supersedes: Optional[str] = None


@router.post("/sense", response_model=SenseResult)
async def sense_endpoint(
    body: SenseRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Query the semantic board for similar claims or findings."""
    result = await sense(query=body.query, namespace=body.namespace, n_results=body.n_results)
    return result


@router.post("/claim", response_model=Claim, status_code=status.HTTP_201_CREATED)
async def claim_endpoint(
    body: ClaimRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Write a claim to the semantic board."""
    from bus.schemas import Claim as ClaimModel
    claim = ClaimModel(
        run_id=body.run_id,
        participant_id=body.participant_id,
        participant_type=body.participant_type,
        content=body.content,
        supersedes=body.supersedes,
    )
    await write_claim(claim)
    return claim


@router.post("/finding", response_model=Finding, status_code=status.HTTP_201_CREATED)
async def finding_endpoint(
    body: FindingRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Write a finding to the semantic board."""
    finding = Finding(
        run_id=body.run_id,
        participant_id=body.participant_id,
        participant_type=body.participant_type,
        title=body.title,
        content=body.content,
        sources=body.sources,
        supersedes=body.supersedes,
    )
    await write_finding(finding)
    return finding


@router.get("/conflicts", response_model=list[Conflict])
async def get_conflicts(
    run_id: Optional[str] = None,
    current_user: TokenData = Depends(get_current_user),
):
    """Get all detected conflicts from the conflict tray."""
    return await conflict_tray.get_all(run_id=run_id)


@router.get("/conflicts/{conflict_id}", response_model=Conflict)
async def get_conflict(
    conflict_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get a specific conflict."""
    conflict = await conflict_tray.get(conflict_id)
    if not conflict:
        raise HTTPException(status_code=404, detail=f"Conflict {conflict_id} not found")
    return conflict


@router.post("/conflicts/{conflict_id}/resolve")
async def resolve_conflict(
    conflict_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Mark a conflict as resolved (human decision only — adjudicator never auto-resolves)."""
    conflict = await conflict_tray.get(conflict_id)
    if not conflict:
        raise HTTPException(status_code=404, detail=f"Conflict {conflict_id} not found")
    await conflict_tray.mark_resolved(conflict_id)
    return {"conflict_id": conflict_id, "resolved": True}
