"""API routes for run management: POST /runs, DELETE /runs/{id}."""
import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

import aiosqlite
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict

from api.auth import get_current_user, TokenData
from db.models import get_db
from db.erasure import erase_run
from bus.namespaces import get_claims_namespace, get_findings_namespace
from config.settings import settings

logger = logging.getLogger(__name__)
router = APIRouter()


class CreateRunRequest(BaseModel):
    """Request body for creating a new run."""
    model_config = ConfigDict(strict=True, extra="forbid")

    question: str
    participant_ids: list[str] = []


class RunResponse(BaseModel):
    """Response body for a run."""
    model_config = ConfigDict(strict=True, extra="forbid")

    id: str
    question: str
    started_at: str
    ended_at: Optional[str] = None
    status: str


class EraseRunResponse(BaseModel):
    """Response body for run erasure."""
    model_config = ConfigDict(strict=True, extra="forbid")

    run_id: str
    erased: dict


@router.post("/", response_model=RunResponse, status_code=status.HTTP_201_CREATED)
async def create_run(
    body: CreateRunRequest,
    current_user: TokenData = Depends(get_current_user),
):
    """Create a new Quorum run."""
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc).isoformat()

    async with get_db() as db:
        await db.execute(
            "INSERT INTO runs (id, question, started_at, status) VALUES (?, ?, ?, ?)",
            (run_id, body.question, started_at, "running"),
        )
        await db.commit()

    logger.info(f"Created run {run_id} question='{body.question[:50]}'")
    return RunResponse(
        id=run_id,
        question=body.question,
        started_at=started_at,
        status="running",
    )


@router.delete("/{run_id}", response_model=EraseRunResponse)
async def delete_run(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Full erasure of a run: Moss namespaces + SQLite rows + OTel tombstones.
    
    All three erasure targets are verified — see db/erasure.py for contract.
    """
    claims_col = get_claims_namespace()
    findings_col = get_findings_namespace()

    async with get_db() as db:
        # Verify run exists
        async with db.execute("SELECT id FROM runs WHERE id = ?", (run_id,)) as cur:
            row = await cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

        result = await erase_run(
            run_id=run_id,
            moss_claims_collection=claims_col,
            moss_findings_collection=findings_col,
            db=db,
        )

    logger.info(f"Erased run {run_id}: {result}")
    return EraseRunResponse(run_id=run_id, erased=result)


@router.get("/{run_id}", response_model=RunResponse)
async def get_run(
    run_id: str,
    current_user: TokenData = Depends(get_current_user),
):
    """Get run metadata."""
    async with get_db() as db:
        async with db.execute(
            "SELECT id, question, started_at, ended_at, status FROM runs WHERE id = ?",
            (run_id,),
        ) as cur:
            row = await cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail=f"Run {run_id} not found")

    return RunResponse(
        id=row[0],
        question=row[1],
        started_at=row[2],
        ended_at=row[3],
        status=row[4],
    )
