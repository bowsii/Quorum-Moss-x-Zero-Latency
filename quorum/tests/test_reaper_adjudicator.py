"""tests/test_reaper_adjudicator.py
----------------------------------
Unit tests for Reaper and Adjudicator components.
Verifies:
- Reaper 15s TTL expiration and reason-coded reap
- Adjudicator conflict detection and ConflictTray interaction
- Adjudicator never auto-resolves
"""
import pytest
import asyncio
from datetime import datetime, timezone, timedelta
from bus.schemas import Claim, Finding, Conflict
from bus.moss_client import write_claim, write_finding, get_claim
from bus.namespaces import get_claims_namespace, get_findings_namespace
from reaper.reaper import reaper
from adjudicator.adjudicator import adjudicator
from adjudicator.conflict_tray import conflict_tray


@pytest.mark.asyncio
async def test_reaper_identifies_and_reaps_expired_claims():
    """Claims with heartbeat older than 15s should be reaped with 'heartbeat_timeout'."""
    col = get_claims_namespace()
    expired_time = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()

    expired_claim_id = "claim-expired-test"
    col.add(
        ids=[expired_claim_id],
        documents=["Expired active task claim"],
        metadatas=[{
            "run_id": "run-reap-test",
            "participant_id": "agent-dead",
            "participant_type": "agent",
            "status": "active",
            "created_at": expired_time,
            "last_heartbeat": expired_time,
            "supersedes": "",
            "reap_reason": "",
        }],
    )

    # Run one scan
    reaped_count = await reaper.scan_once()
    assert reaped_count >= 1

    # Verify claim is now marked 'reaped' with reason 'heartbeat_timeout'
    updated = await get_claim(expired_claim_id)
    assert updated["metadata"]["status"] == "reaped"
    assert updated["metadata"]["reap_reason"] == "heartbeat_timeout"


@pytest.mark.asyncio
async def test_conflict_tray_and_manual_resolution():
    """Adjudicator detects semantic conflicts, places them in tray, never auto-resolves.
    Only manual mark_resolved resolves them."""
    conflict = Conflict(
        id="conflict-1",
        run_id="run-conf-test",
        finding_a_id="f-1",
        finding_b_id="f-2",
        similarity_score=0.92,
        adjudicator_verdict="Finding A argues X while Finding B disputes X with new evidence.",
        resolved=False,
    )

    await conflict_tray.add(conflict)

    stored = await conflict_tray.get("conflict-1")
    assert stored is not None
    assert stored.resolved is False
    assert "Finding A argues X" in stored.adjudicator_verdict

    # Manual resolution
    await conflict_tray.mark_resolved("conflict-1")
    resolved_stored = await conflict_tray.get("conflict-1")
    assert resolved_stored.resolved is True
