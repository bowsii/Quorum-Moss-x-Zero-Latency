"""tests/test_versioning.py
--------------------------
Unit tests for bus/versioning.py.
Verifies chain-head resolution for supersedes-chains.
"""
import pytest
import asyncio
from bus.schemas import Claim
from bus.moss_client import write_claim, get_claim
from bus.versioning import resolve_chain_head, get_active_head


@pytest.mark.asyncio
async def test_three_link_chain_resolves_to_head():
    """A 3-link supersedes chain (Claim A -> supersedes B -> supersedes C)
    resolves only the head claim."""
    run_id = "run-chain-test"

    claim_c = Claim(
        id="claim-c",
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Claim C (initial)",
        status="superseded",
    )
    claim_b = Claim(
        id="claim-b",
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Claim B (replaces C)",
        status="superseded",
        supersedes="claim-c",
    )
    claim_a = Claim(
        id="claim-a",
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Claim A (head of chain)",
        status="active",
        supersedes="claim-b",
    )

    await write_claim(claim_c)
    await write_claim(claim_b)
    await write_claim(claim_a)

    head = await resolve_chain_head("claim-a")
    assert head is not None
    assert head["id"] == "claim-a"
    assert head["metadata"]["status"] == "active"


@pytest.mark.asyncio
async def test_get_active_head_returns_none_if_reaped():
    """If head is not 'active', get_active_head returns None."""
    reaped_claim = Claim(
        id="claim-reaped-1",
        run_id="run-test",
        participant_id="agent-1",
        participant_type="agent",
        content="Reaped claim",
        status="reaped",
    )
    await write_claim(reaped_claim)

    active_head = await get_active_head("claim-reaped-1")
    assert active_head is None
