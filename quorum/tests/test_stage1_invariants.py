"""
tests/test_stage1_invariants.py
--------------------------------
Comprehensive test suite for Stage 1 Quorum invariants (Tests A through R).
Proves:
1. Hard fail-closed enforcement across environments.
2. Typed CoordinationToken capabilities and external work firewall.
3. Authoritative ClaimDecisionEngine with integrated tiebreaking.
4. Exactly-one ownership under high concurrency (2, 10, 20, 100 agents).
5. Absorption of duplicates and completed findings (zero unauthorized external calls).
6. Lease safety and Reaper stale worker rejection.
"""

import asyncio
import time
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from agents.coordination_token import (
    CoordinationToken,
    UnapprovedExternalWorkError,
    bound_coordination_token,
    verify_coordination_capability,
)
from agents.harness import StepHarness, StepOrderViolation, StepState
from bus.claim_engine import ClaimDecisionEngine, ClaimResolutionOutcome
from bus.moss_client import (
    claim,
    get_claim,
    resolve_and_claim,
    update_claim_status,
    write_claim,
    write_finding,
)
from bus.namespaces import get_claims_namespace, get_findings_namespace
from bus.schemas import Claim, Finding
from config.settings import settings
from reaper.reaper import reaper
from search.tavily_client import tavily_search
from tests.fake_external_provider import fake_provider


@pytest.fixture(autouse=True)
def reset_test_state():
    """Reset fake provider, collections, and settings before each test."""
    fake_provider.reset()
    yield
    fake_provider.reset()


# ---------------------------------------------------------------------------
# Test A: Production StepOrderViolation
# ---------------------------------------------------------------------------
def test_a_production_step_order_violation():
    """StepOrderViolation must block invalid execution in production (hard fail closed)."""
    orig_env = settings.ENVIRONMENT
    try:
        settings.ENVIRONMENT = "production"
        harness = StepHarness(participant_id="prod-agent", run_id="run-prod")

        # Attempting external call in INIT state must raise StepOrderViolation
        with pytest.raises(StepOrderViolation) as exc_info:
            harness.assert_can_call_external()
        assert "INIT" in str(exc_info.value)

        # Attempting claim before sense in production must raise StepOrderViolation
        with pytest.raises(StepOrderViolation) as exc_info:
            harness.mark_claimed()
        assert "expected SENSED" in str(exc_info.value)
    finally:
        settings.ENVIRONMENT = orig_env


# ---------------------------------------------------------------------------
# Test B: Unauthorized Tavily Call
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_b_unauthorized_tavily_call():
    """Calling tavily_search without a valid CoordinationToken must raise UnapprovedExternalWorkError."""
    with pytest.raises(UnapprovedExternalWorkError) as exc_info:
        await tavily_search(query="unauthorized test query")
    assert "No coordination capability present" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test C: Unauthorized Inference Call
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_c_unauthorized_inference_call():
    """Calling inference_router without a valid CoordinationToken must raise UnapprovedExternalWorkError."""
    from inference.router import inference_router

    with pytest.raises(UnapprovedExternalWorkError) as exc_info:
        await inference_router.complete(prompt="unauthorized prompt")
    assert "No coordination capability present" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test D: Valid Authorized External Call
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_d_valid_authorized_external_call():
    """With a valid CoordinationToken bound to the context, external execution is approved."""
    harness = StepHarness(participant_id="agent-auth", run_id="run-auth")
    harness.mark_sensed()

    c = Claim(
        run_id="run-auth",
        participant_id="agent-auth",
        participant_type="agent",
        content="Unique authorization capability verification claim",
    )
    token = harness.mark_claimed(claim=c)
    assert token is not None

    harness.assert_can_call_external()

    # Execute fake external work within the authorized capability context
    with bound_coordination_token(token):
        res = await fake_provider.execute_inference(prompt="Synthesize authorized finding")
        assert "Synthesized" in res
        assert fake_provider.call_count == 1

    harness.mark_done()


# ---------------------------------------------------------------------------
# Test E: Identical Claim
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_e_identical_claim():
    """Identical claims: first claim wins, second is rejected as duplicate."""
    run_id = "run-identical-test"
    content = "Identical research thesis on distributed Byzantine agreement."

    c1 = Claim(run_id=run_id, participant_id="agent-1", participant_type="agent", content=content)
    dec1 = await resolve_and_claim(c1)
    assert dec1.outcome == ClaimResolutionOutcome.NO_CONFLICT
    assert dec1.is_duplicate is False

    c2 = Claim(run_id=run_id, participant_id="agent-2", participant_type="agent", content=content)
    dec2 = await resolve_and_claim(c2)
    assert dec2.is_duplicate is True
    assert dec2.reason == "duplicate_claim"


# ---------------------------------------------------------------------------
# Test F: Semantic Duplicate
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_f_semantic_duplicate():
    """Semantically equivalent claims: second claim is recognized as duplicate."""
    run_id = "run-sem-dup-test"
    c1 = Claim(
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Reinforcement learning from human feedback aligns large language models with user intent.",
    )
    dec1 = await resolve_and_claim(c1)
    assert dec1.is_duplicate is False

    c2 = Claim(
        run_id=run_id,
        participant_id="agent-2",
        participant_type="agent",
        content="Reinforcement learning from human feedback aligns language models with user intent.",
    )
    dec2 = await resolve_and_claim(c2)
    assert dec2.is_duplicate is True
    assert dec2.reason == "duplicate_claim"


# ---------------------------------------------------------------------------
# Test G: Unrelated Claims
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_g_unrelated_claims():
    """Semantically distinct claims both acquire ownership."""
    run_id = "run-unrelated-test"
    c1 = Claim(
        run_id=run_id,
        participant_id="agent-1",
        participant_type="agent",
        content="Permafrost thaw in Arctic tundra releases methane gas.",
    )
    dec1 = await resolve_and_claim(c1)
    assert dec1.is_duplicate is False

    c2 = Claim(
        run_id=run_id,
        participant_id="agent-2",
        participant_type="agent",
        content="Quantum error correction using topological surface codes.",
    )
    dec2 = await resolve_and_claim(c2)
    assert dec2.is_duplicate is False


# ---------------------------------------------------------------------------
# Test H: Completed Finding Absorption
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_h_completed_finding_absorption():
    """If a semantically equivalent finding already exists, claim is absorbed with zero external work."""
    run_id = "run-finding-absorb"
    finding = Finding(
        run_id=run_id,
        participant_id="prior-researcher",
        participant_type="agent",
        title="CRISPR Mechanisms",
        content="CRISPR-Cas9 base editing enables single-nucleotide corrections without double-strand breaks.",
    )
    await write_finding(finding)

    challenger = Claim(
        run_id=run_id,
        participant_id="agent-late",
        participant_type="agent",
        content="CRISPR-Cas9 base editing enables single-nucleotide modifications without double-strand breaks.",
    )
    decision = await resolve_and_claim(challenger)
    assert decision.outcome == ClaimResolutionOutcome.COMPLETED_FINDING
    assert decision.is_duplicate is True
    assert decision.existing_finding_id == finding.id


# ---------------------------------------------------------------------------
# Test I, J, K, L: Concurrent Agent Races (2, 10, 20, 100 Agents)
# ---------------------------------------------------------------------------
async def _run_concurrent_race(n_agents: int, task_name: str) -> dict:
    """Helper executing n concurrent agents on identical semantic work.

    Asserts:
    - exactly one agent wins ownership
    - external_call_count == 1
    """
    run_id = f"run-race-{n_agents}-{task_name}"
    content = f"Investigating optimization landscape of non-convex neural objectives: {task_name}"

    winners = []
    absorbed = []

    async def agent_worker(agent_idx: int):
        agent_id = f"agent-{agent_idx:03d}"
        harness = StepHarness(participant_id=agent_id, run_id=run_id)

        # 1. Sense
        harness.mark_sensed()

        # 2. Claim
        challenger = Claim(
            run_id=run_id,
            participant_id=agent_id,
            participant_type="agent",
            content=content,
        )
        decision = await resolve_and_claim(challenger)

        if decision.is_duplicate:
            harness.mark_claim_result(is_duplicate=True)
            absorbed.append(agent_id)
            return {"agent_id": agent_id, "absorbed": True}
        else:
            token = harness.mark_claim_result(is_duplicate=False, claim=challenger)
            winners.append(agent_id)

            # 3. Gate & External work
            harness.assert_can_call_external()
            with bound_coordination_token(token):
                await fake_provider.execute_inference(prompt=f"Work from {agent_id}")

            harness.mark_done()
            return {"agent_id": agent_id, "absorbed": False}

    tasks = [asyncio.create_task(agent_worker(i)) for i in range(n_agents)]
    results = await asyncio.gather(*tasks)

    return {
        "winners": winners,
        "absorbed": absorbed,
        "results": results,
    }


@pytest.mark.asyncio
async def test_i_2_agent_race():
    """2-agent concurrent race: exactly 1 owner, 1 external call."""
    res = await _run_concurrent_race(2, "two-agents")
    assert len(res["winners"]) == 1
    assert len(res["absorbed"]) == 1
    assert fake_provider.call_count == 1


@pytest.mark.asyncio
async def test_j_10_agent_race():
    """10-agent concurrent race: exactly 1 owner, 1 external call."""
    res = await _run_concurrent_race(10, "ten-agents")
    assert len(res["winners"]) == 1
    assert len(res["absorbed"]) == 9
    assert fake_provider.call_count == 1


@pytest.mark.asyncio
async def test_k_20_agent_race():
    """20-agent concurrent race: exactly 1 owner, 1 external call."""
    res = await _run_concurrent_race(20, "twenty-agents")
    assert len(res["winners"]) == 1
    assert len(res["absorbed"]) == 19
    assert fake_provider.call_count == 1


@pytest.mark.asyncio
async def test_l_100_agent_race():
    """100-agent concurrent race: exactly 1 owner, 1 external call (THE STRONGEST TEST)."""
    res = await _run_concurrent_race(100, "hundred-agents")
    assert len(res["winners"]) == 1, f"Expected 1 winner, got {len(res['winners'])}: {res['winners']}"
    assert len(res["absorbed"]) == 99
    assert fake_provider.call_count == 1, f"Expected 1 external call, got {fake_provider.call_count}"


# ---------------------------------------------------------------------------
# Test M: Human / Agent Tiebreak
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_m_human_agent_tiebreak():
    """Human claim within 50ms epsilon window supersedes competing agent claim."""
    run_id = "run-human-tiebreak"
    content = "Deciding final ethical guidelines for autonomous drone flight."

    t0 = time.monotonic()
    now = datetime.now(timezone.utc)

    # 1. Agent writes claim first
    agent_claim = Claim(
        id="claim-agent-incumbent",
        run_id=run_id,
        participant_id="agent-001",
        participant_type="agent",
        content=content,
        created_at=now,
        ts_monotonic=t0,
    )
    await write_claim(agent_claim)

    # 2. Human challenger arrives 20ms later (within 50ms epsilon window)
    human_claim = Claim(
        id="claim-human-challenger",
        run_id=run_id,
        participant_id="human-operator",
        participant_type="human",
        content=content,
        created_at=now + timedelta(milliseconds=20),
        ts_monotonic=t0 + 0.020,
    )

    decision = await resolve_and_claim(human_claim)
    assert decision.outcome == ClaimResolutionOutcome.CHALLENGER_WINS
    assert decision.is_duplicate is False
    assert decision.superseded_claim_id == agent_claim.id

    # Verify incumbent agent claim is now superseded in ChromaDB
    incumbent_record = await get_claim(agent_claim.id)
    assert incumbent_record["metadata"]["status"] == "superseded"


# ---------------------------------------------------------------------------
# Test N: Agent / Agent Tiebreak
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_n_agent_agent_tiebreak():
    """Identical timestamps between same tier: lower participant_id wins deterministically."""
    run_id = "run-agent-tiebreak"
    content = "Exploring memory hierarchy optimization for sparse tensor operations."

    t0 = time.monotonic()
    now = datetime.now(timezone.utc)

    # agent-b wrote first
    claim_b = Claim(
        id="claim-b",
        run_id=run_id,
        participant_id="agent-b",
        participant_type="agent",
        content=content,
        created_at=now,
        ts_monotonic=t0,
    )
    await write_claim(claim_b)

    # agent-a (lexicographically lower) arrives within epsilon window
    claim_a = Claim(
        id="claim-a",
        run_id=run_id,
        participant_id="agent-a",
        participant_type="agent",
        content=content,
        created_at=now,
        ts_monotonic=t0,
    )

    decision = await resolve_and_claim(claim_a)
    # 'agent-a' < 'agent-b' => challenger wins tiebreak
    assert decision.outcome == ClaimResolutionOutcome.CHALLENGER_WINS
    assert decision.is_duplicate is False


# ---------------------------------------------------------------------------
# Test O: Stale Capability after Claim Expiration
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_o_stale_capability_after_expiration():
    """An expired CoordinationToken must be rejected by the external work firewall."""
    now = datetime.now(timezone.utc)
    expired_token = CoordinationToken(
        run_id="run-expired",
        task_id="task-1",
        claim_id="claim-1",
        owner_id="agent-1",
        version="1.0",
        issued_at=now - timedelta(seconds=30),
        expires_at=now - timedelta(seconds=5),  # expired 5s ago
        status="active",
    )

    with pytest.raises(UnapprovedExternalWorkError) as exc_info:
        await fake_provider.execute_inference(prompt="Expired attempt", coordination_token=expired_token)
    assert "expired" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test P: Reaper + Stale Worker
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_p_reaper_plus_stale_worker():
    """If the Reaper reaps an expired claim, the worker's capability is revoked."""
    run_id = "run-reaper-stale"
    content = "Long running synthesis that crashes before heartbeating."

    t_expired = (datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat()
    col = get_claims_namespace()
    claim_id = "claim-reaped-worker"
    col.add(
        ids=[claim_id],
        documents=[content],
        metadatas=[{
            "run_id": run_id,
            "participant_id": "stale-worker",
            "participant_type": "agent",
            "status": "active",
            "created_at": t_expired,
            "last_heartbeat": t_expired,
            "supersedes": "",
            "reap_reason": "",
            "ts_monotonic": time.monotonic() - 30,
        }],
    )

    # Reaper runs and reaps the claim
    reaped = await reaper.scan_once()
    assert reaped >= 1

    # Worker holding a previously issued token attempts external call
    now = datetime.now(timezone.utc)
    token = CoordinationToken(
        run_id=run_id,
        task_id="task-stale",
        claim_id=claim_id,
        owner_id="stale-worker",
        version="1.0",
        issued_at=now - timedelta(seconds=20),
        expires_at=now + timedelta(seconds=60),  # time window unexpired, but claim is reaped!
        status="active",
    )

    with pytest.raises(UnapprovedExternalWorkError) as exc_info:
        await fake_provider.execute_inference(prompt="Attempt after reaped", coordination_token=token)
    assert "no longer active" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Test Q: Duplicate Completion
# ---------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_q_duplicate_completion():
    """Agent A completes work and publishes finding; Agent B claiming same topic gets absorbed."""
    run_id = "run-dup-completion"
    task_topic = "Evaluation of sparse mixture-of-experts throughput per FLOP"

    # Agent A executes and publishes finding
    f = Finding(
        run_id=run_id,
        participant_id="agent-a",
        participant_type="agent",
        title="MoE FLOP Analysis",
        content=task_topic,
    )
    await write_finding(f)

    # Agent B arrives to claim
    c_b = Claim(
        run_id=run_id,
        participant_id="agent-b",
        participant_type="agent",
        content=task_topic,
    )
    decision = await resolve_and_claim(c_b)
    assert decision.outcome == ClaimResolutionOutcome.COMPLETED_FINDING
    assert decision.is_duplicate is True
    assert decision.existing_finding_id == f.id


# ---------------------------------------------------------------------------
# Test R: Invalid State Transitions
# ---------------------------------------------------------------------------
def test_r_invalid_state_transitions():
    """StepHarness must reject any out-of-order state transition."""
    harness = StepHarness(participant_id="agent-r", run_id="run-r")

    # mark_claimed without mark_sensed
    with pytest.raises(StepOrderViolation):
        harness.mark_claimed()

    # mark_done without external_allowed
    with pytest.raises(StepOrderViolation):
        harness.mark_done()

    # sense -> duplicate -> assert_can_call_external
    harness.mark_sensed()
    harness.mark_claim_result(is_duplicate=True)
    assert harness.state == StepState.DONE
    with pytest.raises(StepOrderViolation):
        harness.assert_can_call_external()
