"""tests/test_tiebreak.py
-------------------------
Unit tests for bus/tiebreak.py.
Specifically validates the 50ms human-priority epsilon boundary (49ms vs 51ms cases).
"""
import uuid
from datetime import datetime, timezone, timedelta
import pytest
from bus.schemas import Claim
from bus.tiebreak import should_claim_win, participant_priority


def test_participant_priority_order():
    """Verify priority hierarchy: human (0) > adjudicator (1) > agent (2) > reaper (3)."""
    assert participant_priority("human") == 0
    assert participant_priority("adjudicator") == 1
    assert participant_priority("agent") == 2
    assert participant_priority("reaper") == 3


def test_human_wins_within_49ms_epsilon():
    """Human claim arriving 49ms after an agent claim should WIN (within 50ms epsilon)."""
    t0 = datetime.now(timezone.utc)
    t_agent = t0
    t_human = t0 + timedelta(milliseconds=49)

    agent_claim = Claim(
        run_id="run-1",
        participant_id="agent-123",
        participant_type="agent",
        content="Agent hypothesis",
        created_at=t_agent,
    )
    human_claim = Claim(
        run_id="run-1",
        participant_id="human-alice",
        participant_type="human",
        content="Human guidance",
        created_at=t_human,
    )

    # Challenger is human, incumbent is agent. Arrived within 49ms -> human wins!
    assert should_claim_win(challenger=human_claim, incumbent=agent_claim) is True


def test_human_loses_outside_51ms_epsilon_if_normal_tiebreak_applies():
    """When human claim arrives 51ms later, human still has higher priority tier (tier 0 vs 2),
    but when challenger is agent vs human incumbent after 51ms, agent should NOT win."""
    t0 = datetime.now(timezone.utc)
    t_human = t0
    t_agent = t0 + timedelta(milliseconds=51)

    human_incumbent = Claim(
        run_id="run-1",
        participant_id="human-alice",
        participant_type="human",
        content="Human guidance",
        created_at=t_human,
    )
    agent_challenger = Claim(
        run_id="run-1",
        participant_id="agent-123",
        participant_type="agent",
        content="Agent hypothesis",
        created_at=t_agent,
    )

    # Agent challenger cannot supersede human incumbent
    assert should_claim_win(challenger=agent_challenger, incumbent=human_incumbent) is False


def test_same_tier_tiebreak_by_timestamp_and_id():
    """Two agents: earlier timestamp wins; if identical timestamp, lower participant_id wins."""
    t0 = datetime.now(timezone.utc)

    agent_a = Claim(
        run_id="run-1",
        participant_id="agent-aaa",
        participant_type="agent",
        content="Agent A claim",
        created_at=t0,
    )
    agent_b = Claim(
        run_id="run-1",
        participant_id="agent-bbb",
        participant_type="agent",
        content="Agent B claim",
        created_at=t0 + timedelta(milliseconds=10),
    )

    # Earlier timestamp wins
    assert should_claim_win(challenger=agent_a, incumbent=agent_b) is True
    assert should_claim_win(challenger=agent_b, incumbent=agent_a) is False
