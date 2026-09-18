"""tests/test_harness.py
-----------------------
Unit tests for agents/harness.py.
Verifies that the ordering guard (sense -> claim -> gate -> external)
actually raises StepOrderViolation at runtime in dev/test mode.
"""
import pytest
from agents.harness import StepHarness, StepOrderViolation


def test_valid_step_ordering_succeeds():
    """Proper sequence: mark_sensed() -> mark_claimed() -> assert_can_call_external() -> mark_done()."""
    harness = StepHarness(participant_id="agent-test", run_id="run-test")
    harness.mark_sensed()
    harness.mark_claimed()

    # External call guard should pass without raising
    harness.assert_can_call_external()
    harness.mark_done()


def test_calling_external_before_sense_raises():
    """Attempting external call at initial state must raise StepOrderViolation."""
    harness = StepHarness(participant_id="agent-test", run_id="run-test")
    with pytest.raises(StepOrderViolation):
        harness.assert_can_call_external()


def test_calling_external_after_sense_before_claim_raises():
    """Attempting external call after sense() but before claim() must raise StepOrderViolation."""
    harness = StepHarness(participant_id="agent-test", run_id="run-test")
    harness.mark_sensed()
    with pytest.raises(StepOrderViolation):
        harness.assert_can_call_external()


def test_claiming_before_sense_raises():
    """Attempting to claim before sensing must raise StepOrderViolation."""
    harness = StepHarness(participant_id="agent-test", run_id="run-test")
    with pytest.raises(StepOrderViolation):
        harness.mark_claimed()


def test_harness_reset():
    """After reset(), the harness returns to INIT state."""
    harness = StepHarness(participant_id="agent-test", run_id="run-test")
    harness.mark_sensed()
    harness.mark_claimed()
    harness.mark_done()

    harness.reset()
    # Now asserting external must fail again
    with pytest.raises(StepOrderViolation):
        harness.assert_can_call_external()
