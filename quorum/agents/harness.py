"""
quorum/agents/harness.py
------------------------
Enforcement point for the per-step ordering contract defined in §9.1.

Contract (enforced at runtime in dev/test, logged-only in production):

    1. sense()   MUST be called first  (INIT → SENSED)
    2. claim()   MUST resolve before any external call (SENSED → CLAIMED → EXTERNAL_ALLOWED)
    3. External calls (inference_router, tavily_client) are only permitted
       AFTER claim() resolves.

In ``ENVIRONMENT='development'`` or ``'test'`` a violation raises
:class:`StepOrderViolation` immediately — it will not silently continue.
In ``'production'`` violations are logged as errors so the run can survive
a rare race, but every violation should be treated as a bug.

Usage::

    harness = StepHarness(participant_id="agent-1", run_id="run-abc")

    harness.mark_sensed()                   # after sense()
    harness.mark_claimed()                  # after claim()
    harness.assert_can_call_external()      # gate before any external IO
    ...
    harness.mark_done()
    harness.reset()                         # reuse for the next step
"""

import asyncio
import logging
from enum import Enum, auto
from typing import Any, Callable, Optional
from functools import wraps

from config.settings import settings

from datetime import datetime, timedelta, timezone
from agents.coordination_token import (
    CoordinationToken,
    UnapprovedExternalWorkError,
    bound_coordination_token,
)

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# State machine states
# ---------------------------------------------------------------------------


class StepState(Enum):
    """Ordered states a single agent step may pass through."""

    INIT = auto()
    """Step created; sense() has not yet been called."""

    SENSED = auto()
    """sense() completed; claim() has not yet been called."""

    CLAIMED = auto()
    """claim() has been written; transitioning to EXTERNAL_ALLOWED immediately."""

    EXTERNAL_ALLOWED = auto()
    """Gate open: external calls (inference, search) are now permitted."""

    DONE = auto()
    """Step completed; harness may be reset for the next step."""


# ---------------------------------------------------------------------------
# Exception
# ---------------------------------------------------------------------------


class StepOrderViolation(RuntimeError):
    """Raised when the sense → claim → external ordering contract is violated.

    Hard fail-closed: raised in all environments (development, test, and production).
    """


# ---------------------------------------------------------------------------
# Core harness
# ---------------------------------------------------------------------------


class StepHarness:
    """Enforces the sense() → claim() → external-calls ordering per §9.1.

    One instance should be created per step (or reused via :meth:`reset`).

    Parameters
    ----------
    participant_id:
        The ID of the agent or participant owning this step.
    run_id:
        The run this step belongs to (for log context).
    """

    def __init__(self, participant_id: str, run_id: str) -> None:
        self.participant_id = participant_id
        self.run_id = run_id
        self._state: StepState = StepState.INIT
        self.current_token: Optional[CoordinationToken] = None

    # ------------------------------------------------------------------
    # State transitions
    # ------------------------------------------------------------------

    def mark_sensed(self) -> None:
        """Transition INIT → SENSED.  Must be called immediately after sense()."""
        self.assert_can_sense()
        self._state = StepState.SENSED
        log.debug(
            "[harness] %s run=%s INIT → SENSED",
            self.participant_id,
            self.run_id,
        )

    def mark_claim_result(
        self,
        *,
        is_duplicate: bool = False,
        claim: Any = None,
        claim_id: Optional[str] = None,
        task_id: Optional[str] = None,
        ttl_seconds: Optional[float] = None,
    ) -> Optional[CoordinationToken]:
        """Record the outcome of claim().

        If is_duplicate is True, the agent absorbed an existing finding or
        claim and must not execute subsequent external calls; transitions to DONE.
        If is_duplicate is False, transitions to EXTERNAL_ALLOWED, opening the gate
        and issuing an explicit CoordinationToken capability.
        """
        self.assert_can_claim()
        if is_duplicate:
            self._state = StepState.DONE
            self.current_token = None
            log.debug(
                "[harness] %s run=%s SENSED → DONE (duplicate claim absorbed)",
                self.participant_id,
                self.run_id,
            )
            return None
        else:
            self._state = StepState.EXTERNAL_ALLOWED
            cid = getattr(claim, "id", None) or claim_id or f"claim-{self.participant_id}"
            tid = task_id or f"task-{self.run_id}-{self.participant_id}"
            now = datetime.now(timezone.utc)
            ttl = ttl_seconds if ttl_seconds is not None else float(settings.HEARTBEAT_TTL_SECONDS)
            expires = now + timedelta(seconds=ttl)
            version = str(getattr(claim, "ts_monotonic", now.timestamp()))

            self.current_token = CoordinationToken(
                run_id=self.run_id,
                task_id=tid,
                claim_id=cid,
                owner_id=self.participant_id,
                version=version,
                issued_at=now,
                expires_at=expires,
                status="active",
            )
            log.debug(
                "[harness] %s run=%s SENSED → EXTERNAL_ALLOWED (claim %s authorized)",
                self.participant_id,
                self.run_id,
                cid,
            )
            return self.current_token

    def mark_claimed(
        self,
        claim: Any = None,
        claim_id: Optional[str] = None,
    ) -> Optional[CoordinationToken]:
        """Backward-compatible alias for mark_claim_result(is_duplicate=False)."""
        return self.mark_claim_result(is_duplicate=False, claim=claim, claim_id=claim_id)

    def mark_done(self) -> None:
        """Transition to DONE. Must only be called after external work completed (EXTERNAL_ALLOWED)."""
        self._enforce(
            self._state in (StepState.EXTERNAL_ALLOWED, StepState.DONE),
            f"[{self.participant_id}] mark_done() called in state {self._state.name}; "
            "expected EXTERNAL_ALLOWED.",
        )
        self._state = StepState.DONE
        self.current_token = None
        log.debug(
            "[harness] %s run=%s → DONE",
            self.participant_id,
            self.run_id,
        )

    def reset(self) -> None:
        """Reset the harness to INIT for the next step.

        Useful when a single :class:`StepHarness` instance is reused across
        multiple steps (e.g. inside a :class:`~agents.base_agent.BaseAgent`
        run loop).
        """
        self._state = StepState.INIT
        self.current_token = None
        log.debug(
            "[harness] %s run=%s reset → INIT",
            self.participant_id,
            self.run_id,
        )

    # ------------------------------------------------------------------
    # Assertions
    # ------------------------------------------------------------------

    def assert_can_sense(self) -> None:
        """Assert that sense() is permitted (state must be INIT).

        Raises
        ------
        StepOrderViolation
            When called after the step has already been sensed.
        """
        self._enforce(
            self._state == StepState.INIT,
            f"[{self.participant_id}] sense() called in state {self._state.name}; "
            "sense() must be the very first call in a step (expected INIT).",
        )

    def assert_can_claim(self) -> None:
        """Assert that claim() is permitted (state must be SENSED).

        Raises
        ------
        StepOrderViolation
            When claim() is called without a prior sense().
        """
        self._enforce(
            self._state == StepState.SENSED,
            f"[{self.participant_id}] claim() called in state {self._state.name}; "
            "sense() must complete before claim() (expected SENSED).",
        )

    def assert_can_call_external(self) -> CoordinationToken:
        """Assert that an external call is permitted (state must be EXTERNAL_ALLOWED).

        This is the primary gate that prevents agents from calling
        ``inference_router`` or ``tavily_client`` before ``claim()`` resolves,
        or after duplicate claim absorption / step completion.

        Returns
        -------
        CoordinationToken
            The active coordination capability bound to this claim.

        Raises
        ------
        StepOrderViolation
            When an external call is attempted in an invalid state.
        UnapprovedExternalWorkError
            When the coordination capability has expired or is revoked.
        """
        allowed = self._state == StepState.EXTERNAL_ALLOWED
        self._enforce(
            allowed,
            f"[{self.participant_id}] External call attempted in state {self._state.name}; "
            "claim() must resolve as non-duplicate before any external IO (inference/search).",
        )

        if self.current_token is None or not self.current_token.is_valid():
            exp_str = self.current_token.expires_at.isoformat() if self.current_token else "none"
            status_str = self.current_token.status if self.current_token else "missing"
            raise UnapprovedExternalWorkError(
                f"[{self.participant_id}] External call attempted with invalid/expired coordination capability: "
                f"status={status_str}, expires_at={exp_str}."
            )

        return self.current_token

    # ------------------------------------------------------------------
    # Internal enforcement
    # ------------------------------------------------------------------

    def _enforce(self, condition: bool, message: str) -> None:
        """Fail closed in all environments.

        Parameters
        ----------
        condition:
            When ``True`` the contract is satisfied — nothing happens.
            When ``False``, :class:`StepOrderViolation` is raised immediately.
        message:
            Human-readable description of the violation.
        """
        if condition:
            return

        log.error("[harness:VIOLATION] %s", message)
        raise StepOrderViolation(message)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def state(self) -> StepState:
        """Read-only view of the current state."""
        return self._state

    def __repr__(self) -> str:
        return (
            f"StepHarness(participant_id={self.participant_id!r}, "
            f"run_id={self.run_id!r}, state={self._state.name})"
        )


# ---------------------------------------------------------------------------
# step_guard decorator
# ---------------------------------------------------------------------------


def step_guard(harness_attr: str = "harness") -> Callable:
    """Decorator factory for agent methods that make external calls.

    Wraps an ``async`` method so that ``harness.assert_can_call_external()``
    is called automatically before the wrapped method body executes.  The
    harness is resolved from ``self.<harness_attr>``.

    Parameters
    ----------
    harness_attr:
        Name of the attribute on ``self`` that holds the :class:`StepHarness`
        instance.  Defaults to ``'harness'``.

    Example::

        class MyAgent(BaseAgent):
            self.harness = StepHarness(self.agent_id, run_id)

            @step_guard('harness')
            async def _fetch_sources(self, query: str) -> list[str]:
                return await tavily_search(query)

    Raises
    ------
    StepOrderViolation
        Propagated from :meth:`StepHarness.assert_can_call_external` when
        the ordering contract is violated in dev/test.
    AttributeError
        If ``harness_attr`` is not found on the decorated method's ``self``.
    """

    def decorator(fn: Callable) -> Callable:
        if not asyncio.iscoroutinefunction(fn):
            raise TypeError(
                f"@step_guard can only decorate async functions; "
                f"{fn.__qualname__} is not async."
            )

        @wraps(fn)
        async def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:
            harness: StepHarness = getattr(self, harness_attr)
            token = harness.assert_can_call_external()
            with bound_coordination_token(token):
                return await fn(self, *args, **kwargs)

        return wrapper

    return decorator


# ---------------------------------------------------------------------------
# Inline usage examples (run as a module for quick verification)
# ---------------------------------------------------------------------------

# Example 1 — happy path (no violation):
#
#   harness = StepHarness("agent-1", "run-001")
#   harness.mark_sensed()
#   harness.mark_claimed()
#   harness.assert_can_call_external()   # passes — EXTERNAL_ALLOWED
#   harness.mark_done()
#
# Example 2 — violation in dev/test:
#
#   harness = StepHarness("agent-1", "run-001")
#   # Forgot to call mark_sensed() and mark_claimed()
#   harness.assert_can_call_external()   # raises StepOrderViolation
#
# Example 3 — sense before claim violation:
#
#   harness = StepHarness("agent-1", "run-001")
#   harness.mark_sensed()
#   harness.assert_can_call_external()   # raises StepOrderViolation (still SENSED)
#
# Example 4 — reset between steps:
#
#   harness = StepHarness("agent-1", "run-001")
#   harness.mark_sensed()
#   harness.mark_claimed()
#   harness.mark_done()
#   harness.reset()
#   assert harness.state == StepState.INIT
#
# Example 5 — @step_guard decorator:
#
#   class Agent:
#       def __init__(self):
#           self.harness = StepHarness("agent-1", "run-001")
#
#       @step_guard('harness')
#       async def call_llm(self):
#           return await inference_router.complete(...)
#
#   agent = Agent()
#   await agent.call_llm()   # raises StepOrderViolation — claim not yet done
