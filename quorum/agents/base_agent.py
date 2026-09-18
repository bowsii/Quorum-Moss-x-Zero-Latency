"""
quorum/agents/base_agent.py
----------------------------
Abstract base class for all domain agents in the Quorum system.

Every concrete agent inherits from :class:`BaseAgent` and implements the
:meth:`step` method which must follow the sense → claim → external-calls
contract enforced by :class:`~agents.harness.StepHarness`.

Responsibilities
----------------
- Runs a background heartbeat coroutine every 5 seconds so the Reaper knows
  the agent is alive.
- Drives the top-level ``run()`` loop: up to ``max_steps`` iterations of
  ``step()``, stopping early if the step signals completion.
- Provides ``_make_claim()`` as a convenience wrapper around
  :func:`~bus.moss_client.write_claim` that keeps ``current_claim_id`` up to date.

Usage::

    class MyAgent(BaseAgent):
        async def step(self, context: dict) -> dict:
            harness = StepHarness(self.agent_id, context['run_id'])

            result = await sense(context['question'])
            harness.mark_sensed()

            claim = await self._make_claim(f"Researching: {context['question']}")
            harness.mark_claimed()

            harness.assert_can_call_external()
            findings = await tavily_search(context['question'])

            return {'findings': findings, 'done': False}

    agent = MyAgent(agent_id='my-agent', run_id='run-001')
    await agent.run(run_id='run-001', question='What is...')
"""

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Optional

from bus.moss_client import sense, write_claim, heartbeat
from bus.schemas import Claim, ParticipantType, SenseResult
from agents.harness import StepHarness
from config.settings import settings

log = logging.getLogger(__name__)

_HEARTBEAT_INTERVAL_SECONDS: float = 5.0


class BaseAgent(ABC):
    """Abstract base for all Quorum domain agents.

    Subclasses must implement :meth:`step`.  All other lifecycle concerns
    (heartbeat, loop control, claim bookkeeping) are handled here.

    Parameters
    ----------
    agent_id:
        Stable identifier for this agent (e.g. ``'literature-researcher'``).
    run_id:
        The run this agent is participating in.
    participant_type:
        Role label written into every :class:`~bus.schemas.Claim`.
        Defaults to ``'agent'``.
    """

    def __init__(
        self,
        agent_id: str,
        run_id: str,
        participant_type: ParticipantType = "agent",
    ) -> None:
        self.agent_id: str = agent_id
        self.run_id: str = run_id
        self.participant_type: ParticipantType = participant_type

        # Mutable state — updated by _make_claim() and emit_heartbeat().
        self.current_claim_id: Optional[str] = None
        self._heartbeat_task: Optional[asyncio.Task] = None  # type: ignore[type-arg]

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    async def step(self, context: dict) -> dict:
        """Execute a single agent step.

        Implementors MUST follow the ordering contract:
          1. Call :func:`~bus.moss_client.sense` and then ``harness.mark_sensed()``.
          2. Call :meth:`_make_claim` and then ``harness.mark_claimed()``.
          3. Call ``harness.assert_can_call_external()`` before any
             ``inference_router`` or ``tavily_client`` call.

        Parameters
        ----------
        context:
            A dict containing at minimum:

            - ``'run_id'`` (str)
            - ``'question'`` (str): the research question driving this run
            - ``'step_number'`` (int): 0-based iteration index

        Returns
        -------
        dict
            Must contain at minimum ``'done'`` (bool).
            ``True`` signals the run loop to exit early.
        """

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    async def emit_heartbeat(self) -> None:
        """Emit a heartbeat for the current active claim.

        A no-op if :attr:`current_claim_id` has not been set yet (before the
        first claim is written).
        """
        if self.current_claim_id is None:
            log.debug(
                "[%s] emit_heartbeat: no active claim — skipping.",
                self.agent_id,
            )
            return

        try:
            await heartbeat(
                claim_id=self.current_claim_id,
                participant_id=self.agent_id,
            )
            log.debug(
                "[%s] heartbeat emitted for claim %s",
                self.agent_id,
                self.current_claim_id,
            )
        except Exception:
            log.exception(
                "[%s] heartbeat failed for claim %s",
                self.agent_id,
                self.current_claim_id,
            )

    async def start_heartbeat_loop(self) -> "asyncio.Task[None]":
        """Start a background task that emits heartbeats every 5 seconds.

        The task is stored on :attr:`_heartbeat_task` and returned so the
        caller can cancel it when the run ends.

        Returns
        -------
        asyncio.Task
            The background heartbeat task.
        """

        async def _loop() -> None:
            while True:
                await asyncio.sleep(_HEARTBEAT_INTERVAL_SECONDS)
                await self.emit_heartbeat()

        task: asyncio.Task[None] = asyncio.create_task(
            _loop(),
            name=f"heartbeat-{self.agent_id}",
        )
        self._heartbeat_task = task
        log.info(
            "[%s] Heartbeat loop started (interval=%ss).",
            self.agent_id,
            _HEARTBEAT_INTERVAL_SECONDS,
        )
        return task

    def _cancel_heartbeat(self) -> None:
        """Cancel the running heartbeat task, if any."""
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            log.debug("[%s] Heartbeat task cancelled.", self.agent_id)

    # ------------------------------------------------------------------
    # Main run loop
    # ------------------------------------------------------------------

    async def run(
        self,
        run_id: str,
        question: str,
        max_steps: int = 10,
    ) -> None:
        """Drive the agent through up to *max_steps* step iterations.

        Starts the heartbeat background task before the first step and
        cancels it after the loop exits (normally or via exception).

        Parameters
        ----------
        run_id:
            The active run identifier.
        question:
            The research question driving this run; forwarded to every step.
        max_steps:
            Hard upper bound on the number of :meth:`step` invocations.
        """
        log.info(
            "[%s] Starting run %s — question=%r, max_steps=%d",
            self.agent_id,
            run_id,
            question,
            max_steps,
        )
        self.run_id = run_id
        await self.start_heartbeat_loop()

        try:
            for step_number in range(max_steps):
                context: dict = {
                    "run_id": run_id,
                    "question": question,
                    "step_number": step_number,
                }

                log.info(
                    "[%s] Step %d/%d starting.",
                    self.agent_id,
                    step_number + 1,
                    max_steps,
                )

                try:
                    result = await self.step(context)
                except Exception:
                    log.exception(
                        "[%s] Step %d raised an unhandled exception — aborting run.",
                        self.agent_id,
                        step_number + 1,
                    )
                    raise

                if result.get("done", False):
                    log.info(
                        "[%s] Step %d signalled done — exiting loop early.",
                        self.agent_id,
                        step_number + 1,
                    )
                    break

        finally:
            self._cancel_heartbeat()
            log.info("[%s] Run %s complete.", self.agent_id, run_id)

    # ------------------------------------------------------------------
    # Claim helpers
    # ------------------------------------------------------------------

    async def _make_claim(self, content: str) -> Claim:
        """Build, write, and return a :class:`~bus.schemas.Claim`.

        Sets :attr:`current_claim_id` to the new claim's ID so that
        :meth:`emit_heartbeat` can reference it immediately.

        Parameters
        ----------
        content:
            The natural-language content of the claim (max 4096 chars).

        Returns
        -------
        Claim
            The persisted claim object.
        """
        claim = Claim(
            run_id=self.run_id,
            participant_id=self.agent_id,
            participant_type=self.participant_type,
            content=content,
        )
        await write_claim(claim)
        self.current_claim_id = claim.id
        log.info(
            "[%s] Claim written: id=%s content=%.80r",
            self.agent_id,
            claim.id,
            content,
        )
        return claim

    # ------------------------------------------------------------------
    # Dunder helpers
    # ------------------------------------------------------------------

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"agent_id={self.agent_id!r}, "
            f"run_id={self.run_id!r}, "
            f"participant_type={self.participant_type!r})"
        )
