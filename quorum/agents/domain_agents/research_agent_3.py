"""
quorum/agents/domain_agents/research_agent_3.py
------------------------------------------------
Research Agent 3: Counterargument & Critique Researcher.

Focused on finding opposing views, documented criticisms, known limitations,
methodological flaws, and minority dissenting positions related to the
research question and the findings posted by other agents.

Step loop
---------
1. **sense()** the **findings** namespace — read what other agents have
   concluded so there is something to critique.
2. **claim()** this agent's subtask: "critiquing existing findings on <question>".
3. **[GATE]** ``harness.assert_can_call_external()`` — hard enforcement point.
4. **tavily_search()** targeting counterarguments, critiques, limitations.
5. **inference_router.complete()** to synthesise the critical perspective into
   a structured finding that explicitly references the claims being disputed.
6. **write_finding()** to the findings namespace (tagged as a critique).

Agent 3 has a deliberate 2-second startup delay to let Agents 1 and 2 publish
their findings first — there needs to be something on the board to critique.
"""

import asyncio
import logging
from typing import Optional

from agents.base_agent import BaseAgent
from agents.harness import StepHarness
from bus.moss_client import sense, write_finding
from bus.schemas import Finding, ParticipantType
from inference.router import inference_router
from search.tavily_client import tavily_search
from config.settings import settings

log = logging.getLogger(__name__)

_AGENT_ID = "critique-researcher"
_PARTICIPANT_TYPE: ParticipantType = "agent"

# Deliberate yield time (seconds) to let other agents post findings first.
_STARTUP_YIELD_SECONDS: float = 2.0

# Tavily query template biased toward critical and opposing perspectives.
_SEARCH_TEMPLATE = (
    "{question} counterargument criticism limitations critique opposing view "
    "methodological flaw rebuttal"
)

# LLM synthesis prompt for critiques.
_SYNTHESIS_PROMPT = """\
You are a critical researcher whose job is to find weaknesses in existing claims.

Research question: {question}

Existing findings from other agents:
{existing_findings}

External search results (counterarguments and critiques):
{search_results}

Produce a concise critical analysis (300-500 words) that:
1. Identifies the most significant counterarguments or limitations found.
2. Connects each critique specifically to the existing findings above (cite
   which finding it challenges).
3. Notes any methodological weaknesses, selection biases, or confounds.
4. Acknowledges if the criticisms are minority views or mainstream dissent.
5. Is intellectually honest: if the existing findings are well-supported,
   say so, but still note any caveats or boundary conditions.

Write in plain prose. Be precise and cite search results where relevant.
"""


class CritiqueResearcher(BaseAgent):
    """Domain agent 3: counterargument and critique researcher.

    Reads the findings namespace to understand what other agents have
    concluded, then searches for opposing evidence to challenge those
    conclusions.  All external IO is gated behind the
    :class:`~agents.harness.StepHarness` per §9.1.
    """

    def __init__(self, run_id: str) -> None:
        super().__init__(
            agent_id=_AGENT_ID,
            run_id=run_id,
            participant_type=_PARTICIPANT_TYPE,
        )

    async def step(self, context: dict) -> dict:
        """Execute one critique step.

        Waits briefly on the first step so Agents 1 and 2 have time to post
        their findings before this agent attempts to critique them.

        Parameters
        ----------
        context:
            Must contain ``'run_id'`` (str) and ``'question'`` (str).

        Returns
        -------
        dict
            ``{'done': True, 'finding_id': str}`` on success, or
            ``{'done': False, 'error': str}`` on a recoverable failure.
        """
        run_id: str = context["run_id"]
        question: str = context["question"]
        step_number: int = context.get("step_number", 0)

        # Brief yield on the first step — let other agents post first.
        if step_number == 0:
            log.info(
                "[%s] Waiting %.1fs for other agents to post findings first.",
                self.agent_id,
                _STARTUP_YIELD_SECONDS,
            )
            await asyncio.sleep(_STARTUP_YIELD_SECONDS)

        harness = StepHarness(self.agent_id, run_id)

        # ----------------------------------------------------------------
        # 1. sense() — read the FINDINGS namespace (not claims)
        # ----------------------------------------------------------------
        log.info(
            "[%s] Step %d: sensing findings namespace for content to critique.",
            self.agent_id,
            step_number,
        )
        sense_result = await sense(
            query=question,
            namespace="findings",
            n_results=10,
        )
        harness.mark_sensed()

        existing_findings: list[dict] = sense_result.results or []
        log.debug(
            "[%s] Sensed %d existing findings to critique.",
            self.agent_id,
            len(existing_findings),
        )

        # If no findings exist yet, defer to the next step.
        if not existing_findings:
            harness.mark_done()
            log.info(
                "[%s] No findings on board yet — deferring critique.",
                self.agent_id,
            )
            # Still need to mark_claimed before returning to satisfy the harness
            # in the next call, but for this early-exit we reset and skip.
            harness.reset()
            return {"done": False, "reason": "no_findings_yet"}

        # ----------------------------------------------------------------
        # 2. claim() — register this agent's critique sub-task
        # ----------------------------------------------------------------
        claim_content = (
            f"[CritiqueResearcher] Searching for counterarguments and critiques "
            f"for: {question[:200]}"
        )
        await self._make_claim(claim_content)
        harness.mark_claimed()

        # ----------------------------------------------------------------
        # 3. GATE — external calls are now permitted
        # ----------------------------------------------------------------
        harness.assert_can_call_external()

        # ----------------------------------------------------------------
        # 4. tavily_search() — retrieve counterarguments and criticisms
        # ----------------------------------------------------------------
        search_query = _SEARCH_TEMPLATE.format(question=question)
        log.info("[%s] Running Tavily search: %r", self.agent_id, search_query[:100])

        try:
            search_results: list[dict] = await tavily_search(
                query=search_query,
                max_results=8,
                search_depth="advanced",
            )
        except Exception:
            log.exception("[%s] Tavily search failed.", self.agent_id)
            return {"done": False, "error": "tavily_search failed"}

        # ----------------------------------------------------------------
        # 5. inference_router.complete() — build the critical synthesis
        # ----------------------------------------------------------------
        formatted_findings = _format_findings(existing_findings)
        formatted_results = _format_search_results(search_results)
        synthesis_prompt = _SYNTHESIS_PROMPT.format(
            question=question,
            existing_findings=formatted_findings,
            search_results=formatted_results,
        )

        log.info("[%s] Requesting critical synthesis from inference router.", self.agent_id)
        try:
            synthesis: str = await inference_router.complete(
                prompt=synthesis_prompt,
                max_tokens=700,
            )
        except Exception:
            log.exception("[%s] Inference router failed.", self.agent_id)
            return {"done": False, "error": "inference_router.complete failed"}

        # ----------------------------------------------------------------
        # 6. write_finding() — publish critique to the findings namespace
        # ----------------------------------------------------------------
        sources: list[str] = _extract_sources(search_results)
        finding = Finding(
            run_id=run_id,
            participant_id=self.agent_id,
            participant_type=self.participant_type,
            title=f"Critical Analysis: {question[:100]}",
            content=synthesis,
            sources=sources,
        )

        log.info("[%s] Writing critique finding %s.", self.agent_id, finding.id)
        try:
            await write_finding(finding)
        except Exception:
            log.exception("[%s] write_finding failed.", self.agent_id)
            return {"done": False, "error": "write_finding failed"}

        harness.mark_done()
        log.info(
            "[%s] Step complete — critique finding published: %s",
            self.agent_id,
            finding.id,
        )
        return {"done": True, "finding_id": finding.id}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _format_findings(findings: list[dict]) -> str:
    """Format sensed findings into a block for the LLM prompt."""
    if not findings:
        return "(no prior findings on board)"

    lines: list[str] = []
    for i, f in enumerate(findings, start=1):
        # ChromaDB returns documents as plain strings in results.
        content = f if isinstance(f, str) else f.get("content", str(f))
        lines.append(f"Finding {i}:\n{content[:600]}")

    return "\n\n".join(lines)


def _format_search_results(results: list[dict]) -> str:
    """Convert raw Tavily result dicts into a readable block for the LLM prompt."""
    if not results:
        return "(no results returned)"

    lines: list[str] = []
    for i, r in enumerate(results, start=1):
        title = r.get("title", "Untitled")
        url = r.get("url", "")
        snippet = r.get("content", r.get("snippet", ""))
        lines.append(f"{i}. [{title}]({url})\n   {snippet[:400]}")

    return "\n\n".join(lines)


def _extract_sources(results: list[dict]) -> list[str]:
    """Extract URL strings from Tavily result dicts."""
    return [r["url"] for r in results if r.get("url")]
