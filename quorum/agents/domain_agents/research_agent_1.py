"""
quorum/agents/domain_agents/research_agent_1.py
------------------------------------------------
Research Agent 1: Literature & Prior Art Researcher.

Searches academic databases and literature sources for prior art, published
research, and established theory relevant to the research question.

Step loop
---------
1. **sense()** the claims namespace — observe what subtasks other agents have
   already claimed to avoid duplication.
2. **claim()** this agent's subtask: "literature search for <question>".
3. **[GATE]** ``harness.assert_can_call_external()`` — hard enforcement point.
4. **tavily_search()** for academic / literature sources.
5. **inference_router.complete()** to synthesise the raw search results into
   a structured finding.
6. **write_finding()** to the findings namespace so other agents can build on it.

The agent returns ``done=True`` after successfully publishing a finding, since
its role for this run is complete after one step.
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

_AGENT_ID = "literature-researcher"
_PARTICIPANT_TYPE: ParticipantType = "agent"

# Tavily search query template.
_SEARCH_TEMPLATE = "{question} academic literature review prior art research papers"

# LLM synthesis prompt.
_SYNTHESIS_PROMPT = """\
You are a literature researcher summarising academic prior art.

Research question: {question}

Search results:
{search_results}

Produce a concise synthesis (300-500 words) that:
1. Identifies the most relevant prior work.
2. Notes the key methodologies used in the literature.
3. Highlights any consensus or dominant theories.
4. Explicitly flags gaps or open questions that remain unresolved.

Write in plain prose. Do not invent citations not present in the search results.
"""


class LiteratureResearcher(BaseAgent):
    """Domain agent 1: searches for prior art and academic literature.

    Inherits the full step loop, heartbeat, and run infrastructure from
    :class:`~agents.base_agent.BaseAgent`.  All external IO (search and
    inference) is gated behind the :class:`~agents.harness.StepHarness`.
    """

    def __init__(self, run_id: str) -> None:
        super().__init__(
            agent_id=_AGENT_ID,
            run_id=run_id,
            participant_type=_PARTICIPANT_TYPE,
        )

    async def step(self, context: dict) -> dict:
        """Execute one literature-research step.

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

        harness = StepHarness(self.agent_id, run_id)

        # ----------------------------------------------------------------
        # 1. sense() — observe the claims namespace
        # ----------------------------------------------------------------
        log.info("[%s] Step %d: sensing claims namespace.", self.agent_id, step_number)
        sense_result = await sense(
            query=question,
            namespace="claims",
            n_results=10,
        )
        harness.mark_sensed()

        existing_count = len(sense_result.results) if sense_result.results else 0
        log.debug("[%s] Sensed %d existing claims.", self.agent_id, existing_count)

        # ----------------------------------------------------------------
        # 2. claim() — assert this agent's subtask on the board
        # ----------------------------------------------------------------
        claim_content = (
            f"[LiteratureResearcher] Performing academic literature search "
            f"for: {question[:200]}"
        )
        await self._make_claim(claim_content)
        harness.mark_claimed()

        # ----------------------------------------------------------------
        # 3. GATE — external calls are now permitted
        # ----------------------------------------------------------------
        harness.assert_can_call_external()

        # ----------------------------------------------------------------
        # 4. tavily_search() — retrieve academic / literature sources
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
        # 5. inference_router.complete() — synthesise results
        # ----------------------------------------------------------------
        formatted_results = _format_search_results(search_results)
        synthesis_prompt = _SYNTHESIS_PROMPT.format(
            question=question,
            search_results=formatted_results,
        )

        log.info("[%s] Requesting synthesis from inference router.", self.agent_id)
        try:
            synthesis: str = await inference_router.complete(
                prompt=synthesis_prompt,
                max_tokens=700,
            )
        except Exception:
            log.exception("[%s] Inference router failed.", self.agent_id)
            return {"done": False, "error": "inference_router.complete failed"}

        # ----------------------------------------------------------------
        # 6. write_finding() — publish to findings namespace
        # ----------------------------------------------------------------
        sources: list[str] = _extract_sources(search_results)
        finding = Finding(
            run_id=run_id,
            participant_id=self.agent_id,
            participant_type=self.participant_type,
            title=f"Literature Review: {question[:100]}",
            content=synthesis,
            sources=sources,
        )

        log.info("[%s] Writing finding %s.", self.agent_id, finding.id)
        try:
            await write_finding(finding)
        except Exception:
            log.exception("[%s] write_finding failed.", self.agent_id)
            return {"done": False, "error": "write_finding failed"}

        harness.mark_done()
        log.info("[%s] Step complete — finding published: %s", self.agent_id, finding.id)
        return {"done": True, "finding_id": finding.id}


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


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
