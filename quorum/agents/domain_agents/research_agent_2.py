"""
quorum/agents/domain_agents/research_agent_2.py
------------------------------------------------
Research Agent 2: Data & Statistics Analyst.

Focuses exclusively on quantitative data, statistics, empirical measurements,
numerical evidence, and meta-analyses relevant to the research question.

Step loop
---------
1. **sense()** the claims namespace — check what others are working on.
2. **claim()** this agent's subtask: "quantitative data search for <question>".
3. **[GATE]** ``harness.assert_can_call_external()`` — hard enforcement point.
4. **tavily_search()** targeting datasets, statistics, empirical studies.
5. **inference_router.complete()** to extract and synthesise the numerical
   evidence into a structured finding.
6. **write_finding()** to the findings namespace.
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

_AGENT_ID = "data-analyst"
_PARTICIPANT_TYPE: ParticipantType = "agent"

# Tavily search query template — biased toward statistics and empirical data.
_SEARCH_TEMPLATE = (
    "{question} statistics data empirical study quantitative analysis dataset survey"
)

# LLM synthesis prompt for numerical evidence.
_SYNTHESIS_PROMPT = """\
You are a data analyst extracting quantitative evidence from search results.

Research question: {question}

Search results:
{search_results}

Produce a concise evidence summary (300-500 words) that:
1. Lists specific statistics, percentages, counts, or measurements found.
2. Identifies the sample sizes, populations, and time ranges of the studies.
3. Notes any conflicting statistics between sources and explains the discrepancy
   if possible (e.g. different time periods, methodologies, regions).
4. Explicitly states when data is absent, estimated, or extrapolated.

Write in plain prose. Quote specific numbers where available. Do not fabricate
statistics not present in the search results.
"""


class DataAnalyst(BaseAgent):
    """Domain agent 2: quantitative data and statistics analyst.

    Focuses on finding hard numbers — percentages, effect sizes, counts,
    survey results — to ground the research question in empirical evidence.

    All external IO (search and inference) is gated behind the
    :class:`~agents.harness.StepHarness` per §9.1.
    """

    def __init__(self, run_id: str) -> None:
        super().__init__(
            agent_id=_AGENT_ID,
            run_id=run_id,
            participant_type=_PARTICIPANT_TYPE,
        )

    async def step(self, context: dict) -> dict:
        """Execute one data-analysis step.

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
        # 1. sense() — observe the claims namespace for duplicates
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
        # 2. claim() — register this agent's statistical sub-task
        # ----------------------------------------------------------------
        claim_content = (
            f"[DataAnalyst] Searching for quantitative data and statistics "
            f"for: {question[:200]}"
        )
        await self._make_claim(claim_content)
        harness.mark_claimed()

        # ----------------------------------------------------------------
        # 3. GATE — external calls are now permitted
        # ----------------------------------------------------------------
        harness.assert_can_call_external()

        # ----------------------------------------------------------------
        # 4. tavily_search() — retrieve datasets and empirical studies
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
        # 5. inference_router.complete() — extract and synthesise numbers
        # ----------------------------------------------------------------
        formatted_results = _format_search_results(search_results)
        synthesis_prompt = _SYNTHESIS_PROMPT.format(
            question=question,
            search_results=formatted_results,
        )

        log.info("[%s] Requesting statistical synthesis from inference router.", self.agent_id)
        try:
            synthesis: str = await inference_router.complete(
                prompt=synthesis_prompt,
                max_tokens=700,
            )
        except Exception:
            log.exception("[%s] Inference router failed.", self.agent_id)
            return {"done": False, "error": "inference_router.complete failed"}

        # ----------------------------------------------------------------
        # 6. write_finding() — publish quantitative evidence to the board
        # ----------------------------------------------------------------
        sources: list[str] = _extract_sources(search_results)
        finding = Finding(
            run_id=run_id,
            participant_id=self.agent_id,
            participant_type=self.participant_type,
            title=f"Quantitative Evidence: {question[:100]}",
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
