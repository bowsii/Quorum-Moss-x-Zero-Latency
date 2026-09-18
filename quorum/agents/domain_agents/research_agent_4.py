"""
quorum/agents/domain_agents/research_agent_4.py
------------------------------------------------
Research Agent 4: Synthesis & Consensus Builder.

Reads ALL findings from the board, identifies agreements and conflicts between
them, and synthesises a coherent consensus view (or a structured disagreeement
map if no consensus is possible).

This agent has the slowest step cycle: it deliberately starts last and builds
entirely on the work of Agents 1-3.  It does not generate new search results —
its sole job is to integrate, reconcile, and summarise the collective output.

Step loop
---------
1. **sense()** the **findings** namespace — retrieve all available findings.
2. **claim()** this agent's subtask: "synthesising consensus for <question>".
3. **[GATE]** ``harness.assert_can_call_external()`` — hard enforcement point.
4. No external search — this agent works exclusively with board findings.
5. **inference_router.complete()** to produce the consensus / disagreement map.
6. **write_finding()** to the findings namespace with ``title`` prefixed
   ``"[SYNTHESIS]"`` so downstream readers (Adjudicator, humans) can find it.

Agent 4 waits ``_STARTUP_YIELD_SECONDS`` on step 0 to maximise the chance
that Agents 1-3 have already posted their findings.  It retries (``done=False``)
if fewer than two findings are available (not enough to synthesise from).
"""

import asyncio
import logging
from typing import Optional

from agents.base_agent import BaseAgent
from agents.harness import StepHarness
from bus.moss_client import sense, write_finding
from bus.schemas import Finding, ParticipantType
from inference.router import inference_router
from config.settings import settings

log = logging.getLogger(__name__)

_AGENT_ID = "synthesis-builder"
_PARTICIPANT_TYPE: ParticipantType = "agent"

# Startup yield — let other agents post findings first.
_STARTUP_YIELD_SECONDS: float = 4.0

# Minimum number of findings required before synthesis is attempted.
_MIN_FINDINGS_REQUIRED: int = 2

# LLM synthesis prompt.
_SYNTHESIS_PROMPT = """\
You are a synthesis researcher whose job is to integrate multiple research
perspectives into a coherent, balanced view.

Research question: {question}

Available findings from other researchers:
{all_findings}

Produce a structured synthesis (400-600 words) that:
1. **Areas of Agreement**: Identify claims that multiple findings support.
   Be specific about what is agreed upon.
2. **Areas of Conflict**: Identify where findings contradict each other.
   For each conflict, characterise the nature of the disagreement
   (factual, methodological, interpretive, or temporal).
3. **Consensus View**: Offer the best-supported single answer to the
   research question, given the weight of evidence. If no consensus is
   possible, explicitly say so and describe the disagreement landscape.
4. **Confidence Level**: Rate your confidence (high / medium / low) and
   explain what additional evidence would raise it.
5. **Open Questions**: List the 2-3 most important unresolved questions.

Write in plain prose with clear section headers. Attribute specific claims
to the finding they originate from (e.g. "According to the literature review...").
"""


class SynthesisBuilder(BaseAgent):
    """Domain agent 4: synthesis and consensus builder.

    Reads all findings from the board, reconciles conflicts, and writes a
    final consensus finding.  Operates exclusively through the board — it does
    NOT call Tavily.  All inference calls are gated behind the
    :class:`~agents.harness.StepHarness` per §9.1.
    """

    def __init__(self, run_id: str) -> None:
        super().__init__(
            agent_id=_AGENT_ID,
            run_id=run_id,
            participant_type=_PARTICIPANT_TYPE,
        )

    async def step(self, context: dict) -> dict:
        """Execute one synthesis step.

        Waits on step 0 so that other agents have time to post findings.
        Retries (``done=False``) if fewer than :data:`_MIN_FINDINGS_REQUIRED`
        findings are available.

        Parameters
        ----------
        context:
            Must contain ``'run_id'`` (str) and ``'question'`` (str).

        Returns
        -------
        dict
            ``{'done': True, 'finding_id': str}`` on success, or
            ``{'done': False, 'reason': str}`` when more findings are needed.
        """
        run_id: str = context["run_id"]
        question: str = context["question"]
        step_number: int = context.get("step_number", 0)

        # Yield on step 0 — let Agents 1-3 work first.
        if step_number == 0:
            log.info(
                "[%s] Waiting %.1fs for other agents to post findings.",
                self.agent_id,
                _STARTUP_YIELD_SECONDS,
            )
            await asyncio.sleep(_STARTUP_YIELD_SECONDS)

        harness = StepHarness(self.agent_id, run_id)

        # ----------------------------------------------------------------
        # 1. sense() — read the FINDINGS namespace
        # ----------------------------------------------------------------
        log.info(
            "[%s] Step %d: sensing findings namespace.",
            self.agent_id,
            step_number,
        )
        sense_result = await sense(
            query=question,
            namespace="findings",
            n_results=20,  # retrieve broadly — we want everything
        )
        harness.mark_sensed()

        all_findings: list[dict] = sense_result.results or []
        log.info(
            "[%s] Sensed %d findings on the board.",
            self.agent_id,
            len(all_findings),
        )

        # Require a minimum number of findings before attempting synthesis.
        if len(all_findings) < _MIN_FINDINGS_REQUIRED:
            harness.mark_done()
            harness.reset()
            log.info(
                "[%s] Only %d finding(s) available — need at least %d. "
                "Deferring to next step.",
                self.agent_id,
                len(all_findings),
                _MIN_FINDINGS_REQUIRED,
            )
            return {
                "done": False,
                "reason": "insufficient_findings",
                "available": len(all_findings),
                "required": _MIN_FINDINGS_REQUIRED,
            }

        # ----------------------------------------------------------------
        # 2. claim() — register this agent's synthesis sub-task
        # ----------------------------------------------------------------
        claim_content = (
            f"[SynthesisBuilder] Building consensus from {len(all_findings)} "
            f"findings for: {question[:200]}"
        )
        await self._make_claim(claim_content)
        harness.mark_claimed()

        # ----------------------------------------------------------------
        # 3. GATE — external calls (inference) are now permitted
        # ----------------------------------------------------------------
        harness.assert_can_call_external()

        # ----------------------------------------------------------------
        # 4-5. inference_router.complete() — synthesise the consensus view
        #       (No Tavily search — works exclusively from board findings.)
        # ----------------------------------------------------------------
        formatted_findings = _format_findings(all_findings)
        synthesis_prompt = _SYNTHESIS_PROMPT.format(
            question=question,
            all_findings=formatted_findings,
        )

        log.info(
            "[%s] Requesting consensus synthesis from inference router "
            "(input: %d findings).",
            self.agent_id,
            len(all_findings),
        )
        try:
            synthesis: str = await inference_router.complete(
                prompt=synthesis_prompt,
                max_tokens=900,
            )
        except Exception:
            log.exception("[%s] Inference router failed.", self.agent_id)
            return {"done": False, "error": "inference_router.complete failed"}

        # ----------------------------------------------------------------
        # 6. write_finding() — publish the consensus finding
        # ----------------------------------------------------------------
        # Collect all source URLs already cited by prior findings.
        all_sources: list[str] = _collect_sources(all_findings)

        finding = Finding(
            run_id=run_id,
            participant_id=self.agent_id,
            participant_type=self.participant_type,
            title=f"[SYNTHESIS] Consensus on: {question[:80]}",
            content=synthesis,
            sources=all_sources,
        )

        log.info("[%s] Writing consensus finding %s.", self.agent_id, finding.id)
        try:
            await write_finding(finding)
        except Exception:
            log.exception("[%s] write_finding failed.", self.agent_id)
            return {"done": False, "error": "write_finding failed"}

        harness.mark_done()
        log.info(
            "[%s] Step complete — consensus finding published: %s",
            self.agent_id,
            finding.id,
        )
        return {
            "done": True,
            "finding_id": finding.id,
            "synthesised_from": len(all_findings),
        }


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _format_findings(findings: list[dict]) -> str:
    """Format sensed findings into labelled blocks for the LLM prompt."""
    if not findings:
        return "(no findings on board)"

    lines: list[str] = []
    for i, f in enumerate(findings, start=1):
        # ChromaDB returns raw document strings in results.
        content = f if isinstance(f, str) else f.get("content", str(f))
        lines.append(f"--- Finding {i} ---\n{content[:800]}")

    return "\n\n".join(lines)


def _collect_sources(findings: list[dict]) -> list[str]:
    """Collect unique source URLs referenced in the sensed findings metadata."""
    seen: set[str] = set()
    urls: list[str] = []
    for f in findings:
        if isinstance(f, dict):
            for url in f.get("sources", []):
                if url and url not in seen:
                    seen.add(url)
                    urls.append(url)
    return urls
