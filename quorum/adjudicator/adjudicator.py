"""quorum/adjudicator/adjudicator.py

Adjudicator: separate coroutine loop that watches the findings namespace,
detects semantic conflicts (cosine distance < 0.15 = similarity > 0.85),
asks the LLM to describe each conflict, and adds results to the ConflictTray.

NEVER auto-resolves. The Adjudicator only describes conflicts.

Module-level singleton::

    from adjudicator.adjudicator import adjudicator
    asyncio.create_task(adjudicator.run())
"""
import asyncio
import logging
import uuid
from datetime import datetime, timezone

from bus.schemas import Finding, Conflict
from adjudicator.conflict_tray import conflict_tray
from inference.router import inference_router
from bus.namespaces import get_findings_namespace

logger = logging.getLogger(__name__)

SIMILARITY_THRESHOLD = 0.85
# ChromaDB returns cosine *distance* in [0, 2].
# distance < (1 - threshold) => embeddings are very similar.
_DISTANCE_THRESHOLD = 1.0 - SIMILARITY_THRESHOLD  # 0.15


class Adjudicator:
    """Async loop that scans new findings for semantic conflicts.

    Every 2 seconds:
    1. Fetches all findings from ChromaDB.
    2. For each unseen finding, queries the collection for near-duplicates.
    3. For each near-duplicate pair (distance < 0.15), creates a Conflict
       and asks the LLM to describe the tension (verdict).
    4. Adds conflict + verdict to ConflictTray.

    The adjudicator NEVER auto-resolves conflicts — it only describes them.
    Reaper and Adjudicator are separate coroutine loops, not called
    synchronously inside the bus write path (per implementation.md §7).
    """

    def __init__(self) -> None:
        self._seen_finding_ids: set[str] = set()

    async def detect_conflicts(
        self,
        new_finding_content: str,
        new_finding_id: str,
        run_id: str,
    ) -> list[Conflict]:
        """Query the findings collection for entries similar to *new_finding_content*.

        Args:
            new_finding_content: Text content of the newly seen finding.
            new_finding_id: UUID of the new finding (excluded from results).
            run_id: Run identifier for created Conflict objects.

        Returns:
            List of Conflict objects (one per flagged near-duplicate pair).
        """
        collection = get_findings_namespace()
        conflicts: list[Conflict] = []

        try:
            results = collection.query(
                query_texts=[new_finding_content],
                n_results=10,
                include=["distances", "metadatas", "documents"],
            )
        except Exception as exc:
            logger.error("Adjudicator.detect_conflicts: ChromaDB query failed: %s", exc)
            return conflicts

        ids_lists: list[list[str]] = results.get("ids") or [[]]
        distances_lists: list[list[float]] = results.get("distances") or [[]]
        metadatas_lists: list[list[dict]] = results.get("metadatas") or [[]]

        for match_id, distance, meta in zip(
            ids_lists[0], distances_lists[0], metadatas_lists[0]
        ):
            finding_id_in_meta = meta.get("finding_id", match_id)
            if finding_id_in_meta == new_finding_id:
                continue  # skip self-match

            if distance < _DISTANCE_THRESHOLD:
                similarity_score = 1.0 - distance
                conflict = Conflict(
                    id=str(uuid.uuid4()),
                    run_id=run_id,
                    finding_a_id=new_finding_id,
                    finding_b_id=finding_id_in_meta,
                    similarity_score=round(similarity_score, 4),
                    adjudicator_verdict=None,
                    created_at=datetime.now(timezone.utc),
                    resolved=False,
                )
                conflicts.append(conflict)
                logger.info(
                    "Adjudicator: conflict flagged between %s and %s (similarity=%.3f)",
                    new_finding_id,
                    finding_id_in_meta,
                    similarity_score,
                )

        return conflicts

    async def adjudicate_conflict(
        self,
        conflict: Conflict,
        content_a: str,
        content_b: str,
    ) -> str:
        """Ask the LLM to describe the semantic conflict between two findings.

        NEVER auto-resolves. Returns a purely descriptive verdict string.

        Args:
            conflict: The Conflict object (for logging).
            content_a: Text content of finding A.
            content_b: Text content of finding B.

        Returns:
            LLM verdict string describing the conflict.
        """
        prompt = (
            "You are a neutral research adjudicator. Two findings from different "
            "agents are semantically similar but may be contradictory. "
            "Describe the nature of the conflict or tension between them. "
            "Do NOT resolve or pick a winner — only describe what differs.\n\n"
            f"Finding A:\n{content_a}\n\n"
            f"Finding B:\n{content_b}\n\n"
            "Conflict description:"
        )
        messages = [{"role": "user", "content": prompt}]

        try:
            verdict = await inference_router.complete(messages=messages)
            logger.info("Adjudicator: verdict generated for conflict %s", conflict.id)
            return verdict.strip()
        except Exception as exc:
            logger.error(
                "Adjudicator.adjudicate_conflict: inference failed for %s: %s",
                conflict.id,
                exc,
            )
            return f"[Adjudicator inference error: {exc}]"

    async def run(self) -> None:
        """Run the adjudicator loop forever, scanning every 2 seconds.

        This is a separate coroutine — never called synchronously from
        the bus write path. Started via asyncio.create_task() at app startup.
        """
        logger.info("Adjudicator started (SIMILARITY_THRESHOLD=%.2f)", SIMILARITY_THRESHOLD)
        collection = get_findings_namespace()

        while True:
            try:
                try:
                    all_results = collection.get(include=["metadatas", "documents"])
                except Exception as exc:
                    logger.error("Adjudicator.run: failed to fetch findings: %s", exc)
                    await asyncio.sleep(2)
                    continue

                doc_ids: list[str] = all_results.get("ids") or []
                documents: list[str] = all_results.get("documents") or []
                metadatas: list[dict] = all_results.get("metadatas") or []

                for doc_id, document, meta in zip(doc_ids, documents, metadatas):
                    finding_id = meta.get("finding_id", doc_id)
                    run_id = meta.get("run_id", "unknown")

                    if finding_id in self._seen_finding_ids:
                        continue

                    self._seen_finding_ids.add(finding_id)

                    new_conflicts = await self.detect_conflicts(
                        new_finding_content=document,
                        new_finding_id=finding_id,
                        run_id=run_id,
                    )

                    for conflict in new_conflicts:
                        # Retrieve content of finding B for verdict generation
                        try:
                            b_result = collection.get(
                                where={"finding_id": conflict.finding_b_id},
                                include=["documents"],
                            )
                            b_docs: list[str] = b_result.get("documents") or []
                            content_b = b_docs[0] if b_docs else "(content unavailable)"
                        except Exception:
                            content_b = "(content unavailable)"

                        verdict = await self.adjudicate_conflict(
                            conflict=conflict,
                            content_a=document,
                            content_b=content_b,
                        )
                        conflict_with_verdict = conflict.model_copy(
                            update={"adjudicator_verdict": verdict}
                        )
                        await conflict_tray.add(conflict_with_verdict)

            except asyncio.CancelledError:
                logger.info("Adjudicator task cancelled — shutting down.")
                raise
            except Exception as exc:
                logger.error(
                    "Adjudicator.run: unhandled exception (continuing loop): %s",
                    exc,
                    exc_info=True,
                )

            await asyncio.sleep(2)


# Module-level singleton
adjudicator = Adjudicator()
