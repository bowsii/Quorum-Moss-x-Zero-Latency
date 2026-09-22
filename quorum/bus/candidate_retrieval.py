"""
quorum/bus/candidate_retrieval.py
-----------------------------------
Chroma semantic candidate retrieval boundary for Quorum.

Architectural Boundary:
-----------------------
ChromaDB serves strictly as an approximate nearest-neighbor candidate retriever.
It answers: "What existing claims or findings might be semantically related?"

Chroma NEVER decides ownership, grants leases, or authorizes external execution.
Authoritative ownership is determined strictly by the transactional database.
"""

from dataclasses import dataclass
from typing import Any, Optional

from bus.moss_client import sense


@dataclass(frozen=True)
class SemanticCandidate:
    """Represents a semantically related document hit returned from ChromaDB.

    Attributes
    ----------
    id:
        Identifier of the indexed claim or finding.
    namespace:
        'claims' or 'findings'.
    document:
        The natural-language text of the indexed document.
    distance:
        Cosine distance reported by ChromaDB (lower = more similar).
    similarity:
        Normalized similarity score (1.0 - distance).
    metadata:
        Additional non-authoritative metadata indexed alongside the document.
    """
    id: str
    namespace: str
    document: Optional[str]
    distance: Optional[float]
    similarity: Optional[float]
    metadata: dict[str, Any]


async def find_semantic_candidates(
    query: str,
    namespace: str = "claims",
    n_results: int = 10,
) -> list[SemanticCandidate]:
    """Retrieve semantically related candidates from the Chroma vector index.

    This function is purely advisory. The caller MUST verify the status of
    returned candidates against the authoritative transactional database
    before making any coordination decisions.

    Parameters
    ----------
    query:
        Natural language assertion or question.
    namespace:
        'claims' or 'findings'.
    n_results:
        Maximum number of candidates to retrieve.

    Returns
    -------
    list[SemanticCandidate]
        List of candidate objects with distance and similarity scores.
    """
    sense_res = await sense(query=query, namespace=namespace, n_results=n_results)

    candidates: list[SemanticCandidate] = []
    for hit in sense_res.results:
        dist = hit.get("distance")
        sim = (1.0 - dist) if dist is not None else None
        candidates.append(
            SemanticCandidate(
                id=hit["id"],
                namespace=namespace,
                document=hit.get("document"),
                distance=dist,
                similarity=sim,
                metadata=hit.get("metadata", {}),
            )
        )

    return candidates
