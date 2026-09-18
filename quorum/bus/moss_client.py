"""
Async thin wrapper around chromadb (Moss) for the Quorum semantic bus.

All chromadb calls are synchronous; they are offloaded to the default
ThreadPoolExecutor via asyncio.get_event_loop().run_in_executor so they
never block the event loop.

Public API:
    sense()              — semantic query against claims or findings
    write_claim()        — add a Claim to the claims namespace
    write_finding()      — add a Finding to the findings namespace
    update_claim_status() — mutate a claim's status/reap_reason in-place
    get_claim()          — retrieve a single claim by ID
    heartbeat()          — refresh a claim's last_heartbeat timestamp
"""

import asyncio
import time
from datetime import datetime
from typing import Optional

from bus.namespaces import get_claims_namespace, get_findings_namespace
from bus.schemas import Claim, Finding, SenseResult


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _run_sync(fn, *args):
    """
    Execute a synchronous callable in the default executor.

    Args:
        fn:   Callable to execute.
        *args: Positional arguments forwarded to fn.

    Returns:
        Awaitable that resolves to fn(*args).
    """
    loop = asyncio.get_event_loop()
    return loop.run_in_executor(None, fn, *args)


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.utcnow().isoformat()


# ---------------------------------------------------------------------------
# Public async API
# ---------------------------------------------------------------------------

async def sense(
    query: str,
    namespace: str = "claims",
    n_results: int = 10,
) -> SenseResult:
    """
    Query the semantic board and return matching documents with latency.

    Args:
        query:     Natural-language query string.
        namespace: One of 'claims' or 'findings'.
        n_results: Maximum number of results to return.

    Returns:
        SenseResult containing raw chromadb hits and latency_ms.

    Raises:
        ValueError: If namespace is not 'claims' or 'findings'.
    """
    if namespace == "claims":
        collection = get_claims_namespace()
    elif namespace == "findings":
        collection = get_findings_namespace()
    else:
        raise ValueError(f"Unknown namespace: {namespace!r}. Must be 'claims' or 'findings'.")

    start = time.perf_counter()

    def _query():
        return collection.query(
            query_texts=[query],
            n_results=n_results,
            include=["documents", "metadatas", "distances"],
        )

    raw = await _run_sync(_query)
    latency_ms = (time.perf_counter() - start) * 1000.0

    # Normalise into a list[dict] — one entry per result
    results: list[dict] = []
    ids = raw.get("ids", [[]])[0]
    documents = raw.get("documents", [[]])[0]
    metadatas = raw.get("metadatas", [[]])[0]
    distances = raw.get("distances", [[]])[0]

    for idx, doc_id in enumerate(ids):
        results.append(
            {
                "id": doc_id,
                "document": documents[idx] if idx < len(documents) else None,
                "metadata": metadatas[idx] if idx < len(metadatas) else {},
                "distance": distances[idx] if idx < len(distances) else None,
            }
        )

    return SenseResult(query=query, results=results, latency_ms=round(latency_ms, 3))


async def write_claim(claim: Claim) -> None:
    """
    Write a Claim to the claims namespace on the Moss board.

    Metadata stored alongside the document:
        run_id, participant_id, participant_type, status,
        created_at (ISO), last_heartbeat (ISO), supersedes (or ''),
        reap_reason (or '').

    Args:
        claim: The Claim object to persist.
    """
    collection = get_claims_namespace()

    metadata = {
        "run_id": claim.run_id,
        "participant_id": claim.participant_id,
        "participant_type": claim.participant_type,
        "status": claim.status,
        "created_at": claim.created_at.isoformat(),
        "last_heartbeat": claim.last_heartbeat.isoformat(),
        "supersedes": claim.supersedes or "",
        "reap_reason": claim.reap_reason or "",
    }

    def _add():
        collection.add(
            ids=[claim.id],
            documents=[claim.content],
            metadatas=[metadata],
        )

    await _run_sync(_add)


async def write_finding(finding: Finding) -> None:
    """
    Write a Finding to the findings namespace on the Moss board.

    Metadata stored alongside the document:
        run_id, participant_id, participant_type, title,
        created_at (ISO), supersedes (or ''), sources (pipe-joined str).

    Args:
        finding: The Finding object to persist.
    """
    collection = get_findings_namespace()

    metadata = {
        "run_id": finding.run_id,
        "participant_id": finding.participant_id,
        "participant_type": finding.participant_type,
        "title": finding.title,
        "created_at": finding.created_at.isoformat(),
        "supersedes": finding.supersedes or "",
        "sources": "|".join(finding.sources),
    }

    def _add():
        collection.add(
            ids=[finding.id],
            documents=[finding.content],
            metadatas=[metadata],
        )

    await _run_sync(_add)


async def update_claim_status(
    claim_id: str,
    status: str,
    reap_reason: Optional[str] = None,
) -> None:
    """
    Update a claim's status (and optionally reap_reason) in the board.

    Uses chromadb's update() to mutate only the metadata fields that
    changed, leaving the document vector intact.

    Args:
        claim_id:    ID of the claim to update.
        status:      New ClaimStatus value.
        reap_reason: Optional ReapReason if the claim is being reaped.
    """
    collection = get_claims_namespace()

    updated_meta: dict = {"status": status}
    if reap_reason is not None:
        updated_meta["reap_reason"] = reap_reason

    def _update():
        collection.update(
            ids=[claim_id],
            metadatas=[updated_meta],
        )

    await _run_sync(_update)


async def get_claim(claim_id: str) -> Optional[dict]:
    """
    Retrieve a specific claim by ID from the claims namespace.

    Args:
        claim_id: UUID string of the claim.

    Returns:
        A dict with keys 'id', 'document', 'metadata', or None if not found.
    """
    collection = get_claims_namespace()

    def _get():
        return collection.get(
            ids=[claim_id],
            include=["documents", "metadatas"],
        )

    raw = await _run_sync(_get)

    ids = raw.get("ids", [])
    if not ids:
        return None

    return {
        "id": ids[0],
        "document": raw["documents"][0] if raw.get("documents") else None,
        "metadata": raw["metadatas"][0] if raw.get("metadatas") else {},
    }


async def heartbeat(claim_id: str) -> None:
    """
    Refresh the last_heartbeat timestamp for a claim.

    Called periodically by active agents to signal liveness to the reaper.

    Args:
        claim_id: UUID string of the claim to refresh.
    """
    collection = get_claims_namespace()
    now_iso = _now_iso()

    def _update():
        collection.update(
            ids=[claim_id],
            metadatas=[{"last_heartbeat": now_iso}],
        )

    await _run_sync(_update)
