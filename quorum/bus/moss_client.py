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
from datetime import datetime, timezone
from typing import Optional

from bus.namespaces import get_claims_namespace, get_findings_namespace
from bus.schemas import Claim, Finding, SenseResult
from bus.claim_engine import ClaimDecision, ClaimDecisionEngine, ClaimResolutionOutcome

# Concurrency model:
# Separate locks per namespace eliminate cross-namespace contention:
# - `_claims_lock`: serializes claims mutations (write_claim, update_claim_status, heartbeat)
# - `_findings_lock`: serializes findings mutations (write_finding)
#
# Collision window & TOCTOU resolution:
# In Quorum's high-throughput swarm mode, `sense()` and `write_claim()` run without
# holding a long-lived mutex across the reasoning window. When two agents claim
# concurrently within the collision window (~1-7ms), both writes are accepted and
# downstream readers determine precedence deterministically via `bus/tiebreak.py`
# (50ms human priority window, monotonic timestamps, lexicographical participant IDs).
# For workflows requiring strict pre-write mutual exclusion, `claim(..., atomic=True)`
# provides an atomic lock acquisition across both dedup sensing and writing.
_claims_lock: Optional[asyncio.Lock] = None
_claims_lock_loop: Optional[asyncio.AbstractEventLoop] = None

_findings_lock: Optional[asyncio.Lock] = None
_findings_lock_loop: Optional[asyncio.AbstractEventLoop] = None


def get_claims_lock() -> asyncio.Lock:
    """Retrieve or initialize the claims lock bound to the currently running event loop."""
    global _claims_lock, _claims_lock_loop
    loop = asyncio.get_running_loop()
    if _claims_lock is None or _claims_lock_loop is not loop:
        _claims_lock = asyncio.Lock()
        _claims_lock_loop = loop
    return _claims_lock


def get_findings_lock() -> asyncio.Lock:
    """Retrieve or initialize the findings lock bound to the currently running event loop."""
    global _findings_lock, _findings_lock_loop
    loop = asyncio.get_running_loop()
    if _findings_lock is None or _findings_lock_loop is not loop:
        _findings_lock = asyncio.Lock()
        _findings_lock_loop = loop
    return _findings_lock




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
    loop = asyncio.get_running_loop()
    return loop.run_in_executor(None, fn, *args)


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


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
    Write a Claim to the claims namespace on the Moss board under write lock.

    Metadata stored alongside the document:
        run_id, participant_id, participant_type, status, ts_monotonic,
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
        "ts_monotonic": claim.ts_monotonic,
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

    async with get_claims_lock():
        await _run_sync(_add)



async def write_finding(finding: Finding) -> None:
    """
    Write a Finding to the findings namespace on the Moss board under write lock.

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

    async with get_findings_lock():
        await _run_sync(_add)


async def update_claim_status(
    claim_id: str,
    status: str,
    reap_reason: Optional[str] = None,
) -> None:
    """
    Update a claim's status (and optionally reap_reason) atomically under lock.

    Safely preserves all other metadata fields to prevent clobbering concurrent updates.

    Args:
        claim_id:    ID of the claim to update.
        status:      New ClaimStatus value.
        reap_reason: Optional ReapReason if the claim is being reaped.
    """
    collection = get_claims_namespace()

    def _update():
        existing = collection.get(ids=[claim_id], include=["metadatas"])
        if not existing["ids"]:
            return
        meta = existing["metadatas"][0].copy()
        meta["status"] = status
        if reap_reason is not None:
            meta["reap_reason"] = reap_reason
        collection.update(
            ids=[claim_id],
            metadatas=[meta],
        )

    async with get_claims_lock():
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
    Refresh the last_heartbeat timestamp for a claim under write lock.

    Called periodically by active agents to signal liveness to the reaper.

    Args:
        claim_id: UUID string of the claim to refresh.
    """
    collection = get_claims_namespace()
    now_iso = _now_iso()

    def _update():
        existing = collection.get(ids=[claim_id], include=["metadatas"])
        if not existing["ids"]:
            return
        meta = existing["metadatas"][0].copy()
        meta["last_heartbeat"] = now_iso
        collection.update(
            ids=[claim_id],
            metadatas=[meta],
        )

    async with get_claims_lock():
        await _run_sync(_update)


async def resolve_and_claim(
    challenger: Claim,
    dedup_threshold: float = 0.85,
    ttl_seconds: Optional[float] = None,
) -> ClaimDecision:
    """
    Authoritative, atomic claim resolution and registration under write lock.

    Guarantees that competing claims cannot both acquire ownership.
    Evaluates completed findings, active claims, race-window tiebreaks, and
    stale leases, then writes the winner claim to the board.

    Concurrency and In-Process Atomicity:
    In single-process execution, `_claims_lock` serializes the entire sense-evaluate-write
    decision lifecycle. Competing concurrent coroutines cannot interleave between the
    sensing check and the claim registration.

    Limitations:
    This atomicity guarantee holds within the asyncio event loop of a single process.
    If multiple process instances or distributed workers are deployed in future stages,
    authoritative transactional state in a database (e.g. PostgreSQL with row locks)
    or Redis distributed locks will be required.

    Args:
        challenger: The Claim proposed by the participant.
        dedup_threshold: Similarity threshold (default 0.85).
        ttl_seconds: Lease TTL (default settings.HEARTBEAT_TTL_SECONDS).

    Returns:
        ClaimDecision: Outcome, winner ID, and absorption metadata.
    """
    engine = ClaimDecisionEngine(dedup_threshold=dedup_threshold, ttl_seconds=ttl_seconds)

    async with get_claims_lock():
        findings_res = await sense(query=challenger.content, namespace="findings", n_results=5)
        claims_res = await sense(query=challenger.content, namespace="claims", n_results=10)

        decision = engine.evaluate(
            challenger=challenger,
            sensed_claims=claims_res.results,
            sensed_findings=findings_res.results,
        )

        if decision.external_work_allowed:
            collection = get_claims_namespace()
            metadata = {
                "run_id": challenger.run_id,
                "participant_id": challenger.participant_id,
                "participant_type": challenger.participant_type,
                "status": "active",
                "ts_monotonic": challenger.ts_monotonic,
                "created_at": challenger.created_at.isoformat(),
                "last_heartbeat": challenger.last_heartbeat.isoformat(),
                "supersedes": challenger.supersedes or (decision.superseded_claim_id or ""),
                "reap_reason": challenger.reap_reason or "",
            }

            def _add_winner():
                collection.add(
                    ids=[challenger.id],
                    documents=[challenger.content],
                    metadatas=[metadata],
                )

            await _run_sync(_add_winner)

            if decision.superseded_claim_id:
                def _supersede_incumbent():
                    existing = collection.get(ids=[decision.superseded_claim_id], include=["metadatas"])
                    if existing["ids"]:
                        meta = existing["metadatas"][0].copy()
                        meta["status"] = "superseded"
                        collection.update(ids=[decision.superseded_claim_id], metadatas=[meta])

                await _run_sync(_supersede_incumbent)

        return decision


async def claim(
    run_id: str,
    participant_id: str,
    participant_type: str,
    content: str,
    supersedes: Optional[str] = None,
    atomic: bool = True,
    dedup_threshold: float = 0.85,
) -> tuple[Claim, bool]:
    """
    High-level claim helper with explicit concurrency control.

    Args:
        run_id: Run UUID string.
        participant_id: Agent or human participant ID.
        participant_type: One of 'agent', 'human', 'adjudicator', 'reaper'.
        content: Assertion text.
        supersedes: Optional superseded claim ID.
        atomic: If True (default), holds `_claims_lock` across the entire
            decision lifecycle via resolve_and_claim, guaranteeing mutual exclusion.
        dedup_threshold: Cosine similarity threshold above which a claim is considered duplicate.

    Returns:
        tuple[Claim, bool]: (claim, is_duplicate)
    """
    new_claim = Claim(
        run_id=run_id,
        participant_id=participant_id,
        participant_type=participant_type,  # type: ignore[arg-type]
        content=content,
        supersedes=supersedes,
    )

    if atomic:
        decision = await resolve_and_claim(new_claim, dedup_threshold=dedup_threshold)
        if decision.is_duplicate:
            existing_id = decision.winner_claim_id or decision.existing_claim_id or ""
            existing = await get_claim(existing_id) if existing_id else None
            if existing and existing.get("id"):
                meta = existing.get("metadata", {})
                winner = Claim(
                    id=existing["id"],
                    run_id=meta.get("run_id", run_id),
                    participant_id=meta.get("participant_id", ""),
                    participant_type=meta.get("participant_type", "agent"),
                    content=existing.get("document") or content,
                    status=meta.get("status", "active"),
                )
                return winner, True
            return new_claim, True
        return new_claim, False
    else:
        await write_claim(new_claim)
        return new_claim, False


