"""
Supersedes-chain resolution for Quorum claims (§9.3).

Provides utilities to follow the linked list of superseded claims and
return only the terminal head (the most recent version), with cycle-protection via a depth limit.

Usage:
    head = await resolve_chain_head(claim_id)
    active = await get_active_head(claim_id)
"""
from typing import Optional
from bus.namespaces import get_claims_namespace
from bus.moss_client import get_claim, _run_sync


async def resolve_chain_head(claim_id: str, max_depth: int = 50) -> Optional[dict]:
    """
    Follow the supersedes chain forward and return the head (latest terminal) claim.

    If claim A supersedes claim B, and claim B supersedes claim C, then
    calling resolve_chain_head('claim-c'), resolve_chain_head('claim-b'),
    or resolve_chain_head('claim-a') all resolve to 'claim-a' (the head).

    Args:
        claim_id:  ID of any claim in the chain.
        max_depth: Maximum hops before raising ValueError.

    Returns:
        The head claim dict (keys: 'id', 'document', 'metadata'), or None
        if the initial claim_id does not exist.
    """
    current_claim = await get_claim(claim_id)
    if current_claim is None:
        return None

    collection = get_claims_namespace()
    depth = 0
    current_id = claim_id

    while depth < max_depth:
        # Check if any other claim supersedes current_id
        def _find_successor(target_id: str):
            return collection.get(
                where={"supersedes": target_id},
                include=["documents", "metadatas"],
            )

        raw = await _run_sync(_find_successor, current_id)
        successor_ids = raw.get("ids", [])

        if not successor_ids:
            # No newer claim supersedes current_id — current_id is the head!
            return await get_claim(current_id)

        # Move to the successor (newer claim)
        current_id = successor_ids[0]
        depth += 1

    raise ValueError(
        f"Supersedes chain starting from '{claim_id}' exceeded max depth "
        f"of {max_depth}. Possible cycle or corrupted chain."
    )


async def get_active_head(claim_id: str) -> Optional[dict]:
    """
    Return the head claim only if its status is 'active'.
    """
    head = await resolve_chain_head(claim_id)
    if head is None:
        return None

    metadata: dict = head.get("metadata", {})
    if metadata.get("status") == "active":
        return head

    return None
