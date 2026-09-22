"""
quorum/search/tavily_client.py
------------------------------
Tavily external search client.

Provides a thin async wrapper around the official ``tavily-python`` SDK.
All errors are caught and logged; callers always receive a list (possibly
empty) rather than an exception propagating upward.
"""

import asyncio
import logging
from typing import Optional

from tavily import AsyncTavilyClient

from agents.coordination_token import (
    CoordinationToken,
    UnapprovedExternalWorkError,
    verify_coordination_capability,
)

from config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_tavily_client: Optional[AsyncTavilyClient] = None


def get_tavily_client() -> AsyncTavilyClient:
    """Return (and lazily create) the module-level Tavily client singleton.

    The client is instantiated once per process lifetime using the API key
    from :data:`config.settings.settings`.  Subsequent calls return the same
    instance, avoiding repeated credential resolution overhead.

    Returns
    -------
    AsyncTavilyClient
        The shared Tavily async client instance.

    Raises
    ------
    RuntimeError
        If ``TAVILY_API_KEY`` is empty / not set in the environment.
    """
    global _tavily_client
    if _tavily_client is None:
        api_key = settings.TAVILY_API_KEY
        if not api_key:
            raise RuntimeError(
                "TAVILY_API_KEY is not set. "
                "Add it to your .env or export it as an environment variable."
            )
        _tavily_client = AsyncTavilyClient(api_key=api_key)
        logger.debug("Tavily async client initialised.")
    return _tavily_client


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def tavily_search(
    query: str,
    max_results: int = 5,
    search_depth: str = "basic",
    coordination_token: Optional[CoordinationToken] = None,
) -> list[dict]:
    """Search the web via Tavily and return structured results.

    Each result dict contains at minimum:
    ``{"url": str, "title": str, "content": str, "score": float}``

    Firewall:
        Requires a valid CoordinationToken in the execution context or via
        coordination_token argument. Raises UnapprovedExternalWorkError if unauthorized.

    Parameters
    ----------
    query:
        The natural-language search query.
    max_results:
        Maximum number of results to return (default 5).
    search_depth:
        "basic" or "advanced".
    coordination_token:
        Optional explicit CoordinationToken. If None, resolved from ambient context.

    Returns
    -------
    list[dict]
        A list of result dicts, each with ``url``, ``title``, ``content``,
        and ``score`` keys.  Returns ``[]`` on any transient failure.
    """
    # ------------------------------------------------------------------
    # Coordination Firewall Gate Check (HARD REJECT IF UNAUTHORIZED)
    # ------------------------------------------------------------------
    verify_coordination_capability(token=coordination_token)

    if not query or not query.strip():
        logger.warning("tavily_search called with empty query; returning [].")
        return []

    try:
        client = get_tavily_client()
    except RuntimeError as exc:
        logger.error("Tavily client unavailable: %s", exc)
        return []

    try:
        logger.debug("Tavily search: query=%r max_results=%d", query, max_results)
        response = await client.search(
            query=query,
            max_results=max_results,
            include_answer=False,
            include_raw_content=False,
        )

        raw_results: list[dict] = response.get("results", [])

        # Normalise each result to the documented shape, dropping unknown keys.
        results: list[dict] = []
        for item in raw_results:
            results.append(
                {
                    "url": item.get("url", ""),
                    "title": item.get("title", ""),
                    "content": item.get("content", ""),
                    "score": float(item.get("score", 0.0)),
                }
            )

        logger.info(
            "Tavily search complete: query=%r returned %d results",
            query,
            len(results),
        )
        return results

    except asyncio.CancelledError:
        # Let task cancellation propagate correctly.
        raise
    except Exception as exc:  # noqa: BLE001
        logger.error(
            "Tavily search failed for query=%r: %s",
            query,
            exc,
            exc_info=True,
        )
        return []
