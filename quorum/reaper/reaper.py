"""
quorum/reaper/reaper.py
-----------------------
Heartbeat TTL scan loop.

The Reaper is a long-lived asyncio coroutine (``reaper.run()``) that
periodically queries the ChromaDB claims collection for ``active`` claims
whose ``last_heartbeat`` metadata timestamp has expired beyond
``settings.HEARTBEAT_TTL_SECONDS``.

Expired claims are updated in-place: ``status`` → ``"reaped"``,
``reap_reason`` → ``"heartbeat_timeout"``.

All state lives in-process ChromaDB (EphemeralClient) — no HTTP hop.

Module-level singleton::

    from reaper.reaper import reaper

    asyncio.create_task(reaper.run())
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from bus.namespaces import get_claims_namespace
from config.settings import settings

logger = logging.getLogger(__name__)


class Reaper:
    """Async heartbeat-TTL scanner that reaps stale claims.

    The reaper runs as an infinite coroutine (call ``await reaper.run()`` inside
    a task). On each iteration it:

    1. Fetches all ``active`` claims from ChromaDB.
    2. Checks whether ``last_heartbeat`` is older than ``HEARTBEAT_TTL_SECONDS``.
    3. Updates expired claims to ``status="reaped"`` with
       ``reap_reason="heartbeat_timeout"``.
    """

    def __init__(self, ttl: int | None = None, scan_interval: int | None = None) -> None:
        self.ttl = ttl if ttl is not None else settings.HEARTBEAT_TTL_SECONDS
        self.scan_interval = scan_interval if scan_interval is not None else settings.REAPER_SCAN_INTERVAL_SECONDS

    @property
    def TTL(self) -> int:
        """Backward-compatible uppercase alias for self.ttl."""
        return self.ttl

    # ------------------------------------------------------------------
    # Core scan
    # ------------------------------------------------------------------

    async def scan_once(self) -> int:
        """Query ChromaDB for expired active claims and reap each one.

        Returns:
            The number of claims reaped during this scan.
        """
        collection = get_claims_namespace()
        now = datetime.now(tz=timezone.utc)
        deadline: datetime = now - timedelta(seconds=self.ttl)

        # Fetch all active claims.
        try:
            result = collection.get(
                where={"status": "active"},
                include=["metadatas"],
            )
        except Exception as exc:
            logger.error("Reaper.scan_once: failed to query claims collection: %s", exc)
            return 0

        ids: list[str] = result.get("ids") or []
        metadatas: list[dict] = result.get("metadatas") or []

        reaped = 0
        for doc_id, meta in zip(ids, metadatas):
            claim_id: str = meta.get("claim_id", doc_id)
            last_hb_raw: str | None = meta.get("last_heartbeat")

            if last_hb_raw is None:
                logger.warning(
                    "Reaper: claim %s has no last_heartbeat; reaping.", claim_id
                )
                await self.reap_claim(claim_id=claim_id, doc_id=doc_id)
                reaped += 1
                continue

            try:
                last_hb = datetime.fromisoformat(last_hb_raw)
                if last_hb.tzinfo is None:
                    last_hb = last_hb.replace(tzinfo=timezone.utc)
            except ValueError:
                logger.warning(
                    "Reaper: claim %s has unparseable last_heartbeat=%r; reaping.",
                    claim_id,
                    last_hb_raw,
                )
                await self.reap_claim(claim_id=claim_id, doc_id=doc_id)
                reaped += 1
                continue

            if last_hb < deadline:
                await self.reap_claim(claim_id=claim_id, doc_id=doc_id)
                reaped += 1

        return reaped

    async def reap_claim(self, claim_id: str, doc_id: str) -> None:
        """Mark a single claim as reaped in ChromaDB.

        Uses update_claim_status from bus.moss_client to ensure thread-safe
        mutation under _write_lock without clobbering other metadata.

        Args:
            claim_id: Logical claim identifier (stored in metadata).
            doc_id: ChromaDB document ID.
        """
        from bus.moss_client import update_claim_status
        try:
            await update_claim_status(
                claim_id=doc_id,
                status="reaped",
                reap_reason="heartbeat_timeout",
            )
            logger.info("Reaped claim %s reason=heartbeat_timeout", claim_id)
        except Exception as exc:
            logger.error(
                "Reaper.reap_claim: failed to update claim %s (doc_id=%s): %s",
                claim_id,
                doc_id,
                exc,
            )

    # ------------------------------------------------------------------
    # Infinite run loop
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """Run the reaper loop forever, scanning every ``scan_interval`` seconds."""
        logger.info(
            "Reaper started: TTL=%ds scan_interval=%ds",
            self.ttl,
            self.scan_interval,
        )
        while True:
            try:
                reaped = await self.scan_once()
                if reaped:
                    logger.debug("Reaper scan: reaped %d claim(s) this cycle.", reaped)
            except asyncio.CancelledError:
                logger.info("Reaper task cancelled — shutting down.")
                raise
            except Exception as exc:
                logger.error(
                    "Reaper.run: unhandled exception in scan loop (continuing): %s",
                    exc,
                    exc_info=True,
                )

            await asyncio.sleep(self.scan_interval)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

reaper = Reaper()

