"""quorum/adjudicator/conflict_tray.py

In-memory conflict tray for the Quorum adjudicator.
Never auto-resolves. All mutations are protected by asyncio.Lock.

Module-level singleton::

    from adjudicator.conflict_tray import conflict_tray
"""
import asyncio
import logging
from typing import Optional

from bus.schemas import Conflict

logger = logging.getLogger(__name__)


class ConflictTray:
    """Thread-safe (asyncio) in-memory store for detected semantic conflicts.

    Conflicts are keyed by their ``id`` field. The Adjudicator adds
    to this tray; only a human (via the API) can mark them resolved.
    The Adjudicator itself NEVER calls mark_resolved().

    Attributes:
        _tray: Dict mapping conflict.id -> Conflict instance.
        _lock: Async mutex protecting all tray mutations.
    """

    def __init__(self) -> None:
        self._tray: dict[str, Conflict] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def add(self, conflict: Conflict) -> None:
        """Add a new conflict to the tray (idempotent by id)."""
        async with self._lock:
            self._tray[conflict.id] = conflict

    async def get_all(self, run_id: Optional[str] = None) -> list[Conflict]:
        """Return all conflicts, optionally filtered by run_id."""
        async with self._lock:
            conflicts = list(self._tray.values())
        if run_id is not None:
            conflicts = [c for c in conflicts if c.run_id == run_id]
        return conflicts

    async def get(self, conflict_id: str) -> Optional[Conflict]:
        """Retrieve a single conflict by id."""
        async with self._lock:
            return self._tray.get(conflict_id)

    async def mark_resolved(self, conflict_id: str) -> None:
        """Set resolved=True on a conflict (human decision only). No-op if not found."""
        async with self._lock:
            conflict = self._tray.get(conflict_id)
            if conflict is None:
                logger.warning(
                    "ConflictTray.mark_resolved: conflict_id=%s not found in tray.",
                    conflict_id,
                )
                return
            self._tray[conflict_id] = conflict.model_copy(update={"resolved": True})


# Module-level singleton
conflict_tray = ConflictTray()
