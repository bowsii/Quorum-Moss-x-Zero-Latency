"""
quorum/transport/livekit_room.py
---------------------------------
LiveKit room management for the Quorum multi-agent system.

Board state is distributed over LiveKit data channels rather than SSE,
enabling low-latency real-time updates to all connected participants
(agents, human observers, and the frontend).

§13 server-side token scoping:
  - Agent tokens carry ``room_record=True`` for write access.
  - Human / observer tokens carry ``room_record=False`` (read-only).
"""

import asyncio
import json
import logging
from typing import Optional

from livekit import api
from livekit.api import AccessToken, VideoGrants

from config.settings import settings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Room manager
# ---------------------------------------------------------------------------


class LiveKitRoomManager:
    """Manages LiveKit rooms and participant tokens for Quorum runs.

    Each Quorum run gets its own LiveKit room named ``quorum-{run_id}``.
    Board state updates are broadcast to all room participants via
    the LiveKit data channel so the frontend can re-render without polling.
    """

    def __init__(self) -> None:
        self._lk_api: Optional[api.LiveKitAPI] = None

    def _get_api(self) -> api.LiveKitAPI:
        """Lazily construct and cache the LiveKit API client."""
        if self._lk_api is None:
            if not settings.LIVEKIT_URL:
                raise RuntimeError(
                    "LIVEKIT_URL is not configured. "
                    "Set it in .env or as an environment variable."
                )
            if not settings.LIVEKIT_API_KEY or not settings.LIVEKIT_API_SECRET:
                raise RuntimeError(
                    "LIVEKIT_API_KEY and LIVEKIT_API_SECRET must both be set."
                )
            self._lk_api = api.LiveKitAPI(
                url=settings.LIVEKIT_URL,
                api_key=settings.LIVEKIT_API_KEY,
                api_secret=settings.LIVEKIT_API_SECRET,
            )
            logger.debug("LiveKit API client initialised (url=%s).", settings.LIVEKIT_URL)
        return self._lk_api

    # ------------------------------------------------------------------
    # Room lifecycle
    # ------------------------------------------------------------------

    async def create_room(self, run_id: str) -> str:
        """Create a LiveKit room for the given Quorum run.

        The room is named ``quorum-{run_id}`` so it is uniquely scoped to the
        run and easy to identify in the LiveKit dashboard.

        Parameters
        ----------
        run_id:
            The unique identifier of the Quorum run.

        Returns
        -------
        str
            The canonical room name (``quorum-{run_id}``).
        """
        room_name = f"quorum-{run_id}"
        lk = self._get_api()

        room_options = api.CreateRoomRequest(name=room_name)
        try:
            room = await lk.room.create_room(room_options)
            logger.info("LiveKit room created: %s (sid=%s)", room.name, room.sid)
        except Exception as exc:  # noqa: BLE001
            # Room may already exist if this run_id was restarted; log and continue.
            logger.warning(
                "create_room(%s) raised %s — room may already exist, proceeding.",
                room_name,
                exc,
            )

        return room_name

    # ------------------------------------------------------------------
    # Token minting
    # ------------------------------------------------------------------

    def mint_token(
        self,
        room_name: str,
        participant_identity: str,
        participant_name: str = "",
        is_agent: bool = True,
    ) -> str:
        """Mint a signed JWT for a participant to join a LiveKit room.

        Applies §13 server-side scoping:
        - **Agents** receive ``room_record=True`` (full write access).
        - **Humans / observers** receive ``room_record=False`` (read-only).

        Parameters
        ----------
        room_name:
            The LiveKit room the token grants access to.
        participant_identity:
            Unique identity string for the participant (e.g. agent ID or user ID).
        participant_name:
            Display name shown in the LiveKit dashboard (optional).
        is_agent:
            ``True`` for agent participants, ``False`` for human participants.

        Returns
        -------
        str
            A signed JWT token string the participant can use to join the room.
        """
        if not settings.LIVEKIT_API_KEY or not settings.LIVEKIT_API_SECRET:
            raise RuntimeError(
                "LIVEKIT_API_KEY and LIVEKIT_API_SECRET must be set to mint tokens."
            )

        grants = VideoGrants(
            room_join=True,
            room=room_name,
            can_publish=is_agent,
            can_subscribe=True,
            room_record=is_agent,   # §13: agents write, humans read
        )

        token = (
            AccessToken(
                api_key=settings.LIVEKIT_API_KEY,
                api_secret=settings.LIVEKIT_API_SECRET,
            )
            .with_identity(participant_identity)
            .with_name(participant_name or participant_identity)
            .with_grants(grants)
        )

        jwt_str: str = token.to_jwt()
        logger.debug(
            "Minted LiveKit token: room=%s identity=%s is_agent=%s",
            room_name,
            participant_identity,
            is_agent,
        )
        return jwt_str

    # ------------------------------------------------------------------
    # Board state broadcast
    # ------------------------------------------------------------------

    async def broadcast_board_state(self, room_name: str, state: dict) -> None:
        """Push a board-state snapshot to all participants in a room.

        The state dict is JSON-encoded and sent via the LiveKit data channel
        so every connected participant (agents, frontend) receives the update
        immediately without polling.

        This is called after every sense/claim/write event to keep all
        participants synchronised with the latest board state.

        Parameters
        ----------
        room_name:
            The LiveKit room to broadcast into.
        state:
            Serialisable dict representing the current board state.
        """
        lk = self._get_api()
        payload: bytes = json.dumps(state, default=str).encode("utf-8")

        request = api.SendDataRequest(
            room=room_name,
            data=payload,
            kind=api.DataPacketKind.DATA_PACKET_KIND_RELIABLE,
        )

        try:
            await lk.room.send_data(request)
            logger.debug(
                "Board state broadcast to room=%s (%d bytes).",
                room_name,
                len(payload),
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Failed to broadcast board state to room=%s: %s",
                room_name,
                exc,
                exc_info=True,
            )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

room_manager = LiveKitRoomManager()
"""Shared :class:`LiveKitRoomManager` instance — import and use directly."""
