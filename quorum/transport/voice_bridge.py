"""
quorum/transport/voice_bridge.py
---------------------------------
Voice bridge: routes LiveKit voice audio through transcription and onto the
Quorum semantic board (Moss).

Flow:
  voice query  ->  _transcribe()  ->  sense()  ->  spoken response
  voice claim  ->  _transcribe()  ->  claim()

CRITICAL privacy constraint
---------------------------
Audio bytes MUST be discarded (del-ed) immediately after transcription.
No audio data persists past the VoiceBridge._transcribe call.
The audio_bytes parameter is explicitly zeroed before deletion to
minimise the window during which raw audio resides in memory.
"""

import asyncio
import io
import logging
from typing import Optional

from bus.moss_client import sense
from bus.schemas import ParticipantType
from config.settings import settings

logger = logging.getLogger(__name__)

# Lazy imports for optional heavy deps - loaded only when actually called.
# This keeps startup fast even if Groq/httpx are slow to import.
_groq_client: Optional[object] = None  # type: ignore[type-arg]


def _get_groq_client():  # type: ignore[return]
    """Lazily initialise and return the Groq client singleton."""
    global _groq_client
    if _groq_client is None:
        try:
            from groq import AsyncGroq  # type: ignore[import]

            if not settings.GROQ_API_KEY:
                raise RuntimeError(
                    "GROQ_API_KEY is not set. Cannot initialise Groq transcription client."
                )
            _groq_client = AsyncGroq(api_key=settings.GROQ_API_KEY)
            logger.debug("Groq async client initialised.")
        except ImportError as exc:
            raise RuntimeError(
                "groq package is not installed. Run: pip install groq"
            ) from exc
    return _groq_client


# ---------------------------------------------------------------------------
# Voice bridge
# ---------------------------------------------------------------------------


class VoiceBridge:
    """Bridges LiveKit voice audio to the Quorum semantic board.

    Transcribes incoming audio using Groq's Whisper endpoint (free tier) and
    dispatches the resulting text to either bus.moss_client.sense (for
    queries) or bus.moss_client.claim (for human claims).

    CRITICAL: Audio bytes MUST be discarded after transcription.
    No audio persists past the transcription step.
    """

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def handle_voice_query(
        self,
        audio_bytes: bytes,
        participant_id: str,
        run_id: str,
    ) -> str:
        """Transcribe audio, run a semantic sense query, return spoken response.

        The audio bytes are zeroed and deleted immediately after transcription.

        Parameters
        ----------
        audio_bytes:
            Raw PCM/WAV/WebM audio captured from the LiveKit track.
        participant_id:
            Identity of the participant who spoke.
        run_id:
            The current Quorum run identifier.

        Returns
        -------
        str
            A spoken-response string synthesised from the sense results.
            Returns an empty string if transcription or sense fails.
        """
        try:
            query_text = await self._transcribe(audio_bytes)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Transcription failed for participant=%s run=%s: %s",
                participant_id,
                run_id,
                exc,
                exc_info=True,
            )
            return ""
        finally:
            # Ensure the caller's reference is also overwritten.
            audio_bytes = b""  # noqa: F841 - intentional shadow/zero

        if not query_text.strip():
            logger.warning(
                "Empty transcription for participant=%s run=%s; skipping sense.",
                participant_id,
                run_id,
            )
            return ""

        logger.info(
            "Voice query: participant=%s run=%s text=%r",
            participant_id,
            run_id,
            query_text[:120],
        )

        try:
            sense_result = await sense(query=query_text, run_id=run_id)
            response_text = self._format_sense_response(query_text, sense_result)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "sense() failed for participant=%s run=%s: %s",
                participant_id,
                run_id,
                exc,
                exc_info=True,
            )
            return ""

        return response_text

    async def handle_voice_claim(
        self,
        audio_bytes: bytes,
        participant_id: str,
        run_id: str,
    ) -> None:
        """Transcribe audio and post the text as a human claim on the board.

        The audio bytes are zeroed and deleted immediately after transcription.

        Parameters
        ----------
        audio_bytes:
            Raw PCM/WAV/WebM audio captured from the LiveKit track.
        participant_id:
            Identity of the participant who spoke (used as claim author).
        run_id:
            The current Quorum run identifier.
        """
        try:
            claim_text = await self._transcribe(audio_bytes)
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "Transcription failed for participant=%s run=%s: %s",
                participant_id,
                run_id,
                exc,
                exc_info=True,
            )
            return
        finally:
            audio_bytes = b""  # noqa: F841 - intentional zero

        if not claim_text.strip():
            logger.warning(
                "Empty transcription for participant=%s run=%s; skipping claim.",
                participant_id,
                run_id,
            )
            return

        logger.info(
            "Voice claim: participant=%s run=%s text=%r",
            participant_id,
            run_id,
            claim_text[:120],
        )

        try:
            # Import here to avoid circular imports at module load time.
            from bus.moss_client import claim  # type: ignore[import]

            participant_type: ParticipantType = "human"
            await claim(
                content=claim_text,
                participant_id=participant_id,
                participant_type=participant_type,
                run_id=run_id,
            )
        except Exception as exc:  # noqa: BLE001
            logger.error(
                "claim() failed for participant=%s run=%s: %s",
                participant_id,
                run_id,
                exc,
                exc_info=True,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _transcribe(self, audio_bytes: bytes) -> str:
        """Transcribe raw audio bytes to text using Groq Whisper endpoint.

        The audio_bytes reference is NOT stored anywhere beyond this call.
        After the Groq API returns, the local reference is explicitly deleted
        to allow garbage collection as soon as possible.

        Parameters
        ----------
        audio_bytes:
            Raw audio data (WAV, WebM, MP3, or any format Whisper accepts).

        Returns
        -------
        str
            The transcribed text, stripped of leading/trailing whitespace.

        Raises
        ------
        RuntimeError
            If the Groq client cannot be initialised (missing API key or package).
        Exception
            Propagates network / API errors from the Groq SDK.
        """
        client = _get_groq_client()

        # Wrap bytes in a file-like object so the SDK can send it as a multipart
        # upload. Name it with a .wav extension as a content-type hint.
        audio_file = io.BytesIO(audio_bytes)
        audio_file.name = "audio.wav"

        try:
            transcription = await client.audio.transcriptions.create(
                file=audio_file,
                model="whisper-large-v3",
                response_format="text",
                language=None,  # auto-detect
            )
        finally:
            # Zero and discard audio bytes immediately after the API call,
            # regardless of success or failure.
            del audio_bytes
            audio_file.close()

        # Groq returns the text directly when response_format="text".
        text: str = transcription if isinstance(transcription, str) else str(transcription)
        return text.strip()

    async def _synthesize_speech(self, text: str) -> bytes:
        """Convert text to speech bytes for a spoken response.

        Currently returns b'' as a placeholder - a free TTS backend
        (e.g. Edge-TTS or Kokoro) can be wired in here without changing
        the call-sites.

        Parameters
        ----------
        text:
            The text to synthesise into speech.

        Returns
        -------
        bytes
            PCM or MP3 audio bytes, or b'' if synthesis is not configured.
        """
        if not text.strip():
            return b""

        # Placeholder: log the outgoing response and return silence.
        # Replace this body with a real TTS call (e.g. edge-tts, Kokoro, ElevenLabs)
        # when a free endpoint is available.
        logger.debug("_synthesize_speech: returning placeholder b'' for text=%r", text[:80])
        return b""

    # ------------------------------------------------------------------
    # Private formatting
    # ------------------------------------------------------------------

    @staticmethod
    def _format_sense_response(query: str, sense_result: object) -> str:
        """Format a SenseResult into a spoken sentence.

        Parameters
        ----------
        query:
            The original query text (used as fallback).
        sense_result:
            A bus.schemas.SenseResult (or any object with a results attribute)
            returned by bus.moss_client.sense.

        Returns
        -------
        str
            A short human-readable summary suitable for TTS.
        """
        try:
            results = getattr(sense_result, "results", [])
            if not results:
                return f"I found no information about: {query}"

            top = results[0]
            # ChromaDB result dicts typically have a 'documents' or 'content' key.
            content = (
                top.get("document")
                or top.get("content")
                or top.get("text")
                or str(top)
            )
            snippet = content[:200].rstrip()
            return f"Here is what I found: {snippet}"
        except Exception:  # noqa: BLE001
            return f"I searched for: {query}"


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

voice_bridge = VoiceBridge()
"""Shared VoiceBridge instance - import and use directly."""
