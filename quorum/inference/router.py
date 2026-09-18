"""
quorum/inference/router.py
--------------------------
InferenceRouter: Groq (primary) → OpenRouter (failover) → HuggingFace (third).

Rotates on HTTP 429 (RateLimitError) or 5xx server errors.  After
``max_retries`` total failures across all providers the router raises
``InferenceError``.

Agents MUST call ``inference_router.complete()`` — never talk to provider
SDKs directly.

Module-level singleton::

    from inference.router import inference_router

    response = await inference_router.complete(messages=[...])
"""

import asyncio
import logging
from enum import Enum
from typing import Any

from groq import AsyncGroq, RateLimitError as GroqRateLimitError
from groq import APIStatusError as GroqAPIStatusError
from openai import AsyncOpenAI, RateLimitError as OpenAIRateLimitError
from openai import APIStatusError as OpenAIAPIStatusError
from config.settings import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public types
# ---------------------------------------------------------------------------


class Provider(Enum):
    """The three supported inference providers, in priority order."""

    GROQ = "groq"
    OPENROUTER = "openrouter"
    HUGGINGFACE = "huggingface"


class InferenceError(Exception):
    """Raised when all providers have been exhausted without a successful response."""


# ---------------------------------------------------------------------------
# Provider configuration constants
# ---------------------------------------------------------------------------

_GROQ_MODEL = "llama-3.1-8b-instant"
_OPENROUTER_MODEL = "meta-llama/llama-3.1-8b-instruct:free"
_HUGGINGFACE_MODEL = "meta-llama/Meta-Llama-3-8B-Instruct"

_PROVIDER_SEQUENCE: list[Provider] = [
    Provider.GROQ,
    Provider.OPENROUTER,
    Provider.HUGGINGFACE,
]


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------


class InferenceRouter:
    """Routes LLM chat-completion requests across three providers with failover.

    Priority order:
      1. Groq (fastest, generous free tier)
      2. OpenRouter (OpenAI-compatible proxy, many free models)
      3. HuggingFace Inference API (OpenAI-compatible endpoint)

    On ``RateLimitError`` or HTTP 5xx the router increments ``rotation_count``
    and tries the next provider.  After ``max_retries`` total failures it raises
    :class:`InferenceError`.

    Attributes:
        current_provider: The provider that will be attempted first on the
            next call.  May be updated by rotation.
        rotation_count: Running total of provider-rotation events.
    """

    def __init__(self) -> None:
        self.current_provider: Provider = Provider.GROQ
        self.rotation_count: int = 0

        # Groq native client
        self._groq_client = AsyncGroq(api_key=settings.GROQ_API_KEY or "gsk_dummy_development_key")

        # OpenAI-compatible client pointing at OpenRouter
        self._openrouter_client = AsyncOpenAI(
            api_key=settings.OPENROUTER_API_KEY or "dummy_openrouter_key",
            base_url="https://openrouter.ai/api/v1",
        )

        # OpenAI-compatible client pointing at HuggingFace
        self._hf_client = AsyncOpenAI(
            api_key=settings.HF_API_KEY or "dummy_hf_key",
            base_url="https://api-inference.huggingface.co/v1",
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _rotate(self, from_provider: Provider) -> Provider:
        """Advance to the next provider in sequence and log the rotation."""
        idx = _PROVIDER_SEQUENCE.index(from_provider)
        next_idx = (idx + 1) % len(_PROVIDER_SEQUENCE)
        next_provider = _PROVIDER_SEQUENCE[next_idx]
        self.rotation_count += 1
        logger.warning(
            "InferenceRouter rotating: %s → %s  (rotation #%d)",
            from_provider.value,
            next_provider.value,
            self.rotation_count,
        )
        return next_provider

    @staticmethod
    def _is_retryable_groq(exc: Exception) -> bool:
        """Return True if the Groq exception warrants a provider rotation."""
        if isinstance(exc, GroqRateLimitError):
            return True
        if isinstance(exc, GroqAPIStatusError) and exc.status_code >= 500:
            return True
        return False

    @staticmethod
    def _is_retryable_openai(exc: Exception) -> bool:
        """Return True if an OpenAI-compatible exception warrants a provider rotation."""
        if isinstance(exc, OpenAIRateLimitError):
            return True
        if isinstance(exc, OpenAIAPIStatusError) and exc.status_code >= 500:
            return True
        return False

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    async def _call_groq(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the Groq native SDK."""
        response = await self._groq_client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def _call_openrouter(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the OpenRouter OpenAI-compatible endpoint."""
        response = await self._openrouter_client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def _call_hf(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the HuggingFace Inference API."""
        response = await self._hf_client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def complete(
        self,
        messages: list[dict],
        model: str | None = None,
        max_retries: int = 3,
    ) -> str:
        """Send a chat-completion request, rotating providers on transient errors.

        Args:
            messages: OpenAI-format message list, e.g.
                ``[{"role": "user", "content": "Hello"}]``.
            model: Override the default model for the current provider.
                When ``None`` the per-provider default is used.
            max_retries: Maximum number of provider attempts before raising
                :class:`InferenceError`.

        Returns:
            The assistant's reply as a plain string.

        Raises:
            InferenceError: When every available provider has been tried
                ``max_retries`` times without success.
        """
        attempts = 0
        provider = self.current_provider

        # Normalise the provider sequence so we always start from
        # ``current_provider`` and cycle through the remaining ones.
        start_idx = _PROVIDER_SEQUENCE.index(provider)
        ordered = (
            _PROVIDER_SEQUENCE[start_idx:]
            + _PROVIDER_SEQUENCE[:start_idx]
        )

        last_exc: Exception | None = None

        for attempt_provider in ordered:
            if attempts >= max_retries:
                break
            attempts += 1

            try:
                if attempt_provider is Provider.GROQ:
                    chosen_model = model or _GROQ_MODEL
                    logger.debug("InferenceRouter → Groq model=%s", chosen_model)
                    result = await self._call_groq(messages, chosen_model)

                elif attempt_provider is Provider.OPENROUTER:
                    chosen_model = model or _OPENROUTER_MODEL
                    logger.debug(
                        "InferenceRouter → OpenRouter model=%s", chosen_model
                    )
                    result = await self._call_openrouter(messages, chosen_model)

                else:  # HUGGINGFACE
                    chosen_model = model or _HUGGINGFACE_MODEL
                    logger.debug(
                        "InferenceRouter → HuggingFace model=%s", chosen_model
                    )
                    result = await self._call_hf(messages, chosen_model)

                # Success — update current_provider for sticky routing benefit
                self.current_provider = attempt_provider
                return result

            except Exception as exc:
                should_rotate = (
                    self._is_retryable_groq(exc)
                    if attempt_provider is Provider.GROQ
                    else self._is_retryable_openai(exc)
                )

                if should_rotate:
                    logger.warning(
                        "InferenceRouter: transient error from %s (%s: %s) — rotating",
                        attempt_provider.value,
                        type(exc).__name__,
                        exc,
                    )
                    last_exc = exc
                    provider = self._rotate(attempt_provider)
                    continue

                # Non-retryable error — propagate immediately
                logger.error(
                    "InferenceRouter: non-retryable error from %s: %s",
                    attempt_provider.value,
                    exc,
                )
                raise

        raise InferenceError(
            f"All providers exhausted after {attempts} attempt(s). "
            f"Last error: {last_exc!r}"
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

inference_router = InferenceRouter()
