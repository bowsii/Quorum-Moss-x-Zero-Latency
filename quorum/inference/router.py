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
from typing import Any, Optional

from groq import (
    AsyncGroq,
    RateLimitError as GroqRateLimitError,
    APIStatusError as GroqAPIStatusError,
    APIConnectionError as GroqConnectionError,
    APITimeoutError as GroqTimeoutError,
)
from openai import (
    AsyncOpenAI,
    RateLimitError as OpenAIRateLimitError,
    APIStatusError as OpenAIAPIStatusError,
    APIConnectionError as OpenAIConnectionError,
    APITimeoutError as OpenAITimeoutError,
)
from config.settings import settings
from agents.coordination_token import (
    CoordinationToken,
    UnapprovedExternalWorkError,
    verify_coordination_capability,
)

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
    """Manages chat completion with automatic failover across three providers.

    If Groq returns a 429 rate limit or 5xx error, the router rotates to
    OpenRouter, then to Hugging Face, and finally wraps back to Groq.  On a
    successful response the working provider is kept as the new default
    (sticky failover) to avoid repeatedly hitting a provider that just failed.

    Attributes:
        current_provider: The provider that will be attempted first on the
            next call.  May be updated by rotation.
        rotation_count: Running total of provider-rotation events.
    """

    def __init__(self) -> None:
        self.current_provider: Provider = Provider.GROQ
        self.rotation_count: int = 0
        self._groq_client: AsyncGroq | None = None
        self._openrouter_client: AsyncOpenAI | None = None
        self._hf_client: AsyncOpenAI | None = None

    def _get_groq_client(self) -> AsyncGroq:
        if self._groq_client is None:
            self._groq_client = AsyncGroq(api_key=settings.GROQ_API_KEY)
        return self._groq_client

    def _get_openrouter_client(self) -> AsyncOpenAI:
        if self._openrouter_client is None:
            self._openrouter_client = AsyncOpenAI(
                api_key=settings.OPENROUTER_API_KEY,
                base_url="https://openrouter.ai/api/v1",
            )
        return self._openrouter_client

    def _get_hf_client(self) -> AsyncOpenAI:
        if self._hf_client is None:
            self._hf_client = AsyncOpenAI(
                api_key=settings.HF_API_KEY,
                base_url="https://api-inference.huggingface.co/v1",
            )
        return self._hf_client

    def _has_key(self, provider: Provider) -> bool:
        if provider is Provider.GROQ:
            return bool(settings.GROQ_API_KEY)
        if provider is Provider.OPENROUTER:
            return bool(settings.OPENROUTER_API_KEY)
        if provider is Provider.HUGGINGFACE:
            return bool(settings.HF_API_KEY)
        return False

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
        if isinstance(exc, (GroqRateLimitError, GroqConnectionError, GroqTimeoutError)):
            return True
        if isinstance(exc, GroqAPIStatusError) and exc.status_code and exc.status_code >= 500:
            return True
        return False

    @staticmethod
    def _is_retryable_openai(exc: Exception) -> bool:
        """Return True if an OpenAI-compatible exception warrants a provider rotation."""
        if isinstance(exc, (OpenAIRateLimitError, OpenAIConnectionError, OpenAITimeoutError)):
            return True
        if isinstance(exc, OpenAIAPIStatusError) and exc.status_code and exc.status_code >= 500:
            return True
        return False

    # ------------------------------------------------------------------
    # Core API
    # ------------------------------------------------------------------

    async def _call_groq(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the Groq native SDK."""
        client = self._get_groq_client()
        response = await client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def _call_openrouter(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the OpenRouter OpenAI-compatible endpoint."""
        client = self._get_openrouter_client()
        response = await client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def _call_hf(self, messages: list[dict], model: str) -> str:
        """Perform a chat completion via the HuggingFace Inference API."""
        client = self._get_hf_client()
        response = await client.chat.completions.create(
            model=model,
            messages=messages,  # type: ignore[arg-type]
        )
        return response.choices[0].message.content or ""

    async def complete(
        self,
        messages: list[dict] | None = None,
        prompt: str | None = None,
        model: str | None = None,
        max_tokens: int | None = None,
        max_retries: int = 3,
        coordination_token: Optional[CoordinationToken] = None,
    ) -> str:
        """Send a chat-completion request, rotating providers on transient errors.

        Firewall:
            Requires a valid CoordinationToken in the execution context or via
            coordination_token argument. Raises UnapprovedExternalWorkError if unauthorized.

        Raises:
            UnapprovedExternalWorkError: If no valid coordination capability is held.
            InferenceError: When all providers are exhausted or none are configured.
        """
        # ------------------------------------------------------------------
        # Coordination Firewall Gate Check (HARD REJECT IF UNAUTHORIZED)
        # ------------------------------------------------------------------
        verify_coordination_capability(token=coordination_token)

        if messages is None:
            if prompt is not None:
                messages = [{"role": "user", "content": prompt}]
            else:
                messages = []

        if not any(self._has_key(p) for p in _PROVIDER_SEQUENCE):
            raise InferenceError(
                "No inference providers configured (missing API keys for Groq, OpenRouter, and Hugging Face). "
                "Configure GROQ_API_KEY, OPENROUTER_API_KEY, or HF_API_KEY."
            )

        attempts = 0
        provider = self.current_provider

        start_idx = _PROVIDER_SEQUENCE.index(provider)
        ordered = (
            _PROVIDER_SEQUENCE[start_idx:]
            + _PROVIDER_SEQUENCE[:start_idx]
        )

        errors: dict[str, str] = {}
        last_exc: Exception | None = None

        for attempt_provider in ordered:
            if attempts >= max_retries:
                break

            if not self._has_key(attempt_provider):
                errors[attempt_provider.value] = "not configured"
                logger.debug("InferenceRouter: skipping unconfigured provider %s", attempt_provider.value)
                continue

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
                    errors[attempt_provider.value] = f"{type(exc).__name__}: {exc}"
                    logger.warning(
                        "InferenceRouter: transient error from %s (%s: %s) — rotating",
                        attempt_provider.value,
                        type(exc).__name__,
                        exc,
                    )
                    last_exc = exc
                    self._rotate(attempt_provider)
                    continue

                # Non-retryable error (e.g. KeyError, TypeError, auth error) — propagate immediately
                logger.error(
                    "InferenceRouter: non-retryable error from %s: %s",
                    attempt_provider.value,
                    exc,
                )
                raise

        raise InferenceError(
            f"All providers exhausted after {attempts} attempt(s). "
            f"Errors: {errors}. Last error: {last_exc!r}"
        )


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

inference_router = InferenceRouter()
