"""
quorum/inference/instructor_client.py
--------------------------------------
Instructor-wrapped structured-output calls using Groq as the primary backend.

On ``ValidationError`` the client automatically retries the call once (1 repair
retry) with the validation error appended to the conversation so the LLM can
self-correct.  If the repair also fails, the ``ValidationError`` is re-raised.

Module-level singleton::

    from inference.instructor_client import instructor_client

    result = await instructor_client.structured_complete(
        messages=[{"role": "user", "content": "..."}],
        response_model=MyModel,
    )
"""

import logging
from typing import TypeVar, Type

import instructor
from pydantic import BaseModel, ValidationError
from groq import AsyncGroq

from config.settings import settings

logger = logging.getLogger(__name__)

T = TypeVar("T", bound=BaseModel)


class InstructorClient:
    """Wraps ``instructor`` for structured LLM output with automatic repair retry.

    The primary path uses ``instructor.from_groq()`` which patches the Groq
    async client to validate responses against a Pydantic model.

    If Groq's structured response fails Pydantic validation the client sends
    a follow-up repair message (containing the validation error) and tries
    once more.  A second consecutive failure raises the original
    ``ValidationError`` so callers can decide how to handle it.

    Attributes:
        _groq_raw: Underlying ``AsyncGroq`` client (also used by the router).
        _client: ``instructor``-patched Groq async client.
    """

    def __init__(self) -> None:
        self._groq_raw = AsyncGroq(api_key=settings.GROQ_API_KEY)
        # Patch the raw Groq client so instructor can intercept completions
        # and validate/coerce them into Pydantic models automatically.
        self._client = instructor.from_groq(self._groq_raw, mode=instructor.Mode.JSON)

    async def structured_complete(
        self,
        messages: list[dict],
        response_model: Type[T],
        max_repair_retries: int = 1,
    ) -> T:
        """Execute a structured chat-completion, validating output against *response_model*.

        Args:
            messages: OpenAI-format message list.
            response_model: The Pydantic model class the LLM response must
                conform to.
            max_repair_retries: How many times to retry after a
                ``ValidationError`` before re-raising.  Defaults to ``1``.

        Returns:
            A validated instance of *response_model*.

        Raises:
            ValidationError: If the LLM output still fails validation after
                all repair retries are exhausted.
            Exception: Any non-validation exception from the underlying Groq
                call is propagated immediately without retrying.
        """
        repair_attempts = 0
        repair_messages = list(messages)  # mutable copy

        while True:
            try:
                result: T = await self._client.chat.completions.create(
                    model="llama-3.1-8b-instant",
                    messages=repair_messages,  # type: ignore[arg-type]
                    response_model=response_model,
                )
                if repair_attempts > 0:
                    logger.info(
                        "InstructorClient: structured output succeeded after %d repair attempt(s)",
                        repair_attempts,
                    )
                return result

            except ValidationError as exc:
                if repair_attempts >= max_repair_retries:
                    logger.error(
                        "InstructorClient: validation failed after %d repair attempt(s), "
                        "raising. model=%s errors=%s",
                        repair_attempts,
                        response_model.__name__,
                        exc.errors(),
                    )
                    raise

                repair_attempts += 1
                logger.warning(
                    "InstructorClient: ValidationError (attempt %d/%d) for model=%s — "
                    "appending repair prompt. errors=%s",
                    repair_attempts,
                    max_repair_retries,
                    response_model.__name__,
                    exc.errors(),
                )

                # Append the assistant placeholder and user repair instruction
                # so the model can self-correct on the next attempt.
                repair_messages = repair_messages + [
                    {
                        "role": "user",
                        "content": (
                            "Your previous response could not be parsed. "
                            f"Validation errors: {exc.errors()}. "
                            "Please respond again with valid JSON that strictly matches "
                            f"the required schema for {response_model.__name__}."
                        ),
                    }
                ]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

instructor_client = InstructorClient()
