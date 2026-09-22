from datetime import datetime, timezone, timedelta
import pytest
from unittest.mock import AsyncMock
from groq import RateLimitError as GroqRateLimitError
from inference.router import InferenceRouter, InferenceError, Provider
from config.settings import settings
from agents.coordination_token import CoordinationToken, bound_coordination_token


def _get_test_token() -> CoordinationToken:
    now = datetime.now(timezone.utc)
    return CoordinationToken(
        run_id="run-router-test",
        task_id="task-router-test",
        claim_id="claim-router-test",
        owner_id="test-agent",
        version="1.0",
        issued_at=now,
        expires_at=now + timedelta(minutes=5),
        status="active",
    )


@pytest.mark.asyncio
async def test_router_propagates_programming_errors(monkeypatch):
    """A bug (TypeError) must not be silently treated as a retryable
    provider failure and rotated past."""
    async def broken_create(*args, **kwargs):
        raise TypeError("simulated bug, not a provider failure")

    monkeypatch.setattr(settings, "GROQ_API_KEY", "mock_groq_key")
    router = InferenceRouter()
    groq_client = router._get_groq_client()
    monkeypatch.setattr(groq_client.chat.completions, "create", broken_create)

    with bound_coordination_token(_get_test_token()):
        with pytest.raises(TypeError) as excinfo:
            await router.complete(messages=[{"role": "user", "content": "hi"}])
        assert "simulated bug, not a provider failure" in str(excinfo.value)


@pytest.mark.asyncio
async def test_router_raises_when_no_providers_configured(monkeypatch):
    """Router raises a descriptive InferenceError immediately if no API keys are set."""
    monkeypatch.setattr(settings, "GROQ_API_KEY", "")
    monkeypatch.setattr(settings, "OPENROUTER_API_KEY", "")
    monkeypatch.setattr(settings, "HF_API_KEY", "")

    router = InferenceRouter()
    with bound_coordination_token(_get_test_token()):
        with pytest.raises(InferenceError) as excinfo:
            await router.complete(messages=[{"role": "user", "content": "hi"}])
        assert "No inference providers configured" in str(excinfo.value)

