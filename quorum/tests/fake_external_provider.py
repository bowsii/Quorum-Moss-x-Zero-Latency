"""
tests/fake_external_provider.py
--------------------------------
Deterministic fake external provider for testing the Quorum coordination invariant.
Tracks external call counts without making real internet requests.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from agents.coordination_token import CoordinationToken, verify_coordination_capability


@dataclass
class ExternalCallRecord:
    call_index: int
    tool_name: str
    payload: Any
    token: CoordinationToken
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class FakeExternalProvider:
    """Deterministic in-memory external service provider.

    Enforces that ANY external execution must possess a valid CoordinationToken,
    records every attempt, and counts total external executions.
    """

    def __init__(self) -> None:
        self.call_count: int = 0
        self.call_records: list[ExternalCallRecord] = []
        self._lock = asyncio.Lock()

    def reset(self) -> None:
        self.call_count = 0
        self.call_records.clear()

    async def execute_search(
        self,
        query: str,
        coordination_token: Optional[CoordinationToken] = None,
        latency_seconds: float = 0.001,
    ) -> list[dict]:
        """Simulate Tavily web search behind the coordination firewall."""
        token = verify_coordination_capability(token=coordination_token)

        if latency_seconds > 0:
            await asyncio.sleep(latency_seconds)

        async with self._lock:
            self.call_count += 1
            record = ExternalCallRecord(
                call_index=self.call_count,
                tool_name="tavily_search",
                payload={"query": query},
                token=token,
            )
            self.call_records.append(record)

        return [
            {
                "url": f"https://example.com/result-{self.call_count}",
                "title": f"Deterministic Fake Result for {query[:30]}",
                "content": f"Content snippet for {query[:50]}",
                "score": 0.99,
            }
        ]

    async def execute_inference(
        self,
        prompt: str,
        coordination_token: Optional[CoordinationToken] = None,
        latency_seconds: float = 0.001,
    ) -> str:
        """Simulate LLM inference behind the coordination firewall."""
        token = verify_coordination_capability(token=coordination_token)

        if latency_seconds > 0:
            await asyncio.sleep(latency_seconds)

        async with self._lock:
            self.call_count += 1
            record = ExternalCallRecord(
                call_index=self.call_count,
                tool_name="inference_router",
                payload={"prompt": prompt},
                token=token,
            )
            self.call_records.append(record)

        return f"Synthesized research finding based on {prompt[:40]}"


fake_provider = FakeExternalProvider()
