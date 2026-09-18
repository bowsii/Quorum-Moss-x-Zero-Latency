"""quorum/benchmark/relay_arm.py
---------------------------------
Conventional message-passing baseline (control arm).
Same 4-agent research task, same tools structure, same question.
Agents communicate via asyncio.Queue (no semantic board).
"""
import asyncio
import uuid
import time
from dataclasses import dataclass, field
from typing import Optional
from datetime import datetime, timezone


@dataclass
class RelayMessage:
    """Message passed along the fixed-graph agent pipeline."""
    sender_id: str
    content: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    message_type: str = "finding"
    metadata: dict = field(default_factory=dict)


class RelayAgent:
    """Agent in a fixed message-passing relay topology."""

    def __init__(self, agent_id: str, role: str, out_queue: Optional[asyncio.Queue] = None):
        self.agent_id = agent_id
        self.role = role
        self.out_queue = out_queue
        self.in_queue: asyncio.Queue[RelayMessage] = asyncio.Queue()

    async def process(self, message: RelayMessage) -> RelayMessage:
        """Process incoming message and produce an augmented finding."""
        # Simulated agent step latency
        await asyncio.sleep(0.02)
        response_content = f"[{self.role} ({self.agent_id})] Processed: {message.content[:80]}... Generated sub-finding."
        out_msg = RelayMessage(
            sender_id=self.agent_id,
            content=response_content,
            message_type="finding",
            metadata={"source_sender": message.sender_id, "role": self.role},
        )
        if self.out_queue:
            await self.out_queue.put(out_msg)
        return out_msg


class RelayArm:
    """Conventional fixed-graph message-passing pipeline (Control Arm)."""

    def __init__(self):
        # 4 agents in sequential pipeline
        self.agent1 = RelayAgent("agent-1-literature", "Literature Researcher")
        self.agent2 = RelayAgent("agent-2-data", "Data Analyst")
        self.agent3 = RelayAgent("agent-3-critique", "Critique Specialist")
        self.agent4 = RelayAgent("agent-4-synthesis", "Synthesis Builder")

        # Wire pipeline: 1 -> 2 -> 3 -> 4
        self.agent1.out_queue = self.agent2.in_queue
        self.agent2.out_queue = self.agent3.in_queue
        self.agent3.out_queue = self.agent4.in_queue

    async def run(self, question: str) -> dict:
        """Execute the fixed message passing pipeline on a question."""
        start_time = time.perf_counter()
        messages_exchanged = []

        init_msg = RelayMessage(
            sender_id="user",
            content=question,
            message_type="query",
        )
        messages_exchanged.append(init_msg)

        # Agent 1 processes initial query
        m1 = await self.agent1.process(init_msg)
        messages_exchanged.append(m1)

        # Agent 2 receives and processes
        in2 = await self.agent2.in_queue.get()
        m2 = await self.agent2.process(in2)
        messages_exchanged.append(m2)

        # Agent 3 receives and processes
        in3 = await self.agent3.in_queue.get()
        m3 = await self.agent3.process(in3)
        messages_exchanged.append(m3)

        # Agent 4 receives and produces synthesis
        in4 = await self.agent4.in_queue.get()
        m4 = await self.agent4.process(in4)
        messages_exchanged.append(m4)

        total_ms = (time.perf_counter() - start_time) * 1000

        return {
            "arm": "relay",
            "question": question,
            "total_latency_ms": round(total_ms, 2),
            "total_messages": len(messages_exchanged),
            "final_synthesis": m4.content,
            "agent_steps": 4,
            "redundancy_count": 0,  # Pipeline has no sense/board deduplication
        }
