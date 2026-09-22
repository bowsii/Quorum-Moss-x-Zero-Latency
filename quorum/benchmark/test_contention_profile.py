"""
Test full-system contention profile:
- 5 concurrent claim-writer/sensor tasks (doing 100 sense + write ops each)
- Active Reaper scan loop
- Active Adjudicator scan loop
- Active heartbeat tasks
"""
import asyncio
import time
import uuid
import statistics
from unittest.mock import AsyncMock
from bus.schemas import Claim, Finding
from bus.moss_client import sense, write_claim, write_finding, heartbeat, update_claim_status
from reaper.reaper import Reaper
from adjudicator.adjudicator import Adjudicator
from inference.router import inference_router

# Mock LLM inference to simulate fast local response without missing-key timeouts
inference_router.complete = AsyncMock(return_value="Tension between findings analyzed.")


async def run_full_contention_benchmark():
    reaper = Reaper(ttl=2, scan_interval=1)
    adjudicator = Adjudicator()

    # Seed findings for adjudicator to scan
    for i in range(10):
        f = Finding(
            run_id="contention-run",
            participant_id="agent-seed",
            participant_type="agent",
            title=f"Initial finding {i}",
            content=f"Initial research content finding {i} on multi-agent consensus.",
        )
        await write_finding(f)

    # Start background Reaper and Adjudicator tasks
    reaper_task = asyncio.create_task(reaper.run(), name="reaper_contention")
    adjudicator_task = asyncio.create_task(adjudicator.run(), name="adjudicator_contention")

    sense_latencies: list[float] = []
    write_latencies: list[float] = []
    lock = asyncio.Lock()

    n_workers = 5
    n_ops_per_worker = 100

    async def worker(worker_id: int):
        local_senses = []
        local_writes = []
        for i in range(n_ops_per_worker):
            # 1. Sense
            t0 = time.perf_counter()
            await sense(query=f"worker {worker_id} query {i} coordination", namespace="claims", n_results=5)
            dt_sense = (time.perf_counter() - t0) * 1000.0
            local_senses.append(dt_sense)

            # 2. Write claim
            claim = Claim(
                run_id="contention-run",
                participant_id=f"agent-worker-{worker_id}",
                participant_type="agent",
                content=f"Worker {worker_id} claim iteration {i}: exploring consensus",
            )
            t1 = time.perf_counter()
            await write_claim(claim)
            dt_write = (time.perf_counter() - t1) * 1000.0
            local_writes.append(dt_write)

            # 3. Intermittent heartbeat & finding writes
            if i % 10 == 0:
                await heartbeat(claim.id)
                await write_finding(Finding(
                    run_id="contention-run",
                    participant_id=f"agent-worker-{worker_id}",
                    participant_type="agent",
                    title=f"Worker {worker_id} sub-finding {i}",
                    content=f"Sub-finding from worker {worker_id} on iteration {i}",
                ))

            await asyncio.sleep(0.001)

        async with lock:
            sense_latencies.extend(local_senses)
            write_latencies.extend(local_writes)

    # Launch all workers
    worker_tasks = [asyncio.create_task(worker(i)) for i in range(n_workers)]
    await asyncio.gather(*worker_tasks)

    # Cancel background loops
    reaper_task.cancel()
    adjudicator_task.cancel()
    try:
        await asyncio.gather(reaper_task, adjudicator_task, return_exceptions=True)
    except Exception:
        pass

    def p(arr, pct):
        s = sorted(arr)
        idx = min(int((pct / 100.0) * len(s)), len(s) - 1)
        return s[idx]

    sense_p50 = p(sense_latencies, 50)
    sense_p99 = p(sense_latencies, 99)
    write_p50 = p(write_latencies, 50)
    write_p99 = p(write_latencies, 99)

    print("=" * 60)
    print("FULL CONTENTION BENCHMARK RESULTS")
    print(f"Workers: {n_workers} concurrent agents + Reaper + Adjudicator")
    print(f"Total Claims Written: {len(write_latencies)}, Total Senses: {len(sense_latencies)}")
    print(f"Sense Latency: p50={sense_p50:.3f}ms, p99={sense_p99:.3f}ms (target <20ms)")
    print(f"Write Latency: p50={write_p50:.3f}ms, p99={write_p99:.3f}ms")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_full_contention_benchmark())
