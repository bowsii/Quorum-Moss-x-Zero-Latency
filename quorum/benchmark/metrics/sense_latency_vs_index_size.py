"""quorum/benchmark/metrics/sense_latency_vs_index_size.py
-----------------------------------------------------------
Evaluates and curves sense() query latency across scaling index sizes.
"""
import time
import uuid
import random
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import chromadb
from bus.namespaces import FastFeatureEmbeddingFunction

INDEX_SIZES = [10, 50, 100, 250, 500, 1000]


def benchmark_sense_latency_curve(sizes: list[int] = None, n_queries: int = 100) -> dict:
    """Measure sense latency across index sizes."""
    if sizes is None:
        sizes = INDEX_SIZES

    client = chromadb.EphemeralClient()
    fn = FastFeatureEmbeddingFunction(dim=384)
    results = {}

    for size in sizes:
        col_name = f"growth_bench_{size}"
        col = client.get_or_create_collection(
            name=col_name,
            embedding_function=fn,
            metadata={"hnsw:space": "cosine"},
        )

        # Populate with synthetic documents
        batch_docs = [
            f"Research finding #{i}: experimental observation on autonomous multi-agent coordination."
            for i in range(size)
        ]
        batch_ids = [f"id-{size}-{i}" for i in range(size)]
        col.add(documents=batch_docs, ids=batch_ids)

        # Query benchmark
        q_times = []
        for _ in range(n_queries):
            t0 = time.perf_counter()
            col.query(query_texts=["autonomous multi-agent coordination"], n_results=5)
            q_times.append((time.perf_counter() - t0) * 1000.0)

        q_times.sort()
        p50 = q_times[int(0.50 * len(q_times))]
        p99 = q_times[int(0.99 * len(q_times))]
        results[str(size)] = {
            "p50_ms": round(p50, 3),
            "p99_ms": round(p99, 3),
            "mean_ms": round(sum(q_times) / len(q_times), 3),
        }

    return {
        "index_sizes": results,
        "all_sub_10ms": all(r["p99_ms"] < 10.0 for r in results.values()),
    }


if __name__ == "__main__":
    res = benchmark_sense_latency_curve()
    print("Sense Latency vs Index Size:")
    for size, stats in res["index_sizes"].items():
        print(f"  Size {size:>4} docs -> p50: {stats['p50_ms']}ms, p99: {stats['p99_ms']}ms")
    print(f"All under 10ms gate: {res['all_sub_10ms']}")
