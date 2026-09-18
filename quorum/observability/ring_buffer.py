"""
quorum/observability/ring_buffer.py
------------------------------------
In-process ring buffer for benchmark latency capture.

COMPLETELY SEPARATE from the OpenTelemetry pipeline — zero shared state.
Used exclusively by the benchmark suite and any code that needs sub-millisecond
overhead percentile tracking without exporting spans to a collector.

Usage::

    from quorum.observability.ring_buffer import ring_buffer

    ring_buffer.record("sense", latency_ms=2.4, collection="quorum_findings")
    report = ring_buffer.report("sense")
    # {'p50': 2.1, 'p99': 9.8, 'count': 1000, 'mean': 3.2, 'max': 12.1, 'min': 0.8}
"""

import threading
import time
import statistics
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Data container
# ---------------------------------------------------------------------------

@dataclass
class LatencySample:
    """Immutable record of a single timed operation.

    Attributes:
        operation:   Logical operation name (e.g. ``"sense"``, ``"write"``).
        latency_ms:  Elapsed time in milliseconds.
        timestamp:   Wall-clock epoch at which the sample was recorded.
        context:     Arbitrary key-value metadata (collection name, run ID, …).
    """

    operation: str
    latency_ms: float
    timestamp: float = field(default_factory=time.time)
    context: dict = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Ring buffer
# ---------------------------------------------------------------------------

class RingBuffer:
    """Thread-safe, bounded ring buffer for latency samples.

    Samples older than ``maxlen`` are silently dropped (FIFO eviction).
    All public methods acquire an internal :class:`threading.Lock` so the
    buffer is safe to feed from concurrent threads *and* asyncio tasks running
    via ``loop.run_in_executor``.

    Args:
        maxlen: Maximum number of samples held in memory at any time.
                Defaults to 10 000.
    """

    def __init__(self, maxlen: int = 10_000) -> None:
        self._buf: deque[LatencySample] = deque(maxlen=maxlen)
        self._lock = threading.Lock()
        self._maxlen = maxlen

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------

    def record(self, operation: str, latency_ms: float, **context) -> None:
        """Append a new latency sample to the ring buffer.

        Args:
            operation:   Name of the timed operation.
            latency_ms:  Duration of the operation in milliseconds.
            **context:   Optional keyword arguments stored as sample context
                         (e.g. ``collection="quorum_findings"``).
        """
        sample = LatencySample(
            operation=operation,
            latency_ms=latency_ms,
            context=dict(context),
        )
        with self._lock:
            self._buf.append(sample)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------

    def get_samples(self, operation: Optional[str] = None) -> list[LatencySample]:
        """Return a snapshot of samples, optionally filtered by operation name.

        Args:
            operation: If supplied, only samples whose ``operation`` field
                       matches this string are returned.  ``None`` returns all.

        Returns:
            A new list (copy) so callers may freely mutate it without
            affecting the buffer.
        """
        with self._lock:
            if operation is None:
                return list(self._buf)
            return [s for s in self._buf if s.operation == operation]

    def percentile(self, operation: str, p: float) -> float:
        """Compute the *p*-th percentile latency for a named operation.

        Uses linear interpolation (same as NumPy's default).

        Args:
            operation: Operation name to filter on.
            p:         Percentile in the range [0, 100].

        Returns:
            Percentile value in milliseconds.

        Raises:
            ValueError: If no samples exist for the given operation or *p* is
                        outside [0, 100].
        """
        if not 0 <= p <= 100:
            raise ValueError(f"Percentile p must be in [0, 100], got {p}")

        samples = self.get_samples(operation)
        if not samples:
            raise ValueError(
                f"No samples found for operation '{operation}'. "
                "Did you forget to call record()?"
            )

        latencies = sorted(s.latency_ms for s in samples)
        n = len(latencies)

        # Linear interpolation between adjacent ranks
        idx_float = (p / 100) * (n - 1)
        lo = int(idx_float)
        hi = min(lo + 1, n - 1)
        frac = idx_float - lo
        return latencies[lo] + frac * (latencies[hi] - latencies[lo])

    def report(self, operation: str) -> dict:
        """Compute a summary statistics dict for the named operation.

        Args:
            operation: Operation name to summarise.

        Returns:
            Dictionary with keys:
            ``p50``, ``p99``, ``count``, ``mean``, ``max``, ``min``.
            All latency values are in milliseconds (floats).
            Returns a zeroed report if no samples exist (no exception raised).
        """
        samples = self.get_samples(operation)
        if not samples:
            return {
                "p50": 0.0,
                "p99": 0.0,
                "count": 0,
                "mean": 0.0,
                "max": 0.0,
                "min": 0.0,
            }

        latencies = sorted(s.latency_ms for s in samples)
        n = len(latencies)

        def _pct(p: float) -> float:
            idx_float = (p / 100) * (n - 1)
            lo = int(idx_float)
            hi = min(lo + 1, n - 1)
            frac = idx_float - lo
            return latencies[lo] + frac * (latencies[hi] - latencies[lo])

        return {
            "p50": round(_pct(50), 4),
            "p99": round(_pct(99), 4),
            "count": n,
            "mean": round(statistics.mean(latencies), 4),
            "max": round(latencies[-1], 4),
            "min": round(latencies[0], 4),
        }

    # ------------------------------------------------------------------
    # Housekeeping
    # ------------------------------------------------------------------

    def clear(self) -> None:
        """Remove all samples from the buffer."""
        with self._lock:
            self._buf.clear()

    def __len__(self) -> int:
        with self._lock:
            return len(self._buf)

    def __repr__(self) -> str:
        return f"RingBuffer(maxlen={self._maxlen}, current_size={len(self)})"


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

#: Process-global ring buffer instance.  Import and use this everywhere instead
#: of constructing a new :class:`RingBuffer` — sharing the singleton allows the
#: benchmark harness to read cross-module latency data in one place.
ring_buffer: RingBuffer = RingBuffer()
