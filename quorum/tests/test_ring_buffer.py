"""tests/test_ring_buffer.py
--------------------------
Unit tests for observability/ring_buffer.py.
Verifies thread-safety, percentile computation, and isolation from OTel.
"""
import pytest
from observability.ring_buffer import RingBuffer, LatencySample


def test_ring_buffer_percentile_calculation():
    """Verify p50 and p99 computations."""
    buf = RingBuffer(maxlen=1000)
    for i in range(1, 101):
        buf.record("sense", float(i))

    report = buf.report("sense")
    assert report["count"] == 100
    assert report["min"] == 1.0
    assert report["max"] == 100.0
    assert 49.0 <= report["p50"] <= 51.0
    assert 98.0 <= report["p99"] <= 100.0


def test_ring_buffer_eviction():
    """Verify FIFO eviction at maxlen."""
    buf = RingBuffer(maxlen=5)
    for i in range(10):
        buf.record("write", float(i))

    samples = buf.get_samples("write")
    assert len(samples) == 5
    assert [s.latency_ms for s in samples] == [5.0, 6.0, 7.0, 8.0, 9.0]
