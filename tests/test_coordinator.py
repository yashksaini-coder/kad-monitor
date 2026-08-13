"""
tests/test_coordinator.py
=========================
pytest + trio test suite for DHTQueryCoordinator.

Run with:
    pytest tests/ -v --tb=short

Requires:
    pip install pytest pytest-trio
"""

from __future__ import annotations

import time
from typing import List

import pytest
import trio

from src.coordinator import DHTQueryCoordinator, QueryPriority, QueryStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def fast_query(peer_id: str):
    """Instant successful query."""
    await trio.sleep(0.01)
    return True, [f"peer_{i}" for i in range(5)], 3


async def slow_query(peer_id: str):
    """Query that takes 2 seconds."""
    await trio.sleep(2.0)
    return True, [], 1


async def failing_query(peer_id: str):
    """Query that always raises."""
    await trio.sleep(0.05)
    raise ConnectionError("Simulated stream error")


async def unknown_peer_query(peer_id: str):
    """Query that returns not-found."""
    await trio.sleep(0.1)
    return False, [f"close_{i}" for i in range(3)], 5


def make_coordinator(**kwargs) -> DHTQueryCoordinator:
    defaults = {
        "max_concurrent_queries": 5,
        "max_random_walks": 2,
        "query_timeout": 1.0,
    }
    defaults.update(kwargs)
    return DHTQueryCoordinator(**defaults)


# ---------------------------------------------------------------------------
# Basic correctness
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_successful_find_peer():
    coord = make_coordinator()
    result = await coord.find_peer("abc123", fast_query, QueryPriority.USER)

    assert result.status == QueryStatus.SUCCESS
    assert result.found is True
    assert len(result.closest_peers) == 5
    assert result.hops == 3
    assert result.duration_ms > 0
    assert result.error is None


@pytest.mark.trio
async def test_unknown_peer_returns_closest():
    coord = make_coordinator()
    result = await coord.find_peer("unknown_peer", unknown_peer_query, QueryPriority.USER)

    assert result.status == QueryStatus.SUCCESS
    assert result.found is False
    assert len(result.closest_peers) == 3
    assert result.error is None


@pytest.mark.trio
async def test_failing_query_recorded():
    coord = make_coordinator()
    result = await coord.find_peer("bad_peer", failing_query, QueryPriority.USER)

    assert result.status == QueryStatus.FAILED
    assert "stream error" in result.error.lower()
    assert result.found is False


@pytest.mark.trio
async def test_timeout_enforced():
    coord = make_coordinator(query_timeout=0.1)
    result = await coord.find_peer("slow_peer", slow_query, QueryPriority.USER)

    assert result.status == QueryStatus.TIMEOUT
    assert result.error is not None
    assert result.duration_ms < 500  # should not have waited 2s


# ---------------------------------------------------------------------------
# Concurrency limiting
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_capacity_limiter_blocks_excess_queries():
    """Verify that more than max_concurrent_queries queries queue up, not crash."""
    max_q = 3
    coord = make_coordinator(max_concurrent_queries=max_q, max_random_walks=1)

    delay_s = 0.15
    barrier_hit: List[float] = []

    async def counting_query(peer_id: str):
        barrier_hit.append(time.monotonic())
        await trio.sleep(delay_s)
        return True, [], 1

    # Fire 6 queries — only 3 should run concurrently
    results = []
    async with trio.open_nursery() as nursery:
        for i in range(6):
            async def _run(idx=i):
                r = await coord.find_peer(f"peer_{idx}", counting_query, QueryPriority.USER)
                results.append(r)
            nursery.start_soon(_run)

    assert len(results) == 6
    assert all(r.status == QueryStatus.SUCCESS for r in results)
    
    # Peak concurrent should not exceed max
    snap = coord.snapshot()
    assert snap["coordinator"]["concurrency"]["peak"] <= max_q


@pytest.mark.trio
async def test_background_walks_cannot_starve_user_queries():
    """
    Background walks use Layer B cap (max_random_walks=1).
    User queries must still succeed even when walks are active.
    """
    coord = make_coordinator(
        max_concurrent_queries=4,
        max_random_walks=1,
        query_timeout=2.0,
    )

    walk_started = trio.Event()
    walk_proceed = trio.Event()

    async def blocking_walk(peer_id: str):
        walk_started.set()
        await walk_proceed.wait()
        return False, [], 2

    async def fast_user_query(peer_id: str):
        await trio.sleep(0.01)
        return True, ["found_it"], 1

    results: List = []

    async with trio.open_nursery() as nursery:
        # Start a blocking background walk
        nursery.start_soon(
            coord.find_peer, "walk_target", blocking_walk, QueryPriority.BACKGROUND
        )
        # Wait for walk to start
        await walk_started.wait()

        # Fire 3 user queries — should NOT be blocked by the walk
        for i in range(3):
            async def _user(idx=i):
                r = await coord.find_peer(f"user_{idx}", fast_user_query, QueryPriority.USER)
                results.append(r)
            nursery.start_soon(_user)

        # Give user queries time to complete
        await trio.sleep(0.3)

        # Unblock the walk
        walk_proceed.set()

    assert len(results) == 3
    assert all(r.status == QueryStatus.SUCCESS for r in results)


@pytest.mark.trio
async def test_random_walk_cap_enforced():
    """No more than max_random_walks background queries run simultaneously."""
    max_walks = 2
    coord = make_coordinator(max_concurrent_queries=8, max_random_walks=max_walks)

    concurrent_peaks: List[int] = []
    active = [0]

    async def tracking_walk(peer_id: str):
        active[0] += 1
        concurrent_peaks.append(active[0])
        await trio.sleep(0.1)
        active[0] -= 1
        return False, [], 2

    async with trio.open_nursery() as nursery:
        for i in range(6):
            nursery.start_soon(
                coord.find_peer, f"walk_{i}", tracking_walk, QueryPriority.BACKGROUND
            )

    assert max(concurrent_peaks) <= max_walks


# ---------------------------------------------------------------------------
# Metrics correctness
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_metrics_counters_accurate():
    coord = make_coordinator(query_timeout=0.1)

    # 2 success, 1 fail, 1 timeout
    await coord.find_peer("p1", fast_query)
    await coord.find_peer("p2", fast_query)
    await coord.find_peer("p3", failing_query)
    await coord.find_peer("p4", slow_query)  # will timeout

    snap = coord.snapshot()
    c = snap["coordinator"]["counters"]
    assert c["total"] == 4
    assert c["success"] == 2
    assert c["failed"] == 1
    assert c["timeout"] == 1


@pytest.mark.trio
async def test_snapshot_structure():
    coord = make_coordinator()
    await coord.find_peer("test", fast_query)

    snap = coord.snapshot()
    assert "coordinator" in snap
    assert "capacity_limiter" in snap
    assert "random_walk_limiter" in snap
    assert "live_queries" in snap
    assert "recent_results" in snap

    cl = snap["capacity_limiter"]
    assert cl["total"] == 5
    assert cl["available"] + cl["borrowed"] == cl["total"]


@pytest.mark.trio
async def test_reconfigure_updates_limiter():
    coord = make_coordinator(max_concurrent_queries=5, max_random_walks=2)
    coord._bootstrap()  # Force init
    
    coord.reconfigure(max_concurrent_queries=8, query_timeout=45.0)
    
    assert coord._max_queries == 8
    assert coord._query_timeout == 45.0
    assert coord._query_limiter.total_tokens == 8


# ---------------------------------------------------------------------------
# History and rolling results
# ---------------------------------------------------------------------------


@pytest.mark.trio
async def test_history_bounded():
    coord = make_coordinator()
    coord._max_history = 5

    for i in range(10):
        await coord.find_peer(f"peer_{i}", fast_query)

    assert len(coord._history) <= 5


@pytest.mark.trio
async def test_recent_results_in_snapshot():
    coord = make_coordinator()
    await coord.find_peer("alpha", fast_query)
    await coord.find_peer("beta", failing_query)

    snap = coord.snapshot()
    assert len(snap["recent_results"]) >= 2
    statuses = {r["status"] for r in snap["recent_results"]}
    assert QueryStatus.SUCCESS in statuses
    assert QueryStatus.FAILED in statuses


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


def test_invalid_config_raises():
    with pytest.raises(ValueError, match="max_random_walks must be"):
        DHTQueryCoordinator(max_concurrent_queries=5, max_random_walks=5)


@pytest.mark.trio
async def test_empty_peer_id_handled():
    coord = make_coordinator()
    result = await coord.find_peer("", fast_query)
    assert result.status in (QueryStatus.SUCCESS, QueryStatus.FAILED, QueryStatus.TIMEOUT)


@pytest.mark.trio
async def test_snapshot_latency_percentiles(autojump_clock):
    coord = DHTQueryCoordinator(
        max_concurrent_queries=10, max_random_walks=3, query_timeout=30.0
    )

    def make_query_fn(delay_s):
        async def _fn(pid):
            await trio.sleep(delay_s)
            return True, [], 1
        return _fn

    # 20 queries: durations 0.1s .. 2.0s
    for i in range(1, 21):
        await coord.find_peer(f"peer-{i}", make_query_fn(i * 0.1))

    rates = coord.snapshot()["coordinator"]["rates"]
    assert 0 < rates["p50_ms"] <= rates["p95_ms"] <= rates["p99_ms"]
    assert 900 <= rates["p50_ms"] <= 1200   # median ≈ 1.05s
    assert rates["p99_ms"] <= 2100


def test_percentiles_empty_history():
    coord = DHTQueryCoordinator(max_concurrent_queries=10, max_random_walks=3)
    rates = coord.snapshot()["coordinator"]["rates"]
    assert rates["p50_ms"] == rates["p95_ms"] == rates["p99_ms"] == 0.0


@pytest.mark.trio
async def test_concurrent_mixed_priority():
    """Mix of user + background queries all complete successfully."""
    coord = make_coordinator(
        max_concurrent_queries=6, max_random_walks=2, query_timeout=2.0
    )
    results = []

    async with trio.open_nursery() as nursery:
        for i in range(4):
            async def _user(idx=i):
                r = await coord.find_peer(f"u{idx}", fast_query, QueryPriority.USER)
                results.append(r)
            nursery.start_soon(_user)

        for i in range(2):
            async def _walk(idx=i):
                r = await coord.find_peer(f"w{idx}", fast_query, QueryPriority.BACKGROUND)
                results.append(r)
            nursery.start_soon(_walk)

    assert len(results) == 6
    assert all(r.status == QueryStatus.SUCCESS for r in results)
