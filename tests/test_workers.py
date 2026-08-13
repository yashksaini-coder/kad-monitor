"""Regression tests for the background workers (previously zero coverage —
which is how the fire-and-forget bug survived)."""
import pytest
import trio

from src.coordinator import DHTQueryCoordinator
from src.stream_manager import StreamManager
from src.workers import load_generator, metrics_broadcaster


class SlowFakeNetwork:
    """Every query takes 1 virtual second; records peak concurrency."""

    def __init__(self):
        self.peer_ids = ["peer-aaaa", "peer-bbbb", "peer-cccc"]
        self.current = 0
        self.peak = 0

    async def query(self, pid):
        self.current += 1
        self.peak = max(self.peak, self.current)
        try:
            await trio.sleep(1.0)
        finally:
            self.current -= 1
        return True, [], 1

    def snapshot(self):
        return {"network": {"scenario": "FAKE"}}


@pytest.mark.trio
async def test_load_generator_overlaps_queries(autojump_clock):
    """At 10 QPS with 1s queries, >1 query MUST be in flight (pre-fix peak == 1)."""
    network = SlowFakeNetwork()
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=50, max_random_walks=3, query_timeout=30.0
    )
    sm = StreamManager(max_streams=100)
    state = {"active": True, "qps": 10.0, "mode": "known"}

    async with trio.open_nursery() as nursery:
        await nursery.start(load_generator, coordinator, network, sm, state)
        await trio.sleep(3.0)
        nursery.cancel_scope.cancel()

    assert network.peak > 1, f"queries never overlapped (peak={network.peak})"
    assert state["achieved_qps"] > 5.0, f"achieved {state['achieved_qps']} of 10 QPS"


@pytest.mark.trio
async def test_load_generator_idle_reports_zero_qps(autojump_clock):
    network = SlowFakeNetwork()
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=50, max_random_walks=3, query_timeout=30.0
    )
    sm = StreamManager(max_streams=100)
    state = {"active": False, "qps": 10.0, "mode": "known"}

    async with trio.open_nursery() as nursery:
        await nursery.start(load_generator, coordinator, network, sm, state)
        await trio.sleep(2.0)
        nursery.cancel_scope.cancel()

    assert state["achieved_qps"] == 0.0
    assert network.peak == 0
