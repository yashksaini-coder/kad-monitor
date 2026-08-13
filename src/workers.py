"""
Background Workers
==================
Trio nursery tasks that run alongside the API server:

1. ``random_walk_worker``  – periodic DHT maintenance walks (use Layer B cap)
2. ``load_generator``      – fires configurable bursts of user queries to stress-test
3. ``metrics_broadcaster`` – sends coordinator snapshots to all WebSocket clients
"""

from __future__ import annotations

import hashlib
import logging
import random
import time
from typing import TYPE_CHECKING

import trio

from src.coordinator import QueryPriority

if TYPE_CHECKING:
    from src.coordinator import DHTQueryCoordinator
    from src.dht_simulation import SimulatedDHTNetwork
    from src.stream_manager import StreamManager

logger = logging.getLogger(__name__)


async def random_walk_worker(
    coordinator: "DHTQueryCoordinator",
    network: "SimulatedDHTNetwork",
    stream_manager: "StreamManager",
    interval_s: float = 5.0,
    *,
    task_status=trio.TASK_STATUS_IGNORED,
) -> None:
    """
    Periodic DHT random walk — background maintenance traffic.

    Uses QueryPriority.BACKGROUND so the Layer-B cap limits how many walks
    can run simultaneously.  This prevents walks from starving user queries.
    """
    task_status.started(None)
    logger.info("Random walk worker started (interval=%.1fs)", interval_s)

    while True:
        await trio.sleep(interval_s)
        target = random.choice(network.peer_ids) if network.peer_ids else None
        if target is None:
            continue

        async def _walk_query_fn(pid: str):
            async with stream_manager.open_stream(pid, "/libp2p/kad/1.0.0"):
                return await network.query(pid)

        try:
            result = await coordinator.find_peer(
                target,
                _walk_query_fn,
                priority=QueryPriority.BACKGROUND,
            )
            logger.debug(
                "Random walk → %s | status=%s hops=%d",
                target[:12],
                result.status,
                result.hops,
            )
        except Exception as exc:
            logger.warning("Random walk error: %s", exc)


async def load_generator(
    coordinator: "DHTQueryCoordinator",
    network: "SimulatedDHTNetwork",
    stream_manager: "StreamManager",
    state: dict,
    *,
    task_status=trio.TASK_STATUS_IGNORED,
) -> None:
    """
    Configurable load generator for stress testing.

    ``state`` is a shared dict with keys:
      - ``active``:       bool  — whether the generator is running
      - ``qps``:          float — target queries per second
      - ``mode``:         str   — "known" | "unknown" | "mixed"
      - ``achieved_qps``: float — (written here) rolling 5s rate actually fired

    Queries are spawned into a long-lived nursery so they genuinely overlap —
    a per-iteration nursery would WAIT for each query on exit and cap
    concurrency at 1 (the original bug).
    """
    task_status.started(None)
    logger.info("Load generator started")

    window_s = 5.0
    fired: list[float] = []  # trio.current_time() stamps of fired queries

    async with trio.open_nursery() as nursery:
        while True:
            if not state.get("active", False):
                state["achieved_qps"] = 0.0
                fired.clear()
                await trio.sleep(0.5)
                continue

            qps = float(state.get("qps", 2.0))
            mode = state.get("mode", "mixed")
            interval = max(1.0 / qps, 0.05)

            if mode == "known" and network.peer_ids:
                target = random.choice(network.peer_ids)
            elif mode == "unknown":
                target = hashlib.sha256(str(time.time()).encode()).hexdigest()[:40]
            else:  # mixed
                if random.random() < 0.5 and network.peer_ids:
                    target = random.choice(network.peer_ids)
                else:
                    target = hashlib.sha256(str(time.time()).encode()).hexdigest()[:40]

            async def _fire(pid: str) -> None:
                async def _user_query_fn(p: str):
                    async with stream_manager.open_stream(p, "/libp2p/kad/1.0.0"):
                        return await network.query(p)

                try:
                    await coordinator.find_peer(
                        pid, _user_query_fn, priority=QueryPriority.USER
                    )
                except Exception as exc:
                    logger.debug("Load gen query error: %s", exc)

            nursery.start_soon(_fire, target)

            now = trio.current_time()
            fired.append(now)
            fired[:] = [t for t in fired if t >= now - window_s]
            state["achieved_qps"] = round(len(fired) / window_s, 2)

            await trio.sleep(interval)


async def metrics_broadcaster(
    coordinator: "DHTQueryCoordinator",
    network: "SimulatedDHTNetwork",
    stream_manager: "StreamManager",
    send_channel: trio.MemorySendChannel,
    interval_s: float = 0.5,
    extra: dict | None = None,
    history=None,
    *,
    task_status=trio.TASK_STATUS_IGNORED,
) -> None:
    """
    Periodically assembles a full system snapshot and pushes it to the
    broadcast send channel (consumed by WebSocket handler).
    """
    task_status.started(None)
    logger.info("Metrics broadcaster started (interval=%.2fs)", interval_s)

    while True:
        await trio.sleep(interval_s)
        snapshot = {
            **coordinator.snapshot(),
            **stream_manager.snapshot(),
            **network.snapshot(),
            "ts": time.time(),
            **(extra or {}),
        }
        try:
            send_channel.send_nowait(snapshot)
        except trio.WouldBlock:
            pass  # channel full — skip this tick
        except trio.ClosedResourceError:
            logger.info("Broadcast channel closed, stopping broadcaster")
            return
        if history is not None:
            try:
                history.append(snapshot)
            except Exception:
                logger.exception("history append failed")
