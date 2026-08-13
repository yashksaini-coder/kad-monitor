"""
A/B experiment runner: same workload against an 'unprotected' coordinator
(caps effectively unlimited) and a 'protected' one (real caps), producing a
comparable summary per arm.  Headless — no HTTP server involved.
"""

from __future__ import annotations

import logging
import random

import trio

from src.coordinator import DHTQueryCoordinator, QueryPriority
from src.dht_simulation import SimulatedDHTNetwork
from src.stream_manager import StreamManager

logger = logging.getLogger(__name__)

SAMPLE_INTERVAL_S = 0.5


async def run_arm(name: str, arm_cfg: dict, workload: dict, network_cfg: dict) -> dict:
    network = SimulatedDHTNetwork(
        node_count=network_cfg["nodes"], scenario=network_cfg["scenario"]
    )
    stream_manager = StreamManager(max_streams=arm_cfg["max_streams"])
    coordinator = DHTQueryCoordinator(
        max_concurrent_queries=arm_cfg["max_queries"],
        max_random_walks=arm_cfg["max_walks"],
        query_timeout=arm_cfg["query_timeout"],
    )

    samples: list[dict] = []

    async def sampler() -> None:
        while True:
            await trio.sleep(SAMPLE_INTERVAL_S)
            snap = coordinator.snapshot()
            samples.append({
                "borrowed": snap["capacity_limiter"]["borrowed"],
                "concurrency": snap["coordinator"]["concurrency"]["current"],
                "waiting": snap["coordinator"]["concurrency"]["acquiring"],
            })

    async def fire(target: str) -> None:
        async def _query_fn(pid: str):
            async with stream_manager.open_stream(pid, "/libp2p/kad/1.0.0"):
                return await network.query(pid)

        try:
            await coordinator.find_peer(target, _query_fn, QueryPriority.USER)
        except Exception as exc:  # experiment must survive injected errors
            logger.debug("experiment query error: %s", exc)

    qps = float(workload["qps"])
    duration = float(workload["duration_s"])

    async with trio.open_nursery() as outer:
        outer.start_soon(sampler)
        async with trio.open_nursery() as fires:
            with trio.move_on_after(duration):
                while True:
                    fires.start_soon(fire, random.choice(network.peer_ids))
                    await trio.sleep(1.0 / qps)
            # leaving `fires` waits for in-flight queries (bounded by query_timeout)
        outer.cancel_scope.cancel()

    snap = coordinator.snapshot()
    coord = snap["coordinator"]
    return {
        "arm": name,
        "config": dict(arm_cfg),
        "counters": coord["counters"],
        "rates": coord["rates"],
        "peak_concurrency": coord["concurrency"]["peak"],
        "peak_borrowed": max((s["borrowed"] for s in samples), default=0),
        "peak_waiting": max((s["waiting"] for s in samples), default=0),
        "achieved_qps": round(coord["counters"]["total"] / duration, 2),
        "samples": samples,
    }


async def run_experiment(config: dict) -> dict:
    arms: dict[str, dict] = {}
    for name, arm_cfg in config["arms"].items():
        logger.info("experiment %s: running arm %r…", config.get("name"), name)
        arms[name] = await run_arm(name, arm_cfg, config["workload"], config["network"])
    return {
        "name": config.get("name", "experiment"),
        "network": config["network"],
        "workload": config["workload"],
        "arms": arms,
    }
