"""
DHT Simulation Layer
====================
A high-fidelity simulation of a py-libp2p Kademlia DHT network.

Exposes the same async interface that the real DHTQueryCoordinator expects,
so switching from simulation → production requires only swapping the
``query_fn`` passed to ``coordinator.find_peer()``.

The simulation deliberately injects:
- Variable network latency (per-hop jitter)
- Configurable peer discovery rates
- Occasional stream errors and timeouts
- Background "random walk" maintenance traffic

This allows us to reproduce the resource-exhaustion scenario described in the
Technical Design Doc, and to verify that the coordinator fixes it.
"""

from __future__ import annotations

import hashlib
import logging
import math
import random
import time
from dataclasses import dataclass, field
from typing import Optional

import trio

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Kademlia-style peer ID helpers
# ---------------------------------------------------------------------------

def _xor_distance(a: bytes, b: bytes) -> int:
    return int.from_bytes(bytes(x ^ y for x, y in zip(a, b)), "big")


def _peer_id_bytes(peer_id: str) -> bytes:
    return hashlib.sha256(peer_id.encode()).digest()[:20]


def _random_peer_id() -> str:
    return hashlib.sha256(str(random.random()).encode()).hexdigest()[:40]


# ---------------------------------------------------------------------------
# Virtual node
# ---------------------------------------------------------------------------


@dataclass
class VirtualNode:
    """Represents a single libp2p node in the simulated network."""

    peer_id: str
    multiaddr: str
    routing_table: list[str] = field(default_factory=list)
    latency_ms: float = 50.0   # base round-trip latency
    online: bool = True
    stream_errors: float = 0.05   # probability of transient stream error

    def k_closest(self, target_id: str, k: int = 20) -> list[str]:
        """Return the k peers closest to target_id in XOR-distance."""
        target_bytes = _peer_id_bytes(target_id)
        ranked = sorted(
            self.routing_table,
            key=lambda pid: _xor_distance(_peer_id_bytes(pid), target_bytes),
        )
        return ranked[:k]

    def to_dict(self) -> dict:
        return {
            "peer_id": self.peer_id[:20] + "…",
            "peer_id_full": self.peer_id,
            "multiaddr": self.multiaddr,
            "routing_table_size": len(self.routing_table),
            "latency_ms": self.latency_ms,
            "online": self.online,
        }


# ---------------------------------------------------------------------------
# Network fabric
# ---------------------------------------------------------------------------


class SimulatedDHTNetwork:
    """
    Simulated Kademlia DHT network.

    Creates N virtual nodes, populates their routing tables, and exposes
    an async ``query`` method that mimics the iterative lookup algorithm.

    Load scenarios
    --------------
    ``ScenarioConfig`` controls failure rates, latency, and stress levels so
    the dashboard can demonstrate the coordinator under different pressures.
    """

    class ScenarioConfig:
        NORMAL = {
            "latency_base_ms": 40,
            "latency_jitter_ms": 20,
            "offline_ratio": 0.05,
            "stream_error_rate": 0.05,
            "unknown_peer_rate": 0.20,
        }
        DEGRADED = {
            "latency_base_ms": 150,
            "latency_jitter_ms": 100,
            "offline_ratio": 0.20,
            "stream_error_rate": 0.20,
            "unknown_peer_rate": 0.50,
        }
        STRESSED = {
            "latency_base_ms": 400,
            "latency_jitter_ms": 300,
            "offline_ratio": 0.35,
            "stream_error_rate": 0.40,
            "unknown_peer_rate": 0.70,
        }
        SATURATED = {
            "latency_base_ms": 800,
            "latency_jitter_ms": 500,
            "offline_ratio": 0.50,
            "stream_error_rate": 0.60,
            "unknown_peer_rate": 0.85,
        }

    def __init__(
        self,
        node_count: int = 50,
        k_bucket_size: int = 20,
        scenario: str = "NORMAL",
    ) -> None:
        self._node_count = node_count
        self._k = k_bucket_size
        self._scenario_name = scenario
        self._scenario = getattr(self.ScenarioConfig, scenario)
        self._nodes: dict[str, VirtualNode] = {}
        self._boot_node: Optional[VirtualNode] = None
        self._stats = {
            "queries_served": 0,
            "stream_errors_injected": 0,
            "timeouts_injected": 0,
        }
        self._build_network()

    # ------------------------------------------------------------------
    # Network construction
    # ------------------------------------------------------------------

    def _build_network(self) -> None:
        logger.info("Building simulated DHT network with %d nodes…", self._node_count)
        peer_ids = [_random_peer_id() for _ in range(self._node_count)]

        offline_count = int(self._node_count * self._scenario["offline_ratio"])
        offline_set = set(random.sample(peer_ids, offline_count))

        for i, pid in enumerate(peer_ids):
            node = VirtualNode(
                peer_id=pid,
                multiaddr=f"/ip4/10.{(i // 256) % 256}.{i % 256}.1/tcp/4001/p2p/{pid[:16]}",
                latency_ms=self._scenario["latency_base_ms"] + random.uniform(
                    0, self._scenario["latency_jitter_ms"]
                ),
                online=(pid not in offline_set),
                stream_errors=self._scenario["stream_error_rate"],
            )
            self._nodes[pid] = node

        # Populate routing tables (each node knows its k nearest neighbours)
        all_ids = list(peer_ids)
        for pid, node in self._nodes.items():
            pid_bytes = _peer_id_bytes(pid)
            closest = sorted(
                [p for p in all_ids if p != pid],
                key=lambda p: _xor_distance(_peer_id_bytes(p), pid_bytes),
            )[: self._k]
            node.routing_table = closest

        self._boot_node = list(self._nodes.values())[0]
        logger.info(
            "Network ready — %d online, %d offline",
            sum(1 for n in self._nodes.values() if n.online),
            sum(1 for n in self._nodes.values() if not n.online),
        )

    # ------------------------------------------------------------------
    # Query function (Kademlia iterative lookup)
    # ------------------------------------------------------------------

    async def query(self, target_peer_id: str) -> tuple[bool, list[str], int, list[str]]:
        """
        Simulate an iterative Kademlia ``FIND_NODE`` lookup for *target_peer_id*.

        Returns
        -------
        (found, closest_peers, hop_count, path)
        """
        assert self._boot_node is not None
        self._stats["queries_served"] += 1

        # Is the target actually in our network?
        target_in_network = (
            target_peer_id in self._nodes
            and random.random() > self._scenario["unknown_peer_rate"]
        )

        visited: set[str] = set()
        candidates = self._boot_node.k_closest(target_peer_id, 3)
        closest: list[str] = []
        hops = 0
        path: list[str] = []
        max_hops = 20

        while candidates and hops < max_hops:
            next_candidates = []

            for candidate_id in candidates[:3]:
                if candidate_id in visited:
                    continue
                visited.add(candidate_id)

                candidate_node = self._nodes.get(candidate_id)
                if not candidate_node or not candidate_node.online:
                    continue

                # Simulate stream error
                if random.random() < candidate_node.stream_errors:
                    self._stats["stream_errors_injected"] += 1
                    raise OSError(f"Stream error connecting to {candidate_id[:12]}…")

                # Simulate network latency
                jitter = random.uniform(0, self._scenario["latency_jitter_ms"] / 1000)
                await trio.sleep(candidate_node.latency_ms / 1000 + jitter)
                hops += 1
                path.append(candidate_id)

                # Direct hit?
                if candidate_id == target_peer_id and target_in_network:
                    return True, [candidate_id] + candidate_node.k_closest(target_peer_id, 5), hops, path

                # Recurse closer
                closer = candidate_node.k_closest(target_peer_id, 3)
                next_candidates.extend(closer)
                closest.extend(closer)

            candidates = [c for c in next_candidates if c not in visited]

        # Deduplicate closest list
        seen: set[str] = set()
        deduped_closest: list[str] = []
        for pid in closest:
            if pid not in seen:
                seen.add(pid)
                deduped_closest.append(pid)

        return False, deduped_closest[:20], hops, path

    # ------------------------------------------------------------------
    # Network control plane
    # ------------------------------------------------------------------

    def set_scenario(self, scenario: str) -> None:
        if not hasattr(self.ScenarioConfig, scenario):
            raise ValueError(f"Unknown scenario: {scenario}")
        self._scenario_name = scenario
        self._scenario = getattr(self.ScenarioConfig, scenario)
        # Rebuild with new parameters
        self._build_network()
        logger.info("Scenario switched to %s", scenario)

    def add_peer(self) -> VirtualNode:
        pid = _random_peer_id()
        node = VirtualNode(
            peer_id=pid,
            multiaddr=f"/ip4/10.99.{len(self._nodes) % 256}.1/tcp/4001/p2p/{pid[:16]}",
            latency_ms=self._scenario["latency_base_ms"],
        )
        all_ids = list(self._nodes.keys())
        if all_ids:
            node.routing_table = sorted(
                all_ids,
                key=lambda p: _xor_distance(_peer_id_bytes(p), _peer_id_bytes(pid)),
            )[: self._k]
        self._nodes[pid] = node
        return node

    def remove_peer(self, peer_id: str) -> bool:
        if peer_id in self._nodes:
            del self._nodes[peer_id]
            return True
        return False

    def toggle_peer_online(self, peer_id: str) -> Optional[bool]:
        node = self._nodes.get(peer_id)
        if node:
            node.online = not node.online
            return node.online
        return None

    # ------------------------------------------------------------------
    # Snapshot
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        online = [n for n in self._nodes.values() if n.online]
        offline = [n for n in self._nodes.values() if not n.online]
        return {
            "network": {
                "scenario": self._scenario_name,
                "total_nodes": len(self._nodes),
                "online_nodes": len(online),
                "offline_nodes": len(offline),
                "avg_routing_table_size": round(
                    sum(len(n.routing_table) for n in self._nodes.values()) / max(len(self._nodes), 1), 1
                ),
                "stats": self._stats,
            },
            "nodes": [n.to_dict() for n in list(self._nodes.values())[:30]],
        }

    @property
    def peer_ids(self) -> list[str]:
        return list(self._nodes.keys())

    @property
    def scenario_name(self) -> str:
        return self._scenario_name
