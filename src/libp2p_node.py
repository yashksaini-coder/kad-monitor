"""
Real libp2p DHT Node Integration
================================
Production-ready wrapper around py-libp2p 0.6.0.

Uses the real library APIs:
- ``new_host()``         → creates a libp2p host with transport, muxer, security
- ``ResourceManager``    → stream/connection limiting at the network layer
- ``KadDHT``             → Kademlia DHT for peer routing, value store, providers
- ``GossipSub + Pubsub`` → publish/subscribe messaging
- ``host.run()``         → trio-native host lifecycle

This module replaces SimulatedDHTNetwork when running in "real" mode.
"""

from __future__ import annotations

import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional

import trio
from libp2p import ResourceManager, generate_new_ed25519_identity, new_host
from libp2p.custom_types import TProtocol
from libp2p.kad_dht.kad_dht import DHTMode, KadDHT
from libp2p.peer.id import ID
from libp2p.peer.peerinfo import PeerInfo
from libp2p.pubsub.gossipsub import GossipSub
from libp2p.pubsub.pubsub import Pubsub
from libp2p.rcmgr import ResourceLimits
from libp2p.rcmgr.connection_limits import ConnectionLimits
from libp2p.tools.async_service.trio_service import TrioManager
from multiaddr import Multiaddr

logger = logging.getLogger(__name__)


@dataclass
class Libp2pNodeConfig:
    port: int = 4001
    enable_mdns: bool = False
    enable_upnp: bool = False
    enable_quic: bool = False
    bootstrap_peers: list[str] = field(default_factory=list)
    protocol_prefix: str = "/ipfs"
    enable_random_walk: bool = True
    # ResourceManager limits
    max_connections: int = 200
    max_streams: int = 1000
    max_memory_mb: int = 256
    # Connection limits
    max_established_per_peer: int = 10
    max_established_total: int = 150
    max_pending_inbound: int = 64
    max_pending_outbound: int = 128
    # PubSub
    enable_pubsub: bool = True
    pubsub_topics: list[str] = field(default_factory=lambda: ["/dht-monitor/events/1.0.0"])
    # GossipSub parameters
    gossip_degree: int = 6
    gossip_degree_low: int = 4
    gossip_degree_high: int = 12
    gossip_heartbeat_interval: int = 60

    @classmethod
    def from_dict(cls, d: dict) -> "Libp2pNodeConfig":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class Libp2pNode:
    """
    Production libp2p node with KadDHT + GossipSub + ResourceManager.

    Lifecycle::

        async with Libp2pNode(config) as node:
            result = await node.find_peer(peer_id)
            await node.put_value("/app/key", b"value")
            await node.publish("topic", b"message")
    """

    def __init__(self, config: Optional[Libp2pNodeConfig] = None):
        self.config = config or Libp2pNodeConfig()
        self._host: Any = None
        self._dht: Any = None
        self._pubsub: Any = None
        self._gossipsub: Any = None
        self._resource_manager: Any = None
        self._peer_id: Any = None
        self._peer_id_str: Optional[str] = None
        self._started = False
        self._host_ctx: Any = None
        self._nursery: Any = None  # Background nursery for long-running tasks
        self._dht_cancel_scope: Any = None
        self._subscriptions: dict[str, Any] = {}
        self._received_messages: list[dict] = []
        self._max_message_history = 100
        self._stats = {
            "dht_queries": 0,
            "dht_puts": 0,
            "dht_gets": 0,
            "dht_provides": 0,
            "pubsub_published": 0,
            "pubsub_received": 0,
            "connections_made": 0,
            "streams_opened": 0,
        }

    @property
    def peer_id(self) -> str:
        if self._peer_id_str is None:
            raise RuntimeError("Node not started")
        return self._peer_id_str

    @property
    def is_started(self) -> bool:
        return self._started

    @property
    def routing_table_size(self) -> int:
        if self._dht is None:
            return 0
        try:
            return self._dht.get_routing_table_size()
        except Exception:
            return 0

    @property
    def resource_manager_stats(self) -> dict:
        if self._resource_manager is None:
            return {}
        try:
            return self._resource_manager.get_stats()
        except Exception:
            return {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self, *, task_status=trio.TASK_STATUS_IGNORED) -> None:
        if self._started:
            task_status.started(None)
            return

        identity = generate_new_ed25519_identity()

        listen_addrs = [
            Multiaddr(f"/ip4/0.0.0.0/tcp/{self.config.port}"),
        ]
        if self.config.enable_quic:
            listen_addrs.append(
                Multiaddr(f"/ip4/0.0.0.0/udp/{self.config.port}/quic-v1")
            )

        # --- ResourceManager: network-layer stream/connection guard ---
        resource_limits = ResourceLimits(
            max_connections=self.config.max_connections,
            max_streams=self.config.max_streams,
            max_memory_mb=self.config.max_memory_mb,
        )
        connection_limits = ConnectionLimits(
            max_established_per_peer=self.config.max_established_per_peer,
            max_established_total=self.config.max_established_total,
            max_pending_inbound=self.config.max_pending_inbound,
            max_pending_outbound=self.config.max_pending_outbound,
        )
        self._resource_manager = ResourceManager(
            limits=resource_limits,
            connection_limits=connection_limits,
            enable_circuit_breaker=True,
            enable_graceful_degradation=True,
            enable_rate_limiting=True,
        )

        # --- Host creation ---
        self._host = new_host(
            key_pair=identity,
            listen_addrs=listen_addrs,
            enable_mDNS=self.config.enable_mdns,
            enable_upnp=self.config.enable_upnp,
            enable_quic=self.config.enable_quic,
            muxer_preference="YAMUX",
            resource_manager=self._resource_manager,
        )

        # --- Start host with trio lifecycle (async context manager) ---
        self._host_ctx = self._host.run(listen_addrs)
        await self._host_ctx.__aenter__()

        self._peer_id = self._host.get_id()
        self._peer_id_str = str(self._peer_id)

        # --- KadDHT ---
        self._dht = KadDHT(
            host=self._host,
            mode=DHTMode.SERVER,
            enable_random_walk=self.config.enable_random_walk,
            protocol_prefix=TProtocol(self.config.protocol_prefix),
        )

        # KadDHT must be run via TrioManager, which provides the service
        # lifecycle (_manager attribute). The manager.run() blocks forever,
        # so we spawn it into a background nursery.
        self._dht_manager = TrioManager(self._dht)
        self._nursery = trio.open_nursery()
        self._bg_nursery = await self._nursery.__aenter__()
        self._bg_nursery.start_soon(self._run_dht)

        # Give the DHT manager time to initialise internal services
        await trio.sleep(0.1)

        # --- GossipSub + PubSub ---
        if self.config.enable_pubsub:
            await self._setup_pubsub()

        self._started = True

        addrs = self._host.get_addrs()
        logger.info(
            "libp2p node started — peer_id=%s addrs=%s rt_size=%d rcmgr=%s",
            self._peer_id_str[:20] + "...",
            [str(a) for a in addrs[:3]],
            self.routing_table_size,
            "enabled",
        )

        if self.config.bootstrap_peers:
            await self._bootstrap()

        task_status.started(None)

    async def _run_dht(self) -> None:
        """Background task: run KadDHT via TrioManager until cancelled."""
        try:
            await self._dht_manager.run()
        except trio.Cancelled:
            logger.debug("DHT manager cancelled")
            raise
        except Exception as e:
            logger.debug("DHT manager exited: %s", e)

    async def _setup_pubsub(self) -> None:
        self._gossipsub = GossipSub(
            protocols=[TProtocol("/meshsub/1.1.0"), TProtocol("/meshsub/1.0.0")],
            degree=self.config.gossip_degree,
            degree_low=self.config.gossip_degree_low,
            degree_high=self.config.gossip_degree_high,
            heartbeat_interval=self.config.gossip_heartbeat_interval,
        )

        self._pubsub = Pubsub(
            host=self._host,
            router=self._gossipsub,
            cache_size=256,
        )

        logger.info("GossipSub + Pubsub initialised")

    async def _bootstrap(self) -> None:
        for addr_str in self.config.bootstrap_peers:
            try:
                ma = Multiaddr(addr_str)
                parts = str(ma).split("/p2p/")
                if len(parts) < 2:
                    logger.warning("Invalid bootstrap addr (no /p2p/): %s", addr_str[:40])
                    continue
                peer_id_b58 = parts[-1]
                peer_id = ID.from_base58(peer_id_b58)
                peer_info = PeerInfo(peer_id=peer_id, addrs=[ma])

                await self._host.connect(peer_info)
                self._stats["connections_made"] += 1

                if self._dht:
                    await self._dht.add_peer(peer_id)

                logger.info("Bootstrapped with peer: %s", peer_id_b58[:20])
            except Exception as e:
                logger.warning("Bootstrap failed for %s: %s", addr_str[:40], e)

    async def stop(self) -> None:
        if not self._started:
            return

        try:
            # Unsubscribe all pubsub topics
            for topic, sub in list(self._subscriptions.items()):
                try:
                    await self._pubsub.unsubscribe(topic)
                except Exception:
                    pass
            self._subscriptions.clear()

            # Cancel the background nursery (stops KadDHT.run())
            if self._bg_nursery is not None:
                self._bg_nursery.cancel_scope.cancel()
            if self._nursery is not None:
                try:
                    await self._nursery.__aexit__(None, None, None)
                except BaseException:
                    pass  # Nursery may raise Cancelled
                self._nursery = None
                self._bg_nursery = None

            # Close the host context
            if self._host_ctx:
                try:
                    await self._host_ctx.__aexit__(None, None, None)
                except Exception:
                    pass
                self._host_ctx = None

            if self._resource_manager:
                self._resource_manager.close()

            self._started = False
            logger.info("libp2p node stopped")
        except Exception as e:
            logger.error("Error stopping libp2p node: %s", e)

    async def __aenter__(self) -> "Libp2pNode":
        await self.start()
        return self

    async def __aexit__(self, *args) -> None:
        await self.stop()

    # ------------------------------------------------------------------
    # DHT: Peer Routing
    # ------------------------------------------------------------------

    async def find_peer(self, target_peer_id_str: str) -> tuple[bool, list[str], int]:
        """
        DHT find_peer lookup.

        Returns ``(found, closest_peers, hops)`` — matches the simulated interface.
        """
        if not self._started or self._dht is None:
            raise RuntimeError("Node not started")

        self._stats["dht_queries"] += 1
        hops = 0

        try:
            try:
                target_id = ID.from_base58(target_peer_id_str)
            except Exception:
                target_id = ID(target_peer_id_str.encode("utf-8"))

            peer_info = await self._dht.find_peer(target_id)
            hops = 1

            if peer_info is not None:
                closest = [str(p) for p in self._dht.routing_table.get_peer_ids()[:10]]
                return True, closest, hops

            closest = [str(p) for p in self._dht.routing_table.get_peer_ids()[:20]]
            return False, closest, hops

        except Exception as e:
            logger.debug("find_peer error for %s: %s", target_peer_id_str[:20], e)
            closest = self._get_routing_table_peers(20)
            return False, closest, hops

    # ------------------------------------------------------------------
    # DHT: Value Store
    # ------------------------------------------------------------------

    async def put_value(self, key: str, value: bytes) -> bool:
        if not self._started or self._dht is None:
            raise RuntimeError("Node not started")

        self._stats["dht_puts"] += 1
        try:
            await self._dht.put_value(key, value)
            logger.debug("DHT put_value: %s (%d bytes)", key, len(value))
            return True
        except Exception as e:
            logger.warning("put_value failed for %s: %s", key, e)
            return False

    async def get_value(self, key: str) -> Optional[bytes]:
        if not self._started or self._dht is None:
            raise RuntimeError("Node not started")

        self._stats["dht_gets"] += 1
        try:
            value = await self._dht.get_value(key)
            logger.debug("DHT get_value: %s → %s", key, value is not None)
            return value
        except Exception as e:
            logger.debug("get_value failed for %s: %s", key, e)
            return None

    # ------------------------------------------------------------------
    # DHT: Content Providers
    # ------------------------------------------------------------------

    async def provide(self, key: str) -> bool:
        if not self._started or self._dht is None:
            raise RuntimeError("Node not started")

        self._stats["dht_provides"] += 1
        try:
            result = await self._dht.provide(key)
            logger.debug("DHT provide: %s → %s", key, result)
            return result
        except Exception as e:
            logger.warning("provide failed for %s: %s", key, e)
            return False

    async def find_providers(self, key: str, count: int = 20) -> list[dict]:
        if not self._started or self._dht is None:
            raise RuntimeError("Node not started")

        try:
            providers = await self._dht.find_providers(key, count)
            return [
                {
                    "peer_id": str(p.peer_id),
                    "addrs": [str(a) for a in p.addrs],
                }
                for p in providers
            ]
        except Exception as e:
            logger.debug("find_providers failed for %s: %s", key, e)
            return []

    # ------------------------------------------------------------------
    # PubSub
    # ------------------------------------------------------------------

    async def subscribe(self, topic: str) -> bool:
        if self._pubsub is None:
            logger.warning("PubSub not enabled")
            return False

        if topic in self._subscriptions:
            return True

        try:
            sub = await self._pubsub.subscribe(topic)
            self._subscriptions[topic] = sub
            logger.info("Subscribed to topic: %s", topic)
            return True
        except Exception as e:
            logger.warning("Subscribe failed for %s: %s", topic, e)
            return False

    async def unsubscribe(self, topic: str) -> bool:
        if self._pubsub is None or topic not in self._subscriptions:
            return False

        try:
            await self._pubsub.unsubscribe(topic)
            del self._subscriptions[topic]
            logger.info("Unsubscribed from topic: %s", topic)
            return True
        except Exception as e:
            logger.warning("Unsubscribe failed for %s: %s", topic, e)
            return False

    async def publish(self, topic: str, data: bytes) -> bool:
        if self._pubsub is None:
            logger.warning("PubSub not enabled")
            return False

        self._stats["pubsub_published"] += 1
        try:
            await self._pubsub.publish(topic, data)
            logger.debug("Published to %s (%d bytes)", topic, len(data))
            return True
        except Exception as e:
            logger.warning("Publish failed for %s: %s", topic, e)
            return False

    def get_subscribed_topics(self) -> list[str]:
        return list(self._subscriptions.keys())

    # ------------------------------------------------------------------
    # Streams (direct protocol streams via host)
    # ------------------------------------------------------------------

    async def open_stream(
        self, peer_id_str: str, protocol: str = "/libp2p/kad/1.0.0"
    ) -> Any:
        """Open a direct protocol stream to a remote peer."""
        if not self._started or self._host is None:
            raise RuntimeError("Node not started")

        try:
            target_id = ID.from_base58(peer_id_str)
        except Exception:
            target_id = ID(peer_id_str.encode("utf-8"))

        self._stats["streams_opened"] += 1
        stream = await self._host.new_stream(target_id, [TProtocol(protocol)])
        return stream

    # ------------------------------------------------------------------
    # Peer Management
    # ------------------------------------------------------------------

    def get_known_peers(self) -> list[str]:
        if not self._started or self._dht is None:
            return []
        return self._get_routing_table_peers(100)

    def get_connected_peers(self) -> list[str]:
        if not self._started or self._host is None:
            return []
        try:
            return [str(p) for p in self._host.get_connected_peers()]
        except Exception:
            return []

    def get_listen_addresses(self) -> list[str]:
        if not self._started or self._host is None:
            return []
        return [str(addr) for addr in self._host.get_addrs()]

    async def connect_peer(self, addr_str: str) -> bool:
        if not self._started or self._host is None:
            return False

        try:
            ma = Multiaddr(addr_str)
            parts = str(ma).split("/p2p/")
            if len(parts) < 2:
                return False
            peer_id = ID.from_base58(parts[-1])
            await self._host.connect(PeerInfo(peer_id=peer_id, addrs=[ma]))
            self._stats["connections_made"] += 1

            if self._dht:
                await self._dht.add_peer(peer_id)

            return True
        except Exception as e:
            logger.warning("connect_peer failed: %s", e)
            return False

    async def disconnect_peer(self, peer_id_str: str) -> bool:
        if not self._started or self._host is None:
            return False

        try:
            target_id = ID.from_base58(peer_id_str)
            await self._host.disconnect(target_id)
            return True
        except Exception as e:
            logger.debug("disconnect_peer failed: %s", e)
            return False

    def _get_routing_table_peers(self, limit: int) -> list[str]:
        try:
            return [str(p) for p in self._dht.routing_table.get_peer_ids()[:limit]]
        except Exception:
            return []

    # ------------------------------------------------------------------
    # Snapshot / Telemetry
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        rcmgr_stats = self.resource_manager_stats

        connected = self.get_connected_peers()
        known = self.get_known_peers()
        addrs = self.get_listen_addresses()

        return {
            "node": {
                "peer_id": self._peer_id_str,
                "started": self._started,
                "listen_addrs": addrs,
                "connected_peers": len(connected),
                "known_peers": len(known),
                "routing_table_size": self.routing_table_size,
                "subscribed_topics": self.get_subscribed_topics(),
            },
            "resource_manager": rcmgr_stats,
            "stats": dict(self._stats),
        }


class RealDHTNetwork:
    """
    Adapter that wraps Libp2pNode to match the SimulatedDHTNetwork interface.

    Exposes ``query()``, ``snapshot()``, ``set_scenario()``, and ``peer_ids``
    so the DHTQueryCoordinator and workers can treat real and simulated backends
    identically.
    """

    class ScenarioConfig:
        NORMAL = {"latency_base_ms": 0, "label": "Real network — no artificial delay"}
        DEGRADED = {"latency_base_ms": 100, "label": "Added 100ms latency per query"}
        STRESSED = {"latency_base_ms": 300, "label": "Added 300ms latency per query"}
        SATURATED = {"latency_base_ms": 600, "label": "Added 600ms latency per query"}

    def __init__(self, node: Libp2pNode, scenario: str = "NORMAL") -> None:
        self._node = node
        self._scenario_name = scenario
        self._scenario = getattr(
            self.ScenarioConfig, scenario, self.ScenarioConfig.NORMAL
        )
        self._stats = {
            "queries_served": 0,
            "stream_errors_injected": 0,
            "timeouts_injected": 0,
        }

    @property
    def peer_ids(self) -> list[str]:
        peers = self._node.get_known_peers()
        if self._node.is_started:
            peers.insert(0, self._node.peer_id)
        return peers

    @property
    def scenario_name(self) -> str:
        return self._scenario_name

    async def query(self, target_peer_id: str) -> tuple[bool, list[str], int]:
        latency_ms = self._scenario.get("latency_base_ms", 0)
        if latency_ms > 0:
            jitter_ms = latency_ms * 0.3
            await trio.sleep((latency_ms + random.uniform(0, jitter_ms)) / 1000)

        self._stats["queries_served"] += 1
        return await self._node.find_peer(target_peer_id)

    def set_scenario(self, scenario: str) -> None:
        if not hasattr(self.ScenarioConfig, scenario):
            raise ValueError(f"Unknown scenario: {scenario}")
        self._scenario_name = scenario
        self._scenario = getattr(self.ScenarioConfig, scenario)
        logger.info("Scenario switched to %s", scenario)

    def add_peer(self) -> None:
        pass  # Real network — peers join organically

    def snapshot(self) -> dict:
        node_snap = self._node.snapshot()
        node_info = node_snap.get("node", {})
        rcmgr = node_snap.get("resource_manager", {})
        stats = node_snap.get("stats", {})

        peers = self.peer_ids
        return {
            "network": {
                "scenario": self._scenario_name,
                "scenario_label": self._scenario.get("label", ""),
                "total_nodes": len(peers),
                "online_nodes": len(peers),
                "offline_nodes": 0,
                "avg_routing_table_size": node_info.get("routing_table_size", 0),
                "stats": {**self._stats, **stats},
                "is_real_libp2p": True,
                "peer_id": node_info.get("peer_id"),
                "listen_addrs": node_info.get("listen_addrs", []),
                "connected_peers": node_info.get("connected_peers", 0),
                "subscribed_topics": node_info.get("subscribed_topics", []),
            },
            "resource_manager": rcmgr,
            "nodes": [
                {
                    "peer_id": p[:20] + "..." if len(p) > 20 else p,
                    "peer_id_full": p,
                    "multiaddr": "",
                    "routing_table_size": (
                        node_info.get("routing_table_size", 0) if i == 0 else 0
                    ),
                    "latency_ms": self._scenario.get("latency_base_ms", 0),
                    "online": True,
                }
                for i, p in enumerate(peers[:30])
            ],
        }


async def create_libp2p_network(
    config: Optional[Libp2pNodeConfig] = None,
    scenario: str = "NORMAL",
) -> tuple[Libp2pNode, RealDHTNetwork]:
    """Create and start a real libp2p DHT network."""
    node = Libp2pNode(config)
    await node.start()
    network = RealDHTNetwork(node, scenario)
    return node, network
